//go:build plori
// +build plori

/*
 * JuiceFS, Copyright 2026 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mount

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// Litestream drives the metadata replica as a CHILD PROCESS, not as a library.
//
// The library route is cheaper — a separate process costs about 36 MiB fixed
// per mount, roughly 1.5 of the 8 slots on a 2 GiB node, against roughly
// 0.7 MiB per database if the Go runtime were shared — and it was rejected
// anyway, for a reason that is not about memory and not about the SBOM gate.
//
// Litestream v0.5.17 opens the database it replicates with `modernc.org/sqlite`
// (db.go:1049 `sql.Open("sqlite", dsn)`, plus sqlite.FileControl at db.go:1009
// for checkpoint control). JuiceFS opens that same file with
// `mattn/go-sqlite3`. Linking both into one binary would put TWO independent
// SQLite library instances on ONE database file inside ONE process, and SQLite
// does not support that: POSIX advisory locks are held per process, so closing
// any descriptor on the file drops every lock the process holds on it, and each
// library instance keeps its own inode/WAL-index registry, so neither can see
// the other's locks or shared-memory state. Two SQLite builds in two processes
// is the configuration the locking protocol is designed for — and it is what
// the M0 harness already measured working. Two in one process is the
// documented corruption hazard, on the one file the entire design treats as
// the filesystem.
//
// `modernc.org/sqlite` being one of hack/verify_plori_sbom.py's DENIED_PREFIXES
// is the same fact written down as a gate, not an independent objection: had
// the hazard not existed, the gate change would have been small (admit
// modernc.org/sqlite, github.com/superfly/ltx, github.com/psanford/sqlite3vfs,
// github.com/benbjohnson/litestream and the go-dateparser/wazero chain it
// pulls in, and note in the support policy that the profile now links a second
// SQLite implementation). It is not worth making, because the sentence that
// would have to go into the support policy is the reason not to.
//
// What the library was wanted for costs nothing over the CLI. DB.SyncAndWait
// (db.go:714-728) is reachable as `litestream sync -wait`, which drives the
// same call through the control socket (cmd/litestream/sync.go); a single
// SIGTERM makes `replicate` run Store.Close and its final sync
// (cmd/litestream/main.go:194-198, db.go:839); and restore takes a TXID or a
// timestamp on the command line (cmd/litestream/restore.go:31-33,88-91). Exec
// additionally keeps the crash domains apart: a Litestream panic does not take
// the FUSE mount with it.
//
// The memory is still worth recovering. The route that does it without the
// hazard is ONE node-level Litestream process watching many databases —
// v0.5.17 supports it natively (DBConfig Dir/Pattern/Watch) at 36 MiB plus
// ~0.7 MiB per database — and the open question there is whether a per-volume
// replica prefix can be expressed under a single directory-watch config. That
// belongs with PLO-366 and the plugin, not here.
type Litestream struct {
	Bin        string
	ConfigPath string
	SocketPath string
	DBPath     string

	cmd  *exec.Cmd
	done chan error

	spec *MountSpec
	opts MountOptions
}

// ErrReplicaEmpty means the metadata prefix holds no restorable generation.
// It is the first-boot signal, not a failure.
var ErrReplicaEmpty = errors.New("metadata replica is empty")

// litestreamConfig is the subset of the v0.5.17 config schema the worker
// writes. It is generated rather than templated so every knob is a typed
// field, and it is written 0600 into the state directory, which the Agent
// container cannot see.
type litestreamConfig struct {
	Addr   string `yaml:"addr"`
	Socket struct {
		Enabled     bool   `yaml:"enabled"`
		Path        string `yaml:"path"`
		Permissions uint32 `yaml:"permissions"`
	} `yaml:"socket"`
	Levels []struct {
		Interval string `yaml:"interval"`
	} `yaml:"levels"`
	Snapshot struct {
		Interval string `yaml:"interval"`
	} `yaml:"snapshot"`
	L0Retention string `yaml:"l0-retention"`
	Logging     struct {
		Level  string `yaml:"level"`
		Type   string `yaml:"type"`
		Stderr bool   `yaml:"stderr"`
	} `yaml:"logging"`
	DBs []litestreamDB `yaml:"dbs"`
}

type litestreamDB struct {
	Path    string             `yaml:"path"`
	Replica litestreamReplicaC `yaml:"replica"`
}

type litestreamReplicaC struct {
	Type           string `yaml:"type"`
	Bucket         string `yaml:"bucket"`
	Path           string `yaml:"path"`
	Endpoint       string `yaml:"endpoint"`
	Region         string `yaml:"region"`
	ForcePathStyle bool   `yaml:"force-path-style"`
	SyncInterval   string `yaml:"sync-interval"`
}

// Compaction and retention cadence, from the PLO-316 wave-2 measurement
// (docs/design/per-agent-juicefs/benchmark-wave2-object-ops.md). Together with
// the 1 s sync interval in options.go these produce 0.018 object ops/s per
// idle mount.
const (
	DefaultCompactionL1  = 10 * time.Minute
	DefaultCompactionL2  = time.Hour
	DefaultCompactionL3  = 6 * time.Hour
	DefaultSnapshotEvery = 24 * time.Hour
	DefaultL0Retention   = 30 * time.Minute
)

// WriteConfig renders the config file. `credential` never appears in it: the
// child inherits AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY from this process's
// environment, which keeps the one bucket-wide key off disk (threat-model
// F-11: a config file is one more place a debug dump can read it from).
func (l *Litestream) WriteConfig(spec *MountSpec, opts MountOptions) error {
	return l.writeConfig(l.ConfigPath, spec, opts, strings.TrimSuffix(spec.MetaPrefix, "/"))
}

// writeRestoreConfig points the same database at a DIFFERENT prefix, the one
// the previous generation replicated into. Two files rather than one mutated
// file, so the replicating child is never a moment away from writing into the
// prefix it is only supposed to read.
func (l *Litestream) writeRestoreConfig(spec *MountSpec, opts MountOptions, sourcePrefix string) error {
	return l.writeConfig(l.restoreConfigPath(), spec, opts, strings.TrimSuffix(sourcePrefix, "/"))
}

func (l *Litestream) restoreConfigPath() string { return l.ConfigPath + ".restore" }

func (l *Litestream) writeConfig(path string, spec *MountSpec, opts MountOptions, replicaPrefix string) error {
	l.spec, l.opts = spec, opts
	var cfg litestreamConfig
	cfg.Socket.Enabled = true
	cfg.Socket.Path = l.SocketPath
	cfg.Socket.Permissions = 0600
	for _, d := range []time.Duration{DefaultCompactionL1, DefaultCompactionL2, DefaultCompactionL3} {
		cfg.Levels = append(cfg.Levels, struct {
			Interval string `yaml:"interval"`
		}{Interval: d.String()})
	}
	cfg.Snapshot.Interval = DefaultSnapshotEvery.String()
	cfg.L0Retention = DefaultL0Retention.String()
	cfg.Logging.Level = "INFO"
	cfg.Logging.Type = "json"
	cfg.Logging.Stderr = true
	cfg.DBs = []litestreamDB{{
		Path: l.DBPath,
		Replica: litestreamReplicaC{
			Type:           "s3",
			Bucket:         spec.ObjectStore.Bucket,
			Path:           replicaPrefix,
			Endpoint:       spec.ObjectStore.Endpoint,
			Region:         spec.ObjectStore.Region,
			ForcePathStyle: true,
			SyncInterval:   opts.LitestreamSync.String(),
		},
	}}
	data, err := marshalLitestreamYAML(&cfg)
	if err != nil {
		return fmt.Errorf("render litestream config: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create litestream config dir: %w", err)
	}
	return os.WriteFile(path, data, 0o600)
}

// Restore materialises the metadata database. `timestamp` is the pre-barrier
// durable point when one is known; the zero time restores the latest
// transaction.
//
// The empty-replica probe is deliberately never combined with a timestamp.
// `-if-replica-exists` turns "no transaction matches" into a silent success
// (cmd/litestream/restore.go:143-149), and a timestamp older than everything
// in the replica produces exactly that — so combining the two would let a
// too-old restore point look like a brand-new volume and be answered with a
// format, which is total data loss.
func (l *Litestream) Restore(ctx context.Context, sourcePrefix string, timestamp time.Time) error {
	if _, err := os.Stat(l.DBPath); err == nil {
		return fmt.Errorf("refusing to restore over an existing %s", l.DBPath)
	}
	if sourcePrefix == "" {
		// No populated generation below this epoch: nothing has ever been
		// replicated for this volume.
		return ErrReplicaEmpty
	}
	if err := l.writeRestoreConfig(l.spec, l.opts, sourcePrefix); err != nil {
		return err
	}
	defer os.Remove(l.restoreConfigPath())
	args := []string{"restore", "-config", l.restoreConfigPath(), "-o", l.DBPath, "-integrity-check", "full"}
	if timestamp.IsZero() {
		args = append(args, "-if-replica-exists")
	} else {
		args = append(args, "-timestamp", timestamp.UTC().Format(time.RFC3339))
	}
	args = append(args, l.DBPath)
	if out, err := l.run(ctx, args...); err != nil {
		return fmt.Errorf("litestream restore: %w: %s", err, lastLine(out))
	}
	if _, err := os.Stat(l.DBPath); err != nil {
		if os.IsNotExist(err) {
			return ErrReplicaEmpty
		}
		return fmt.Errorf("stat restored database: %w", err)
	}
	return nil
}

// Start launches continuous replication and waits for the control socket.
func (l *Litestream) Start(ctx context.Context) error {
	_ = os.Remove(l.SocketPath)
	cmd := exec.Command(l.Bin, "replicate", "-config", l.ConfigPath)
	cmd.Stdout = os.Stderr // one stream; the plugin reads the last stderr line
	cmd.Stderr = os.Stderr
	cmd.Env = os.Environ()
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start litestream: %w", err)
	}
	l.cmd = cmd
	l.done = make(chan error, 1)
	go func() { l.done <- cmd.Wait() }()

	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, err := os.Stat(l.SocketPath); err == nil {
			return nil
		}
		select {
		case err := <-l.done:
			l.done = nil
			return fmt.Errorf("litestream exited during startup: %w", err)
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			return errors.New("litestream control socket did not appear within 30s")
		}
	}
}

type syncResponse struct {
	TXID string `json:"txid"`
}

// SyncAndWait forces a sync and blocks until it completes — the CLI half of
// DB.SyncAndWait.
func (l *Litestream) SyncAndWait(ctx context.Context) error {
	_, err := l.control(ctx, "/sync", map[string]any{
		"path":    l.DBPath,
		"wait":    true,
		"timeout": 30,
	})
	return err
}

// TxID reports the replica's current transaction id for the durable-point
// report. A failure here is not fatal: the durable point is still meaningful
// without it, so the caller records an empty id rather than aborting a
// successful barrier.
func (l *Litestream) TxID(ctx context.Context) (string, error) {
	body, err := l.control(ctx, "/sync", map[string]any{
		"path":    l.DBPath,
		"wait":    true,
		"timeout": 30,
	})
	if err != nil {
		return "", err
	}
	var resp syncResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return "", fmt.Errorf("decode sync response: %w", err)
	}
	return resp.TXID, nil
}

func (l *Litestream) control(ctx context.Context, route string, body any) ([]byte, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	client := &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", l.SocketPath)
			},
		},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://litestream"+route, strings.NewReader(string(payload)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("litestream control %s: %w", route, err)
	}
	defer resp.Body.Close()
	data := make([]byte, 0, 4096)
	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		data = append(data, buf[:n]...)
		if err != nil || len(data) > 1<<20 {
			break
		}
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("litestream control %s: status %d: %s", route, resp.StatusCode, strings.TrimSpace(string(data)))
	}
	return data, nil
}

// Stop asks for a graceful shutdown — one SIGTERM, which makes `replicate`
// run its shutdown sync — and waits for the process. A second signal would
// make Litestream SKIP that sync (ErrShutdownInterrupted,
// cmd/litestream/main.go:186-190), so this never sends one; if the wait times
// out the process is killed and the caller reports the barrier incomplete.
func (l *Litestream) Stop(ctx context.Context) error {
	if l.cmd == nil || l.done == nil {
		return nil
	}
	if err := l.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		return fmt.Errorf("signal litestream: %w", err)
	}
	select {
	case err := <-l.done:
		l.done = nil
		if err != nil && !isSignalExit(err) {
			return fmt.Errorf("litestream shutdown: %w", err)
		}
		return nil
	case <-ctx.Done():
		_ = l.cmd.Process.Kill()
		<-l.done
		l.done = nil
		return fmt.Errorf("litestream did not finish its shutdown sync: %w", ctx.Err())
	}
}

func (l *Litestream) run(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, l.Bin, args...)
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func isSignalExit(err error) bool {
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		return false
	}
	status, ok := exitErr.Sys().(syscall.WaitStatus)
	return ok && status.Signaled()
}

func lastLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	return lines[len(lines)-1]
}
