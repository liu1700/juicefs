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
	"regexp"
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

	// Env builds the environment every litestream process is started with. It
	// is a function rather than a slice because it is where the object key
	// enters the child, and the key can change while the worker runs: a
	// rotation restarts the child and this is read again (PLO-322). Nil means
	// the child inherits this process's environment unchanged, which is the
	// environment-variable path.
	Env func() []string

	// Log is the worker's structured logger. It carries the one decision this
	// type makes on its own — falling back from an unreachable restore TXID to
	// the timestamp of the same durable point — because a silent fallback
	// would be indistinguishable from the anchor having worked. Nil is silent,
	// which is what the tests that do not assert on it want.
	Log func(event string, kv ...any)

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

// txidPattern is `ltx.TXID.String()`: a fixed-width 16-digit hex number.
// `litestream restore -txid` rejects anything else outright
// (`ltx@v0.5.2/ltx.go:130-140`), and a durable point is machine-written, so a
// value that does not match is a broken contract rather than a stale anchor.
var txidPattern = regexp.MustCompile(`^[0-9a-f]{16}$`)

// errTxUnreachable is what the CLI prints when the restore plan cannot reach
// the requested transaction: the litestream error is `ErrTxNotAvailable`
// mapped to this sentence at cmd/litestream/restore.go:147. It is the one
// failure the TXID path recovers from — see Restore.
const errTxUnreachable = "no matching backup files available"

// Restore materialises the metadata database at the point `opt` names.
//
// Precedence is TXID, then timestamp, then the replica's latest transaction,
// and the two anchors are never passed together — v0.5.17 refuses that
// combination ("cannot specify index & timestamp to restore",
// `replica.go:612`), so preferring one means not sending the other.
//
// The empty-replica probe is deliberately never combined with an anchor.
// `-if-replica-exists` turns "no transaction matches" into a silent success
// (cmd/litestream/restore.go:143-149), and an anchor older than everything in
// the replica produces exactly that — so combining the two would let a too-old
// restore point look like a brand-new volume and be answered with a format,
// which is total data loss.
//
// One recovery, and only one: a TXID the restore plan cannot reach falls back
// to the timestamp of the same durable point. That is not defensive
// scaffolding, it is a mechanism that was reproduced — compaction merges the
// L0 files a recorded TXID was the boundary of into one file that STRADDLES
// it, `l0-retention` (30 m) then deletes the L0 originals, and from that
// moment `-txid` on the swallowed value fails permanently while the data is
// still there. Without the fallback, an Agent whose last durable point was
// mid-run and whose Pod comes back an hour later would exit 67 forever.
// Falling back cannot overshoot the durable point (a file is included iff it
// was encoded before `T_before`, and encoding follows every commit it
// carries), so the worst case is an older crash-consistent image, which is
// exactly what the unclean-generation repair already handles. The retry is
// safe to run in place because this failure happens while the restore PLAN is
// being calculated, before any output exists — verified: the failing run
// leaves no database behind, so the second attempt still meets litestream's
// "output path must not exist" precondition.
func (l *Litestream) Restore(ctx context.Context, sourcePrefix string, opt RestoreOptions) error {
	if _, err := os.Stat(l.DBPath); err == nil {
		return fmt.Errorf("refusing to restore over an existing %s", l.DBPath)
	}
	if sourcePrefix == "" {
		// No populated generation below this epoch: nothing has ever been
		// replicated for this volume.
		return ErrReplicaEmpty
	}
	if opt.TXID != "" && !txidPattern.MatchString(opt.TXID) {
		return fmt.Errorf("durable point carries an unparseable replica txid %q: want 16 hex digits", opt.TXID)
	}
	if err := l.writeRestoreConfig(l.spec, l.opts, sourcePrefix); err != nil {
		return err
	}
	defer os.Remove(l.restoreConfigPath())

	err := l.restoreAt(ctx, opt.TXID, opt.Timestamp)
	if err != nil && opt.TXID != "" && !opt.Timestamp.IsZero() && strings.Contains(err.Error(), errTxUnreachable) {
		l.logf("restore_txid_unreachable", "txid", opt.TXID, "falling_back_to", opt.Timestamp.UTC().Format(time.RFC3339Nano))
		err = l.restoreAt(ctx, "", opt.Timestamp)
	}
	if err != nil {
		return err
	}
	if _, err := os.Stat(l.DBPath); err != nil {
		if os.IsNotExist(err) {
			return ErrReplicaEmpty
		}
		return fmt.Errorf("stat restored database: %w", err)
	}
	return nil
}

// restoreAt runs one `litestream restore` with at most one anchor.
func (l *Litestream) restoreAt(ctx context.Context, txid string, timestamp time.Time) error {
	if out, err := l.run(ctx, restoreArgs(l.restoreConfigPath(), l.DBPath, l.DBPath, txid, timestamp)...); err != nil {
		return fmt.Errorf("litestream restore: %w: %s", err, lastLine(out))
	}
	return nil
}

// restoreArgs is the argv of one restore. It is a function of its own so the
// test that runs it against the REAL pinned binary builds the same command
// line the worker does, rather than a hand-copied one.
//
// `dbPath` is the positional argument, which selects WHICH database in the
// config is being restored; `outPath` is where the bytes land. The worker
// passes the same path for both — it restores the volume's own database into
// its own place — and only the test needs them to differ.
func restoreArgs(configPath, dbPath, outPath, txid string, timestamp time.Time) []string {
	args := []string{"restore", "-config", configPath, "-o", outPath, "-integrity-check", "full"}
	switch {
	case txid != "":
		args = append(args, "-txid", txid)
	case !timestamp.IsZero():
		// RFC3339Nano, not RFC3339: `Format(time.RFC3339)` truncates to the
		// second, and a truncated `T_before` excludes every LTX file encoded
		// earlier in that same second. Measured (PLO-396): the truncated form
		// restored one row where the full-precision form and the TXID both
		// restored two. Litestream parses either — `time.Parse(time.RFC3339,
		// …)` accepts the fractional seconds (cmd/litestream/restore.go:88).
		args = append(args, "-timestamp", timestamp.UTC().Format(time.RFC3339Nano))
	default:
		args = append(args, "-if-replica-exists")
	}
	return append(args, dbPath)
}

// Start launches continuous replication and waits for the control socket.
func (l *Litestream) Start(ctx context.Context) error {
	_ = os.Remove(l.SocketPath)
	cmd := exec.Command(l.Bin, "replicate", "-config", l.ConfigPath)
	cmd.Stdout = os.Stderr // one stream; the plugin reads the last stderr line
	cmd.Stderr = os.Stderr
	cmd.Env = l.env()
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

// syncResponse is v0.5.17's `POST /sync` body (`server.go:566-571`). Both ids
// are JSON NUMBERS on the wire — decoding `txid` into a string is a decode
// error, which is how this field spent its whole life empty: TxID returned
// "json: cannot unmarshal number into Go struct field", the supervisor logged
// `replica_txid_unavailable`, and every durable point went to the
// control-plane with no transaction id at all (PLO-396).
type syncResponse struct {
	// TXID is the LOCAL database's position after the sync.
	TXID uint64 `json:"txid"`
	// ReplicatedTXID is how far the replica has actually been pushed. It is
	// the one of the two a restore can name: an anchor the object store has
	// never seen is unreachable by definition.
	ReplicatedTXID uint64 `json:"replicated_txid"`
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
// report, in the 16-hex-digit form `litestream restore -txid` parses. A
// failure here is not fatal: the durable point is still meaningful without it,
// so the caller records an empty id rather than aborting a successful barrier.
//
// The value is the REPLICATED position, not the local one. They are equal
// after a `wait` sync, and where they are not, the difference is precisely the
// transactions the object store does not hold — which no restore could ever
// reach. Zero means nothing has been replicated yet, and that is reported as
// no id rather than as transaction zero.
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
	if resp.ReplicatedTXID == 0 {
		return "", nil
	}
	return fmt.Sprintf("%016x", resp.ReplicatedTXID), nil
}

func (l *Litestream) control(ctx context.Context, route string, body any) ([]byte, error) {
	return litestreamControl(ctx, l.SocketPath, route, body)
}

// litestreamControl posts one request to a Litestream control socket.
//
// It is a free function because two replicators speak this protocol: the
// per-mount child on its own socket in the state directory, and the
// node-level replicator on the socket the plugin owns (PLO-366). The only
// thing that differs is which socket, which is the argument.
func litestreamControl(ctx context.Context, socketPath, route string, body any) ([]byte, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	client := &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", socketPath)
			},
			// One connection per call. Control calls are minutes apart (a
			// barrier, a health tick, a stop), so pooling saves nothing, and a
			// pooled connection to a replicator that has since restarted is a
			// request that fails for a reason unrelated to what it asked --
			// which, on the node-level socket the plugin restarts under us, is
			// the failure mode this whole path exists to detect correctly.
			DisableKeepAlives: true,
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
		return nil, &controlStatusError{Route: route, Status: resp.StatusCode, Body: strings.TrimSpace(string(data))}
	}
	return data, nil
}

// controlStatusError is a non-200 from a control socket. The status code is
// kept rather than folded into prose because one of them is load-bearing: a
// 404 from the node-level replicator means it does not know this database,
// which is what a daemon restart looks like from a worker that is still
// running, and it is repaired by re-registering rather than by giving up.
type controlStatusError struct {
	Route  string
	Status int
	Body   string
}

func (e *controlStatusError) Error() string {
	return fmt.Sprintf("litestream control %s: status %d: %s", e.Route, e.Status, e.Body)
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

// Abort kills replication without letting it sync. SIGTERM would make
// litestream run its own shutdown sync, and a writer fenced out of band must
// not push its remaining LTX into the metadata prefix its successor restores
// from: no barrier ran, so that history can reference blocks the object store
// never received (PLO-323 F-1).
func (l *Litestream) Abort(ctx context.Context) error {
	if l.cmd == nil || l.done == nil {
		return nil
	}
	if err := l.cmd.Process.Kill(); err != nil {
		return fmt.Errorf("kill litestream: %w", err)
	}
	select {
	case <-l.done:
		l.done = nil
		return nil
	case <-ctx.Done():
		return fmt.Errorf("litestream did not exit after SIGKILL: %w", ctx.Err())
	}
}

func (l *Litestream) logf(event string, kv ...any) {
	if l.Log != nil {
		l.Log(event, kv...)
	}
}

// Probe reports whether this child is still replicating (PLO-411).
//
// It answers two different deaths with one call. The cheap one is the process
// itself: `done` is buffered and written by the reaper goroutine, so a
// non-blocking read of it sees an exit that nothing else in this process was
// watching — before PLO-411 that channel was read only by Stop and Abort, so
// a Litestream that died on its own was noticed by nobody. The other is a
// process that is alive but no longer serving, which only a round trip
// finds; `/sync` without `wait` is the cheapest one Litestream has, because
// it does the WAL-to-LTX step and leaves the upload to the replica monitor
// (store.go:428-431).
func (l *Litestream) Probe(ctx context.Context) error {
	if l.cmd == nil || l.done == nil {
		return errors.New("litestream is not running")
	}
	select {
	case err := <-l.done:
		l.done = nil
		if err == nil {
			return errors.New("litestream exited on its own with status 0")
		}
		return fmt.Errorf("litestream exited on its own: %w", err)
	default:
	}
	probe, cancel := context.WithTimeout(ctx, ProbeTimeout)
	defer cancel()
	_, err := l.control(probe, "/sync", map[string]any{"path": l.DBPath, "wait": false})
	return err
}

// Restart reaps whatever is left of the child and starts a new one.
//
// A restarted Litestream costs nothing that a crash would not already have
// cost: its position is on disk in the state directory, not in the process,
// so it compares local state against the replica and continues. The reap is
// unconditional because starting a second replicator on one database is the
// one outcome worse than a lagging replica.
func (l *Litestream) Restart(ctx context.Context) error {
	if l.cmd != nil && l.done != nil {
		_ = l.cmd.Process.Kill()
		select {
		case <-l.done:
		case <-ctx.Done():
			return fmt.Errorf("previous litestream did not exit, so a new one was NOT started: %w", ctx.Err())
		}
		l.done = nil
	}
	l.cmd = nil
	return l.Start(ctx)
}

func (l *Litestream) run(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, l.Bin, args...)
	cmd.Env = l.env()
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func (l *Litestream) env() []string {
	if l.Env == nil {
		return os.Environ()
	}
	return l.Env()
}

// ReloadCredentials restarts replication so the child picks up the object key
// the worker now holds.
//
// Litestream reads AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY once, at startup,
// and has no control-socket route that replaces them, so a restart is the only
// mechanism. What makes the restart free is that Litestream's position is on
// disk in the state directory, not in the process: a restarted child compares
// its local state against the replica and continues, which is exactly what it
// does after a crash. Nothing is lost that a crash would not already have to
// survive.
//
// The final sync before the restart is best effort on purpose. In the
// rotation this method exists for, the OLD key is already dead by the time the
// new one arrives — one key pair per subscription, regenerated wholesale
// (PLO-351) — so the sync will usually fail, and treating that as an error
// would turn every rotation into a stopped replicator. Whatever it could not
// push is still in the local WAL and goes out on the next sync under the new
// key.
func (l *Litestream) ReloadCredentials(ctx context.Context) error {
	if l.cmd == nil || l.done == nil {
		return nil // not replicating yet; Start will read the current key
	}
	sync, cancel := context.WithTimeout(ctx, 10*time.Second)
	syncErr := l.SyncAndWait(sync)
	cancel()

	stop, cancel := context.WithTimeout(ctx, 20*time.Second)
	stopErr := l.Stop(stop)
	cancel()
	if l.done != nil {
		// Stop returned without reaping — it could not even signal the child,
		// which usually means the child had already exited on its own. Reap it
		// if so; that is the whole of what is left to do.
		select {
		case <-l.done:
			l.done = nil
		default:
		}
	}
	if l.done != nil {
		// Still alive and Stop could not end it. Kill it: starting a second
		// replicator on one database is the one outcome worse than a lagging
		// replica. (Stop's own timeout branch already kills and reaps, so this
		// path is narrow.)
		abort, cancelAbort := context.WithTimeout(ctx, 10*time.Second)
		abortErr := l.Abort(abort)
		cancelAbort()
		if l.done != nil {
			return fmt.Errorf("could not stop litestream for a credential reload, so it was NOT restarted: %w (kill: %v)", stopErr, abortErr)
		}
	}
	// Reached only with the previous child reaped, which is the invariant that
	// keeps two SQLite writers off one database.
	if err := l.Start(ctx); err != nil {
		return fmt.Errorf("restart litestream after credential reload: %w (final sync: %v)", err, syncErr)
	}
	return nil
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
