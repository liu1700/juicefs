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

package cmd

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	pmount "github.com/juicedata/juicefs/pkg/plori/mount"
	"github.com/juicedata/juicefs/pkg/version"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/urfave/cli/v2"
)

// extraCommands registers the Plori distribution's own entry point. Every
// generic JuiceFS command is untouched and still usable (PLO-321 acceptance
// criterion 3).
func extraCommands() []*cli.Command { return []*cli.Command{cmdPloriMount()} }

func cmdPloriMount() *cli.Command {
	return &cli.Command{
		Name:     "plori-mount",
		Action:   ploriMount,
		Category: "SERVICE",
		Usage:    "Supervise one per-Agent volume: restore, mount, renew, fence and stop",
		Description: `Runs one Agent's JuiceFS volume for the lifetime of the process, in the
foreground, as a single process. It restores the metadata replica, proves the
mount opens the filesystem it was told to open, holds the writer lease, and
runs the ordered durability shutdown on SIGTERM.

See docs/en/development/plori_mount.md for the exit codes and the state
machine.`,
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "spec-file", Required: true, Usage: "path to the MountSpec JSON issued by the control-plane"},
			&cli.StringFlag{Name: "mount-point", Required: true, Usage: "kubelet target path to mount at"},
			&cli.StringFlag{Name: "state-dir", Required: true, Usage: "private directory for the metadata database, WAL and replication state"},
			&cli.StringFlag{Name: "cache-dir", Required: true, Usage: "JuiceFS writeback cache directory, one per volume"},
			&cli.StringFlag{Name: "control-plane-url", Required: true, Usage: "base URL of the control-plane"},
			&cli.StringFlag{Name: "token-file", Required: true, Usage: "projected ServiceAccount token, re-read on every call"},
			&cli.StringFlag{Name: "litestream-bin", Value: "litestream", Usage: "path to the pinned litestream binary"},
			&cli.StringFlag{Name: "log-format", Value: "json", Usage: "log format (json)"},
		},
	}
}

func ploriMount(c *cli.Context) error {
	paths := pmount.Paths{
		SpecFile:   c.String("spec-file"),
		MountPoint: c.String("mount-point"),
		StateDir:   c.String("state-dir"),
		CacheDir:   c.String("cache-dir"),
		TokenFile:  c.String("token-file"),
	}
	spec, err := pmount.LoadSpec(paths.SpecFile)
	if err != nil {
		exitTerminal("", 0, pmount.Classify(err))
	}
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if accessKey == "" || secretKey == "" {
		exitTerminal(spec.StorageVolumeID, spec.FenceEpoch, &pmount.Fatal{
			Exit:      pmount.CodeObjectStore,
			ErrCode:   pmount.ErrCodeObjectStoreUnreachable,
			Retryable: true,
			Err:       fmt.Errorf("no object credential in the worker environment; credential_source is %q", spec.ObjectStore.CredentialSource),
		})
	}

	ctx := context.Background()
	fencer, err := pmount.NewS3Fencer(ctx, spec.ObjectStore, accessKey, secretKey)
	if err != nil {
		exitTerminal(spec.StorageVolumeID, spec.FenceEpoch, pmount.Classify(err))
	}

	ls := &pmount.Litestream{
		Bin:        c.String("litestream-bin"),
		ConfigPath: filepath.Join(paths.StateDir, "litestream.yml"),
		SocketPath: filepath.Join(paths.StateDir, "litestream.sock"),
		DBPath:     paths.MetaPath(),
	}
	if err := os.MkdirAll(paths.StateDir, 0o700); err != nil {
		exitTerminal(spec.StorageVolumeID, spec.FenceEpoch, pmount.Classify(err))
	}
	if err := ls.WriteConfig(spec); err != nil {
		exitTerminal(spec.StorageVolumeID, spec.FenceEpoch, pmount.Classify(err))
	}

	stop := make(chan os.Signal, 2)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	sup := &pmount.Supervisor{
		Spec:  spec,
		Paths: paths,
		Deps: pmount.Deps{
			FS:                   &ploriFS{paths: paths, accessKey: accessKey, secretKey: secretKey},
			CP:                   pmount.NewClient(c.String("control-plane-url"), paths.TokenFile, 10*time.Second),
			Replicator:           ls,
			Fencer:               fencer,
			ControlGateInstalled: vfs.InternalMsgGateInstalled,
			Log:                  ploriLog,
		},
	}
	exitTerminal(spec.StorageVolumeID, spec.FenceEpoch, sup.Run(ctx, stop))
	return nil
}

func exitTerminal(volume string, epoch int64, f *pmount.Fatal) {
	if f == nil {
		os.Exit(pmount.CodeOK)
	}
	pmount.WriteTerminal(os.Stderr, volume, epoch, f)
	os.Exit(f.Exit)
}

func ploriLog(event string, kv ...any) {
	line := map[string]any{
		"ts":    time.Now().UTC().Format(time.RFC3339Nano),
		"level": "info",
		"event": event,
	}
	for i := 0; i+1 < len(kv); i += 2 {
		key, _ := kv[i].(string)
		line[key] = kv[i+1]
	}
	data, err := json.Marshal(line)
	if err != nil {
		return
	}
	fmt.Fprintln(os.Stderr, string(data))
}

// ---------------------------------------------------------------------------
// The JuiceFS half of the supervisor.
// ---------------------------------------------------------------------------

type ploriFS struct {
	paths                pmount.Paths
	accessKey, secretKey string
}

// metaURI is the local SQLite metadata engine. It is deliberately a plain
// path: the replicated database is the filesystem, and putting it anywhere but
// the private state directory would expose it to the Agent.
func (f *ploriFS) metaURI() string { return "sqlite3://" + f.paths.MetaPath() }

// credentialPatch injects the object key into an in-memory Format immediately
// before the storage client is built, and never anywhere else.
//
// cmd.NewReloadableStorage runs `patch` before createStorage and again on
// every OnReload (mount.go:461-483), which is what keeps the key out of the
// replicated SQLite: the stored Format has empty AccessKey/SecretKey and this
// function supplies them for the lifetime of one storage client only.
// KeyEncrypted is cleared as well — Format.Decrypt() would otherwise try to
// AES-open the plaintext we just injected (pkg/meta/config.go:263-296).
func (f *ploriFS) credentialPatch() func(*meta.Format) {
	return func(fmtp *meta.Format) {
		fmtp.AccessKey = f.accessKey
		fmtp.SecretKey = f.secretKey
		fmtp.SessionToken = ""
		fmtp.KeyEncrypted = false
	}
}

// Format creates the filesystem. The persisted Format carries no credential:
// object.CreateStorage is handed empty keys and the AWS SDK resolves them from
// this process's environment for the duration of the format only.
func (f *ploriFS) Format(ctx context.Context, spec *pmount.MountSpec) error {
	fs := spec.EffectiveFormat()
	blockSize := fs.BlockSizeKB
	if blockSize == 0 {
		blockSize = 4096
	}
	format := &meta.Format{
		Name:             spec.VolumeName(),
		UUID:             uuid.New().String(),
		Storage:          fs.Storage,
		Bucket:           spec.ObjectStore.Endpoint + "/" + spec.ObjectStore.Bucket,
		Tiers:            object.NewTiers(""),
		BlockSize:        blockSize,
		Compression:      fs.Compression,
		Capacity:         uint64(max64(spec.Grant.Bytes, 0)),
		Inodes:           uint64(max64(spec.Grant.Inodes, 0)),
		TrashDays:        fs.TrashDays,
		DirStats:         true,
		MetaVersion:      meta.MaxVersion,
		MinClientVersion: "1.1.0-A",
	}
	if format.TrashDays < 1 {
		return fmt.Errorf("refusing to format with trash-days %d", format.TrashDays)
	}
	m := meta.NewClient(f.metaURI(), nil)
	defer m.Shutdown() //nolint:errcheck
	blob, err := NewReloadableStorage(format, m, f.credentialPatch())
	if err != nil {
		return fmt.Errorf("object storage: %w", err)
	}
	defer object.Shutdown(blob)
	if err := test(ctx, blob); err != nil {
		return fmt.Errorf("object storage is not usable: %w", err)
	}
	if err := blob.Put(ctx, "juicefs_uuid", strings.NewReader(format.UUID)); err != nil {
		return fmt.Errorf("write juicefs_uuid: %w", err)
	}
	// Init persists the format. The pointer handed to it is the one the patch
	// mutated, so the credential is stripped back out first; the assertion
	// below is what keeps a future edit from re-introducing the leak.
	stored := *format
	stored.AccessKey, stored.SecretKey, stored.SessionToken = "", "", ""
	stored.KeyEncrypted = false
	if err := m.Init(&stored, false); err != nil {
		_ = blob.Delete(ctx, "juicefs_uuid")
		return fmt.Errorf("init metadata: %w", err)
	}
	return nil
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// Open loads the restored metadata and builds the whole mount stack in this
// process. It deliberately does NOT go through cmd.mount(): that path always
// re-execs itself (mount_unix.go:1014 `exec.Command(path, os.Args[1:]...)`),
// which is where the second juicefs process in the M0 RSS measurement comes
// from. One volume per process means one process.
func (f *ploriFS) Open(ctx context.Context, spec *pmount.MountSpec) (pmount.Volume, error) {
	c, err := ploriMountContext(spec, f.paths)
	if err != nil {
		return nil, err
	}
	metaConf := getMetaConf(c, f.paths.MountPoint, false)
	m := meta.NewClient(f.metaURI(), metaConf)
	format, err := m.Load(true)
	if err != nil {
		return nil, fmt.Errorf("load format: %w", err)
	}
	chunkConf := getChunkConf(c, format)
	vfsConf := getVfsConf(c, metaConf, format, chunkConf)
	setFuseOption(c, format, vfsConf)
	blob, err := NewReloadableStorage(format, m, f.credentialPatch())
	if err != nil {
		return nil, fmt.Errorf("object storage: %w", err)
	}
	registerer, registry := wrapRegister(c, f.paths.MountPoint, format.Name)
	store := chunk.NewCachedStore(blob, *chunkConf, registerer)
	registerMetaMsg(m, store, chunkConf)
	return &ploriVolume{
		paths:    f.paths,
		cli:      c,
		m:        m,
		blob:     blob,
		store:    store,
		vfsConf:  vfsConf,
		registry: registry,
		reg:      registerer,
		identity: pmount.FormatIdentity{
			Name:      format.Name,
			UUID:      format.UUID,
			TrashDays: format.TrashDays,
			Capacity:  format.Capacity,
			Inodes:    format.Inodes,
		},
	}, nil
}

type ploriVolume struct {
	paths     pmount.Paths
	cli       *cli.Context
	m         meta.Meta
	blob      object.ObjectStorage
	store     chunk.ChunkStore
	vfsConf   *vfs.Config
	registry  *prometheus.Registry
	reg       prometheus.Registerer
	identity  pmount.FormatIdentity
	v         *vfs.VFS
	sessioned bool
}

func (p *ploriVolume) Identity() pmount.FormatIdentity { return p.identity }

// IntegrityCheck runs SQLite's own check over the restored page image.
// Litestream's restore-time check proves the LTX chain replays; this proves
// the database it produced is not corrupt.
func (p *ploriVolume) IntegrityCheck(ctx context.Context) error {
	db, err := sql.Open("sqlite3", p.paths.MetaPath())
	if err != nil {
		return fmt.Errorf("open metadata for integrity check: %w", err)
	}
	defer db.Close()
	var result string
	if err := db.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&result); err != nil {
		return fmt.Errorf("integrity_check: %w", err)
	}
	if result != "ok" {
		return fmt.Errorf("integrity_check reported %q", result)
	}
	return nil
}

func (p *ploriVolume) StoredUUID(ctx context.Context) (string, error) {
	r, err := p.blob.Get(ctx, "juicefs_uuid", 0, -1)
	if err != nil {
		return "", err
	}
	defer r.Close()
	buf := make([]byte, 64)
	n, _ := r.Read(buf)
	return strings.TrimSpace(string(buf[:n])), nil
}

func (p *ploriVolume) PurgeSessions(ctx context.Context) (int, error) {
	return meta.PloriPurgeAllSessions(p.m)
}

func (p *ploriVolume) Serve(ctx context.Context) error {
	if st := p.m.Chroot(meta.Background(), p.vfsConf.Meta.Subdir); st != 0 {
		return st
	}
	if err := p.m.NewSession(true); err != nil {
		return fmt.Errorf("new session: %w", err)
	}
	p.sessioned = true
	p.m.OnReload(func(fmtp *meta.Format) {
		p.store.UpdateLimit(fmtp.UploadLimit, fmtp.DownloadLimit)
	})
	p.v = vfs.NewVFS(p.vfsConf, p.m, p.store, p.reg, p.registry)
	p.v.UpdateFormat = updateFormat(p.cli)
	logger.Infof("JuiceFS version %s, plori-mount serving %s", version.Version(), p.identity.Name)
	mountMain(p.v, p.cli)
	return nil
}

// AwaitMounted proves the filesystem is serving without issuing a syscall
// against our own mount point. A stat from this process would be answered by
// this process, which is exactly the shape that hangs when the FUSE session
// is not up yet; the mount table plus an in-process root lookup proves the
// same two facts with neither risk.
func (p *ploriVolume) AwaitMounted(ctx context.Context) error {
	deadline := time.Now().Add(2 * time.Minute)
	for {
		if p.v != nil && mountedAt(p.paths.MountPoint) {
			var attr meta.Attr
			if st := p.m.GetAttr(meta.Background(), meta.RootInode, &attr); st == 0 {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("mount point %s did not appear within 2m", p.paths.MountPoint)
		}
	}
}

// mountedAt reads the process's own mount table rather than touching the
// mount point.
func mountedAt(mp string) bool {
	data, err := os.ReadFile("/proc/self/mounts")
	if err != nil {
		return false
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 3 && fields[1] == mp && strings.HasPrefix(fields[2], "fuse") {
			return true
		}
	}
	return false
}

// Barrier is the in-process form of `juicefs durability`.
//
// The CLI form goes through the `.control` file (cmd/durability.go
// requestRemoteDurability), which means opening a file inside the Agent's own
// mount. Running in the same process removes both the round trip and the
// reason to keep `.control` reachable at all.
func (p *ploriVolume) Barrier(ctx context.Context) (pmount.BarrierResult, error) {
	store, ok := p.store.(chunk.RemoteDurabilityStore)
	if !ok {
		return pmount.BarrierResult{}, fmt.Errorf("chunk store does not support a remote durability barrier")
	}
	if p.v != nil {
		if err := p.v.FlushAll(""); err != nil {
			return pmount.BarrierResult{}, fmt.Errorf("flush buffered data: %w", err)
		}
	}
	status, err := store.RemoteDurability(ctx)
	res := pmount.BarrierResult{BarrierAt: time.Now().UTC(), PendingBlocks: status.PendingBlocks}
	return res, err
}

func (p *ploriVolume) PendingBlocks() uint64 {
	if store, ok := p.store.(chunk.RemoteDurabilityStore); ok {
		return store.RemoteDurabilityStatus().PendingBlocks
	}
	return 0
}

func (p *ploriVolume) Usage(ctx context.Context) (pmount.Usage, error) {
	var total, avail, iused, iavail uint64
	if st := p.m.StatFS(meta.Background(), meta.RootInode, &total, &avail, &iused, &iavail); st != 0 {
		return pmount.Usage{}, st
	}
	return pmount.Usage{Bytes: int64(total - avail), Inodes: int64(iused)}, nil
}

// ApplyGrant writes the new ceiling. It reloads the Format from the engine
// rather than persisting the in-memory one, because the in-memory one has been
// through credentialPatch and persisting it would write the bucket-wide key
// into the database Litestream replicates.
func (p *ploriVolume) ApplyGrant(ctx context.Context, bytes, inodes int64) error {
	stored, err := p.m.Load(false)
	if err != nil {
		return fmt.Errorf("reload format: %w", err)
	}
	next := *stored
	next.Capacity = uint64(max64(bytes, 0))
	next.Inodes = uint64(max64(inodes, 0))
	next.AccessKey, next.SecretKey, next.SessionToken = "", "", ""
	next.KeyEncrypted = false
	return p.m.Init(&next, false)
}

func (p *ploriVolume) FenceWrites() { meta.PloriFenceWrites() }
func (p *ploriVolume) Fenced() bool { return meta.PloriWritesFenced() }

// Unmount detaches with `umount --flush` semantics: the caller has already run
// the barrier, and a failure here is fail-closed rather than best effort
// (cmd/umount.go:120-125 keeps the same rule for the CLI).
func (p *ploriVolume) Unmount(ctx context.Context) error {
	if p.v != nil {
		if err := p.v.FlushAll(""); err != nil {
			return fmt.Errorf("flush all delayed data: %w", err)
		}
	}
	return doUmount(p.paths.MountPoint, true)
}

func (p *ploriVolume) Close() error {
	var err error
	if p.sessioned {
		err = p.m.CloseSession()
	}
	object.Shutdown(p.blob)
	if e := p.m.Shutdown(); err == nil {
		err = e
	}
	return err
}

// ---------------------------------------------------------------------------
// Turning the MountSpec into the mount command's flags.
// ---------------------------------------------------------------------------

// ploriDefaults are the client settings applied when the spec's mount_options
// do not name them. They come from PLO-316 wave 2
// (docs/design/per-agent-juicefs/benchmark-wave2-object-ops.md): heartbeat 300
// and backup-meta 0 are what take an idle mount down to 0.018 object ops/s,
// because the 12-second heartbeat and the hourly metadata dump were the only
// JuiceFS-side idle object writers. Litestream is the metadata backup now, so
// the dump is redundant as well as expensive.
var ploriDefaults = map[string]string{
	"writeback":       "true",
	"heartbeat":       "300s",
	"backup-meta":     "0",
	"no-usage-report": "true",
	"metrics":         "",
	"consul":          "",
}

// ploriMountContext builds the cli.Context the generic mount helpers read.
//
// Each mount_options entry is one flag of the `juicefs mount` command in
// `--name=value` or `--name` form. An unrecognised entry is refused rather
// than ignored: the list is server-built (threat-model R14), so an entry this
// worker does not understand means the two sides disagree about what the mount
// is, which exit code 64 exists for.
func ploriMountContext(spec *pmount.MountSpec, paths pmount.Paths) (*cli.Context, error) {
	set := flag.NewFlagSet("plori-mount", flag.ContinueOnError)
	set.SetOutput(devNull{})
	for _, f := range append(cmdMount().Flags, globalFlags()...) {
		if err := f.Apply(set); err != nil {
			return nil, err
		}
	}
	apply := func(name, value string) error {
		if set.Lookup(name) == nil {
			return fmt.Errorf("%w: mount option %q is not a flag of this client", pmount.ErrSpec, name)
		}
		return set.Set(name, value)
	}
	for name, value := range ploriDefaults {
		if value == "" {
			continue
		}
		if err := apply(name, value); err != nil {
			return nil, err
		}
	}
	if err := apply("cache-dir", paths.CacheDir); err != nil {
		return nil, err
	}
	for _, opt := range spec.EffectiveMountOptions(os.Getenv) {
		name, value, hasValue := strings.Cut(strings.TrimLeft(opt, "-"), "=")
		if !hasValue {
			value = "true"
		}
		if err := apply(name, value); err != nil {
			return nil, err
		}
	}
	if err := set.Parse(nil); err != nil {
		return nil, err
	}
	return cli.NewContext(nil, set, nil), nil
}

type devNull struct{}

func (devNull) Write(p []byte) (int, error) { return len(p), nil }
