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
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	pmount "github.com/juicedata/juicefs/pkg/plori/mount"
	prestore "github.com/juicedata/juicefs/pkg/plori/restore"
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
	opts := spec.Options(os.Getenv)
	if len(opts.Ignored) > 0 {
		// An option this worker does not know is the control-plane tuning
		// something it does not have, not authority it cannot honour, so it
		// is reported and ignored rather than refused.
		ploriLog("mount_options_ignored", "keys", strings.Join(opts.Ignored, ","))
	}
	if err := ls.WriteConfig(spec, opts); err != nil {
		exitTerminal(spec.StorageVolumeID, spec.FenceEpoch, pmount.Classify(err))
	}

	stop := make(chan os.Signal, 2)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)

	sup := &pmount.Supervisor{
		Spec:    spec,
		Paths:   paths,
		Options: opts,
		Deps: pmount.Deps{
			FS:                   &ploriFS{paths: paths, accessKey: accessKey, secretKey: secretKey, opts: opts},
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
	opts                 pmount.MountOptions
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
	// Everything `juicefs format` needs comes out of the spec's format block,
	// which the control-plane composed for this volume; Compression is left at
	// JuiceFS's default (none), which is the Plori profile. Validate has already
	// checked the block against the rest of the spec, so nothing here re-derives
	// a value the server sent.
	fs := spec.Format
	format := &meta.Format{
		Name:             spec.VolumeName(),
		UUID:             uuid.New().String(),
		Storage:          pmount.FormatStorage,
		Bucket:           fs.Bucket,
		Tiers:            object.NewTiers(""),
		BlockSize:        pmount.FormatBlockSizeKB,
		Capacity:         uint64(max64(fs.CapacityBytes, 0)),
		Inodes:           uint64(max64(fs.Inodes, 0)),
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
	c, err := ploriMountContext(f.opts, f.paths)
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

// IntegrityCheck runs SQLite's own check over the restored page image and,
// on the same pass, refuses a Format that must not be replicated onward.
// Litestream's restore-time check proves the LTX chain replays; this proves
// the database it produced is sound, credential-free (threat-model F-9) and
// has the trash the Rank 1 protocol depends on.
func (p *ploriVolume) IntegrityCheck(ctx context.Context) error {
	_, err := prestore.VerifyRestored(ctx, p.paths.MetaPath(), false)
	return err
}

// RepairAfterRestore is the restore-time missing-block repair. The scan reuses
// cmd/fsck.go's traversal shape without its full-prefix LIST, and the repair
// bounds each damaged file at its last readable byte and marks it; it never
// deletes anything.
func (p *ploriVolume) RepairAfterRestore(ctx context.Context) (pmount.RepairReport, error) {
	format, err := p.m.Load(false)
	if err != nil {
		return pmount.RepairReport{}, fmt.Errorf("load format for repair: %w", err)
	}
	skip, err := prestore.DeletedInos(meta.Background(), p.m)
	if err != nil {
		// A file already queued for deletion is allowed to have lost its
		// blocks. Without that list every such file looks like damage, so a
		// failure here is a reason to stop, not to over-report.
		return pmount.RepairReport{}, fmt.Errorf("list deleted inodes: %w", err)
	}
	scan, err := prestore.ScanMissingBlocks(ctx, p.m,
		object.WithPrefix(p.blob, "chunks/"),
		prestore.ScanOptions{Format: format, SkipInos: skip})
	if err != nil {
		return pmount.RepairReport{}, err
	}
	report := pmount.RepairReport{
		Scanned: scan.SlicesScanned,
		Checked: scan.BlocksChecked,
		Missing: len(scan.Missing),
		Files:   scan.InodesAffected,
		Elapsed: scan.Duration,
	}
	if len(scan.Missing) == 0 {
		return report, nil
	}
	q, err := prestore.Quarantine(ctx, p.m, scan.Missing, prestore.ModeTruncate, format)
	if err != nil {
		return report, err
	}
	for _, e := range q.Entries {
		if e.TruncatedTo != nil {
			report.Truncated++
		}
	}
	return report, nil
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
	// serveMount rather than mountMain: the FUSE loop runs IN THIS PROCESS
	// (cmd.mount() would re-exec itself at mount_unix.go:1016, which is the
	// second juicefs process in the M0 footprint measurement), and because
	// JuiceFS's own child supervisor is therefore gone, a session that ends
	// has to be reported to our caller rather than exiting as 1.
	return serveMount(p.v, p.cli)
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

// ApplyGrant writes the new ceiling into the metadata engine this process
// already holds. The mechanism, and why it needs neither a remount nor a second
// metadata client, is meta.PloriApplyGrant.
func (p *ploriVolume) ApplyGrant(_ context.Context, bytes, inodes int64) error {
	return meta.PloriApplyGrant(p.m, bytes, inodes)
}

// QuotaTrips is the engine's count of operations the volume ceiling has
// refused. The supervisor turns a movement in it into a Grow request on the
// next lease renewal (PLO-324).
func (p *ploriVolume) QuotaTrips() uint64 { return meta.PloriVolumeQuotaTrips() }

func (p *ploriVolume) FenceWrites() { meta.PloriFenceWrites() }
func (p *ploriVolume) Fenced() bool { return meta.PloriWritesFenced() }

// SetWriteExpiry arms the metadata engine's own deadline, so every gated
// operation re-checks it immediately before it runs (PLO-323 F-5).
func (p *ploriVolume) SetWriteExpiry(at time.Time) { meta.PloriSetWriteExpiry(at) }

// Detach unmounts without flushing: `fusermount -uz`, the lazy detach the
// plugin's own recycle path already uses. Unmount is the ordered way out; this
// is the out-of-band fence's, where the flush `umount --flush` performs would
// push staged bytes into a data prefix this writer no longer owns, and where
// the seal that has already been set would make that flush fail and leave the
// mount attached (PLO-323 F-1).
func (p *ploriVolume) Detach(context.Context) error {
	return doUmount(p.paths.MountPoint, true)
}

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

// ploriMountContext turns the resolved option vocabulary into the cli.Context
// the generic mount helpers read.
//
// The fixed half is not configurable and should not be: --backup-meta 0
// because Litestream is the metadata backup and the hourly dump was one of the
// two idle object writers, --no-usage-report because the mount is not a
// telemetry client, and an empty --metrics because one Prometheus listener per
// mount does not fit a node running many (health.json is the surface, and
// PLO-325 owns the rest).
func ploriMountContext(opts pmount.MountOptions, paths pmount.Paths) (*cli.Context, error) {
	set := flag.NewFlagSet("plori-mount", flag.ContinueOnError)
	set.SetOutput(devNull{})
	for _, f := range append(cmdMount().Flags, globalFlags()...) {
		if err := f.Apply(set); err != nil {
			return nil, err
		}
	}
	values := map[string]string{
		"backup-meta":     "0",
		"no-usage-report": "true",
		"cache-dir":       paths.CacheDir,
		"buffer-size":     strconv.Itoa(opts.BufferSizeMB),
		"heartbeat":       opts.Heartbeat.String(),
	}
	if opts.Writeback {
		values["writeback"] = "true"
	}
	if opts.AllowOther {
		// Through the -o string, not the AllowOther default: upstream ties
		// that default to uid 0 (pkg/fuse/fuse.go:485) while the explicit
		// option sets it at any uid (:500-501).
		values["o"] = "allow_other"
	}
	for name, value := range values {
		if set.Lookup(name) == nil {
			return nil, fmt.Errorf("%w: %q is not a flag of this client", pmount.ErrSpec, name)
		}
		if err := set.Set(name, value); err != nil {
			return nil, fmt.Errorf("%w: %s=%s: %s", pmount.ErrSpec, name, value, err)
		}
	}
	if err := set.Parse(nil); err != nil {
		return nil, err
	}
	return cli.NewContext(nil, set, nil), nil
}

type devNull struct{}

func (devNull) Write(p []byte) (int, error) { return len(p), nil }
