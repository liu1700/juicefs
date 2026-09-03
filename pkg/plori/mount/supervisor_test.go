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
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// ------------------------------------------------------------------ fakes ---

type fakeVolume struct {
	mu          sync.Mutex
	id          FormatIdentity
	storedID    string
	storedErr   error
	integrity   error
	purgeErr    error
	purged      int
	repair      RepairReport
	repairErr   error
	repaired    int
	barrier     func(context.Context) (BarrierResult, error)
	usage       Usage
	usageErr    error
	usageReads  int
	trashWalks  int
	fenced      bool
	writeExpiry time.Time
	calls       []string
	serve       chan error
	grants      [][2]int64
	grantErr    error
	quotaTrips  atomic.Uint64
	// pending is the writeback backlog the fake reports. A settable number
	// rather than a constant zero because the whole of PLO-383 is what the
	// supervisor does when it is NOT zero.
	pending atomic.Uint64
	// backlogCaps is every cap the supervisor pushed down, in order.
	backlogCaps []int64
}

func (f *fakeVolume) record(name string) {
	f.mu.Lock()
	f.calls = append(f.calls, name)
	f.mu.Unlock()
}

func (f *fakeVolume) order() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func (f *fakeVolume) Identity() FormatIdentity                   { return f.id }
func (f *fakeVolume) IntegrityCheck(context.Context) error       { return f.integrity }
func (f *fakeVolume) StoredUUID(context.Context) (string, error) { return f.storedID, f.storedErr }
func (f *fakeVolume) RepairAfterRestore(context.Context) (RepairReport, error) {
	f.record("repair")
	f.mu.Lock()
	f.repaired++
	f.mu.Unlock()
	return f.repair, f.repairErr
}
func (f *fakeVolume) PurgeSessions(context.Context) (int, error) {
	f.record("purge_sessions")
	return f.purged, f.purgeErr
}
func (f *fakeVolume) Serve(ctx context.Context) error {
	if f.serve == nil {
		<-ctx.Done()
		return nil
	}
	return <-f.serve
}
func (f *fakeVolume) AwaitMounted(context.Context) error { return nil }
func (f *fakeVolume) Barrier(ctx context.Context) (BarrierResult, error) {
	f.record("barrier")
	if f.barrier != nil {
		return f.barrier(ctx)
	}
	return BarrierResult{BarrierAt: time.Now().UTC()}, nil
}
func (f *fakeVolume) PendingBlocks() uint64 { return f.pending.Load() }
func (f *fakeVolume) SetStagingBacklogCap(blocks int64) {
	f.mu.Lock()
	f.backlogCaps = append(f.backlogCaps, blocks)
	f.mu.Unlock()
}

// caps is every staging-backlog cap the supervisor pushed down, in order.
func (f *fakeVolume) caps() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int64(nil), f.backlogCaps...)
}

// lastCap is the cap in force, or -1 if none was ever pushed.
func (f *fakeVolume) lastCap() int64 {
	c := f.caps()
	if len(c) == 0 {
		return -1
	}
	return c[len(c)-1]
}

// Usage counts its two halves separately, because the whole of PLO-427's
// second half is that they run at different cadences: the totals on every
// health tick, the trash walk on the report's interval.
func (f *fakeVolume) Usage(_ context.Context, withTrash bool) (Usage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.usageReads++
	u := f.usage
	if !withTrash {
		// What ploriVolume returns when the caller did not ask for the walk:
		// totals only, and the "nobody measured it" shape for the rest.
		u.TrashKnown, u.TrashBytes, u.TrashInodes, u.TrashPartial = false, 0, 0, false
		return u, f.usageErr
	}
	f.trashWalks++
	return u, f.usageErr
}

// usageReads is how many times the totals were read; trashWalks how many of
// those also walked the trash.
func (f *fakeVolume) usageCounts() (reads, walks int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.usageReads, f.trashWalks
}

// setUsage moves the volume under a running supervisor, which is what a writing
// Agent does.
func (f *fakeVolume) setUsage(u Usage, err error) {
	f.mu.Lock()
	f.usage, f.usageErr = u, err
	f.mu.Unlock()
}
func (f *fakeVolume) ApplyGrant(_ context.Context, bytes, inodes int64) error {
	f.record("apply_grant")
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.grantErr != nil {
		return f.grantErr
	}
	f.grants = append(f.grants, [2]int64{bytes, inodes})
	return nil
}

// QuotaTrips is the metadata engine's refusal counter. The double exposes it as
// a settable number rather than a "the volume is full" flag for the same reason
// the engine does: the supervisor has to distinguish a NEW refusal from an old
// one, and only a monotonic counter carries that (PLO-324).
func (f *fakeVolume) QuotaTrips() uint64 { return f.quotaTrips.Load() }

// appliedGrants is every ceiling ApplyGrant was asked for, in order.
func (f *fakeVolume) appliedGrants() [][2]int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][2]int64(nil), f.grants...)
}
func (f *fakeVolume) FenceWrites() {
	f.mu.Lock()
	if !f.fenced {
		f.fenced = true
		f.calls = append(f.calls, "fence")
	}
	f.mu.Unlock()
}
func (f *fakeVolume) Fenced() bool                  { f.mu.Lock(); defer f.mu.Unlock(); return f.fenced }
func (f *fakeVolume) Unmount(context.Context) error { f.record("unmount"); return nil }
func (f *fakeVolume) Detach(context.Context) error  { f.record("detach"); return nil }
func (f *fakeVolume) Close() error                  { f.record("close"); return nil }
func (f *fakeVolume) SetWriteExpiry(at time.Time) {
	f.mu.Lock()
	f.writeExpiry = at
	f.mu.Unlock()
}

type fakeFS struct {
	vol       *fakeVolume
	openErr   error
	formatted bool
	formatErr error
}

func (f *fakeFS) Format(context.Context, *MountSpec) error { f.formatted = true; return f.formatErr }
func (f *fakeFS) Open(context.Context, *MountSpec) (Volume, error) {
	if f.openErr != nil {
		return nil, f.openErr
	}
	return f.vol, nil
}

type fakeCP struct {
	mu         sync.Mutex
	calls      []string
	renewErr   error
	expiry     func() time.Time
	grant      GrantSpec
	overBudget bool
	// onGrow is the allocator: it answers a RenewRequest carrying Grow with
	// whatever the account can fund. nil means an allocator that never moves.
	onGrow func(GrantSpec) GrantSpec
	// renewEcho rewrites the fencing echo the renew answer carries. nil is the
	// honest control-plane: it echoes back the volume and epoch the request
	// presented, which is the only answer the worker will accept (PLO-520).
	renewEcho func(volumeID string, epoch int64) (string, int64)
	renews    []RenewRequest
	released  string
	// ackErr is what /format-ack answers with; nil is a control-plane that
	// records the UUID. acks is every attempt, so a test can assert both what
	// was reported and how many times it was tried.
	ackErr    error
	acks      []formatAck
	readyPath string
	// durableTxID is the anchor the worker last reported, kept so a test can
	// assert that the local durable-point file and the control-plane were told
	// the same instant (PLO-416).
	durableTxID string
	// usages is every usage figure the worker reported, in order.
	usages []Usage
}

// formatAck is one observed /format-ack call. readyExists is the point of it:
// the plugin publishes the volume when the ready file appears, so an ack that
// arrives after it is an ack the Agent did not wait for.
type formatAck struct {
	volume      string
	epoch       int64
	uuid        string
	readyExists bool
}

func (c *fakeCP) record(name string) { c.mu.Lock(); c.calls = append(c.calls, name); c.mu.Unlock() }
func (c *fakeCP) order() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.calls...)
}

// The echo is not decoration: since PLO-520 the worker refuses a renew answer
// whose storage_volume_id / fence_epoch are not the ones it presented, so a
// double that answered without them would fence every mount it serves. Echoing
// the arguments is also what the real control-plane does — issuer.go answers
// vol.ID and the renewed lease's epoch, having refused any other epoch before
// it got there.
func (c *fakeCP) RenewLease(_ context.Context, volumeID string, epoch int64, req RenewRequest) (LeaseResponse, error) {
	c.record("renew")
	c.mu.Lock()
	c.renews = append(c.renews, req)
	if req.Grow && c.onGrow != nil {
		c.grant = c.onGrow(c.grant)
	}
	if req.AckedGrantEpoch > c.grant.AckedEpoch {
		c.grant.AckedEpoch = req.AckedGrantEpoch
	}
	grant, overBudget := c.grant, c.overBudget
	c.mu.Unlock()
	if c.renewErr != nil {
		return LeaseResponse{}, c.renewErr
	}
	exp := time.Now().UTC().Add(2 * time.Minute)
	if c.expiry != nil {
		exp = c.expiry()
	}
	echoVolume, echoEpoch := volumeID, epoch
	if c.renewEcho != nil {
		echoVolume, echoEpoch = c.renewEcho(volumeID, epoch)
	}
	return LeaseResponse{
		StorageVolumeID: echoVolume,
		FenceEpoch:      echoEpoch,
		LeaseExpiresAt:  exp,
		Grant:           grant,
		OverBudget:      overBudget,
	}, nil
}

// renewRequests is every RenewRequest the worker sent, in order.
func (c *fakeCP) renewRequests() []RenewRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]RenewRequest(nil), c.renews...)
}
func (c *fakeCP) ReleaseLease(_ context.Context, _ string, _ int64, reason string) error {
	c.mu.Lock()
	c.calls = append(c.calls, "release")
	c.released = reason
	c.mu.Unlock()
	return nil
}
func (c *fakeCP) ReportUsage(_ context.Context, _ string, _ int64, u Usage, _ time.Time) error {
	c.mu.Lock()
	c.calls = append(c.calls, "usage")
	c.usages = append(c.usages, u)
	c.mu.Unlock()
	return nil
}

// reportedUsages is every figure the worker posted to /usage, in order. It is
// what health.json is checked against: the two must never disagree (PLO-427).
func (c *fakeCP) reportedUsages() []Usage {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Usage(nil), c.usages...)
}
func (c *fakeCP) ReportDurablePoint(_ context.Context, _ string, _ int64, _ BarrierResult, txid string) error {
	c.mu.Lock()
	c.durableTxID = txid
	c.mu.Unlock()
	c.record("durable_point")
	return nil
}

func (c *fakeCP) AckFormat(_ context.Context, volumeID string, epoch int64, uuid string) (VolumeStateResponse, error) {
	ready := false
	if c.readyPath != "" {
		_, err := os.Stat(c.readyPath)
		ready = err == nil
	}
	c.mu.Lock()
	c.calls = append(c.calls, "format_ack")
	c.acks = append(c.acks, formatAck{volume: volumeID, epoch: epoch, uuid: uuid, readyExists: ready})
	err := c.ackErr
	c.mu.Unlock()
	if err != nil {
		return VolumeStateResponse{}, err
	}
	return VolumeStateResponse{State: VolumeStateActive}, nil
}

// formatAcks is every /format-ack the worker made, in order.
func (c *fakeCP) formatAcks() []formatAck {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]formatAck(nil), c.acks...)
}

type fakeReplicator struct {
	mu           sync.Mutex
	calls        []string
	restoreErr   error
	syncErr      error
	restoredFrom string
}

func (r *fakeReplicator) record(n string) { r.mu.Lock(); r.calls = append(r.calls, n); r.mu.Unlock() }
func (r *fakeReplicator) order() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}
func (r *fakeReplicator) Restore(_ context.Context, sourcePrefix string, _ RestoreOptions) error {
	r.record("restore")
	r.mu.Lock()
	r.restoredFrom = sourcePrefix
	r.mu.Unlock()
	return r.restoreErr
}
func (r *fakeReplicator) Start(context.Context) error       { r.record("start"); return nil }
func (r *fakeReplicator) SyncAndWait(context.Context) error { r.record("sync"); return r.syncErr }
func (r *fakeReplicator) TxID(context.Context) (string, error) {
	return "0000000000000009", nil
}
func (r *fakeReplicator) Stop(context.Context) error  { r.record("stop"); return nil }
func (r *fakeReplicator) Abort(context.Context) error { r.record("abort"); return nil }

type fakeFencer struct {
	err    error
	prior  string
	marker FenceMarker
	getErr error
}

func (f *fakeFencer) Claim(context.Context, string, []byte) error { return f.err }
func (f *fakeFencer) ReadMarker(context.Context, string) (FenceMarker, error) {
	return f.marker, f.getErr
}
func (f *fakeFencer) PriorMetaPrefix(_ context.Context, root string, epoch int64) (string, error) {
	if f.prior != "" {
		return f.prior, nil
	}
	return "", nil
}

// ------------------------------------------------------------------ setup ---

func testSpec() *MountSpec {
	spec, err := func() (*MountSpec, error) {
		var s MountSpec
		if err := json.Unmarshal([]byte(validSpecJSON), &s); err != nil {
			return nil, err
		}
		return &s, s.Validate()
	}()
	if err != nil {
		panic(err)
	}
	spec.LeaseExpiresAt = time.Now().UTC().Add(2 * time.Minute)
	spec.LeaseRenewInterval = Duration(50 * time.Millisecond)
	spec.WriteStopMargin = Duration(300 * time.Millisecond)
	return spec
}

// bootstrapSpec is a brand-new Agent's first mount: an `allocating` volume whose
// lease IS the formatting lease (PLO-373). The three fields move together —
// `may_format` is the authorisation, and it is granted exactly when no
// Format.UUID has been recorded, in either of the two places the wire spells it.
func bootstrapSpec() *MountSpec {
	spec := testSpec()
	spec.Generation = 1
	spec.VolumeState = VolumeStateAllocating
	spec.MayFormat = true
	spec.FormatUUID = ""
	spec.Format.ExpectedUUID = ""
	if err := spec.Validate(); err != nil {
		panic(err)
	}
	return spec
}

func newSup(t *testing.T, spec *MountSpec, fs *fakeFS, cp *fakeCP, rep *fakeReplicator, fencer Fencer) *Supervisor {
	t.Helper()
	dir := t.TempDir()
	sup := &Supervisor{
		Spec:    spec,
		Paths:   Paths{StateDir: filepath.Join(dir, "state"), CacheDir: filepath.Join(dir, "cache"), MountPoint: filepath.Join(dir, "mnt")},
		Options: MountOptions{BarrierInterval: 30 * time.Millisecond},
		Deps: Deps{
			FS: fs, CP: cp, Replicator: rep, Fencer: fencer,
			ControlGateInstalled: func() bool { return true },
		},
	}
	if cp != nil {
		cp.readyPath = sup.Paths.ReadyPath()
	}
	return sup
}

// stopOnReady delivers one SIGTERM the instant the worker publishes its mount.
//
// Since PLO-393 F-3 a signal that arrives BEFORE the mount is up ABORTS the
// startup rather than waiting behind it, so "queue a TERM and let the run loop
// take it on its first pass" no longer means what it did — it now means the
// abort, which TestATermArrivingBeforeTheMountIsUpAbortsTheStartup owns. This
// keeps the old idiom's determinism, with no sleep, by hanging the send off the
// `ready` log event: the worker emits it immediately after writing the ready
// file and immediately before entering the run loop.
func stopOnReady(sup *Supervisor) chan os.Signal {
	ch := make(chan os.Signal, 2)
	prev := sup.Deps.Log
	sup.Deps.Log = func(event string, kv ...any) {
		if prev != nil {
			prev(event, kv...)
		}
		if event == "ready" {
			ch <- syscall.SIGTERM
		}
	}
	return ch
}

func healthyVolume() *fakeVolume {
	return &fakeVolume{
		id:       FormatIdentity{Name: "agents/550e8400-e29b-41d4-a716-446655440000", UUID: "6c1e5f2c-0f0a-4a1c-9f2d-2b4e6a8c0d1e", TrashDays: 1},
		storedID: "6c1e5f2c-0f0a-4a1c-9f2d-2b4e6a8c0d1e",
	}
}

// ------------------------------------------------------------------ tests ---

// A 412 on the fence marker means another writer reached this epoch. The
// contract maps that to exit 66, and the lease is handed back so the Agent
// does not wait a full TTL for a mount that never happened.
func TestFenceMarkerConflictExitsFenced(t *testing.T) {
	cp := &fakeCP{}
	sup := newSup(t, testSpec(), &fakeFS{vol: healthyVolume()}, cp, &fakeReplicator{}, &fakeFencer{err: ErrFenceMarkerHeld})
	got := sup.Run(context.Background(), make(chan os.Signal))
	if got.Exit != CodeFenced {
		t.Fatalf("exit = %d, want %d (%v)", got.Exit, CodeFenced, got.Err)
	}
	if got.ErrCode != ErrCodeFenceMarkerHeld {
		t.Errorf("error code = %s, want %s", got.ErrCode, ErrCodeFenceMarkerHeld)
	}
	// A marker held by somebody else is the startup half of "this volume is
	// not ours", so the lease goes back with the out-of-band reason rather
	// than the deadline one (PLO-323 F-1).
	if cp.released != ReasonFencedOutOfBand {
		t.Errorf("release reason = %q, want %q", cp.released, ReasonFencedOutOfBand)
	}
}

func TestIdentityMismatchExits65AndTellsTheControlPlane(t *testing.T) {
	tests := map[string]func(*fakeVolume){
		"format name is another volume":     func(v *fakeVolume) { v.id.Name = "agents/some-other-volume" },
		"format uuid is another filesystem": func(v *fakeVolume) { v.id.UUID = "11111111-1111-1111-1111-111111111111" },
		"data prefix belongs to another":    func(v *fakeVolume) { v.storedID = "22222222-2222-2222-2222-222222222222" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			vol := healthyVolume()
			mutate(vol)
			cp := &fakeCP{}
			sup := newSup(t, testSpec(), &fakeFS{vol: vol}, cp, &fakeReplicator{}, &fakeFencer{})
			got := sup.Run(context.Background(), make(chan os.Signal))
			if got.Exit != CodeIdentityMismatch {
				t.Fatalf("exit = %d, want %d (%v)", got.Exit, CodeIdentityMismatch, got.Err)
			}
			if cp.released != "identity_mismatch" {
				t.Errorf("release reason = %q, want identity_mismatch", cp.released)
			}
		})
	}
}

func TestStartupRefusalsExit70(t *testing.T) {
	t.Run("trash days disabled", func(t *testing.T) {
		vol := healthyVolume()
		vol.id.TrashDays = 0
		sup := newSup(t, testSpec(), &fakeFS{vol: vol}, &fakeCP{}, &fakeReplicator{}, &fakeFencer{})
		got := sup.Run(context.Background(), make(chan os.Signal))
		if got.Exit != CodeRefused || got.ErrCode != ErrCodeVolumeTrashDisabled {
			t.Fatalf("got exit %d / %s, want %d / %s", got.Exit, got.ErrCode, CodeRefused, ErrCodeVolumeTrashDisabled)
		}
	})
	t.Run("control gate missing", func(t *testing.T) {
		sup := newSup(t, testSpec(), &fakeFS{vol: healthyVolume()}, &fakeCP{}, &fakeReplicator{}, &fakeFencer{})
		sup.Deps.ControlGateInstalled = func() bool { return false }
		got := sup.Run(context.Background(), make(chan os.Signal))
		if got.Exit != CodeRefused || got.ErrCode != ErrCodeControlWritable {
			t.Fatalf("got exit %d / %s, want %d / %s", got.Exit, got.ErrCode, CodeRefused, ErrCodeControlWritable)
		}
	})
	t.Run("cache dir holds another tenant", func(t *testing.T) {
		sup := newSup(t, testSpec(), &fakeFS{vol: healthyVolume()}, &fakeCP{}, &fakeReplicator{}, &fakeFencer{})
		other := filepath.Join(sup.Paths.CacheDir, "99999999-9999-9999-9999-999999999999", "rawstaging", "chunks")
		if err := os.MkdirAll(other, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(other, "1_0_4194304"), []byte("staged"), 0o600); err != nil {
			t.Fatal(err)
		}
		got := sup.Run(context.Background(), make(chan os.Signal))
		if got.Exit != CodeRefused || got.ErrCode != ErrCodeCacheDirTenantMismatch {
			t.Fatalf("got exit %d / %s, want %d / %s", got.Exit, got.ErrCode, CodeRefused, ErrCodeCacheDirTenantMismatch)
		}
	})
}

// An empty replica means "new volume" only where a new volume is possible.
// Anywhere else it means the replica was lost, and formatting there replaces a
// filesystem with an empty one.
func TestEmptyReplicaFormatsOnlyOnAFirstGeneration(t *testing.T) {
	t.Run("first generation formats", func(t *testing.T) {
		spec := bootstrapSpec()
		fs := &fakeFS{vol: healthyVolume()}
		rep := &fakeReplicator{restoreErr: ErrReplicaEmpty}
		sup := newSup(t, spec, fs, &fakeCP{}, rep, &fakeFencer{})
		stop := stopOnReady(sup)
		got := sup.Run(context.Background(), stop)
		if !fs.formatted {
			t.Fatalf("expected a format, got exit %d: %v", got.Exit, got.Err)
		}
	})
	t.Run("later generation refuses", func(t *testing.T) {
		spec := testSpec()
		spec.Generation = 2
		fs := &fakeFS{vol: healthyVolume()}
		sup := newSup(t, spec, fs, &fakeCP{}, &fakeReplicator{restoreErr: ErrReplicaEmpty}, &fakeFencer{})
		got := sup.Run(context.Background(), make(chan os.Signal))
		if fs.formatted {
			t.Fatal("a lost replica on generation 2 must never be formatted over")
		}
		if got.Exit != CodeRestoreFailed {
			t.Fatalf("exit = %d, want %d", got.Exit, CodeRestoreFailed)
		}
	})
	t.Run("recorded format uuid refuses", func(t *testing.T) {
		spec := testSpec()
		spec.Generation = 1
		spec.VolumeState = VolumeStateFormatted
		fs := &fakeFS{vol: healthyVolume()}
		sup := newSup(t, spec, fs, &fakeCP{}, &fakeReplicator{restoreErr: ErrReplicaEmpty}, &fakeFencer{})
		got := sup.Run(context.Background(), make(chan os.Signal))
		if fs.formatted {
			t.Fatal("a volume the control-plane already formatted must not be formatted again")
		}
		if got.Exit != CodeIdentityMismatch {
			t.Fatalf("exit = %d, want %d", got.Exit, CodeIdentityMismatch)
		}
	})
}

// The ordered stop of ADR / PLO-326, asserted as an order rather than as a set
// of calls: barrier, unmount, fence, close, final sync, stop, then release.
//
// The seal sits AFTER the unmount, not before the barrier, and that position is
// load-bearing. Since PLO-323 F-2 the fence covers the data path, so it also
// stops the slice commit the barrier's FlushAll depends on: sealing first makes
// `vfs.FlushAll` answer EROFS and turns every clean stop with a dirty buffer
// into exit 69, reported data loss. The write-stop margin exists to pay for
// exactly that drain (threat-model.md §7.5), so the drain runs first and the
// seal lands the moment the mount is detached — the first instant at which no
// further filesystem request can arrive. `pkg/vfs.TestPloriOrderedStopFlushes`
// pins the same thing against the real VFS.
func TestSigtermRunsTheOrderedShutdown(t *testing.T) {
	vol := healthyVolume()
	fs := &fakeFS{vol: vol}
	cp := &fakeCP{}
	rep := &fakeReplicator{}
	sup := newSup(t, testSpec(), fs, cp, rep, &fakeFencer{})

	stop := stopOnReady(sup)
	got := sup.Run(context.Background(), stop)
	if got.Exit != CodeOK {
		t.Fatalf("exit = %d, want 0 (%v)", got.Exit, got.Err)
	}
	want := []string{"purge_sessions", "barrier", "unmount", "fence", "close"}
	if diff := firstDiff(vol.order(), want); diff != "" {
		t.Errorf("volume call order: %s", diff)
	}
	if diff := firstDiff(rep.order(), []string{"restore", "start", "sync", "stop"}); diff != "" {
		t.Errorf("replicator call order: %s", diff)
	}
	// The lease is released last, and only after the durable point and the
	// usage have been reported.
	cpOrder := cp.order()
	if len(cpOrder) == 0 || cpOrder[len(cpOrder)-1] != "release" {
		t.Errorf("control-plane call order %v must end in release", cpOrder)
	}
	if cp.released != "shutdown" {
		t.Errorf("release reason = %q, want shutdown", cp.released)
	}
	// Readiness and health must both be on disk.
	for _, p := range []string{sup.Paths.ReadyPath(), sup.Paths.DurablePointPath()} {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("expected %s to exist: %v", filepath.Base(p), err)
		}
	}
}

// A shutdown whose barrier fails is reported data loss (exit 69) and STILL
// releases the lease: holding it costs the Agent a full TTL and the data is
// lost either way.
func TestFailedShutdownBarrierExits69ButReleasesTheLease(t *testing.T) {
	vol := healthyVolume()
	vol.barrier = func(context.Context) (BarrierResult, error) {
		return BarrierResult{}, errors.New("pending blocks did not upload")
	}
	cp := &fakeCP{}
	sup := newSup(t, testSpec(), &fakeFS{vol: vol}, cp, &fakeReplicator{}, &fakeFencer{})
	stop := stopOnReady(sup)
	got := sup.Run(context.Background(), stop)
	if got.Exit != CodeBarrierIncomplete || got.ErrCode != ErrCodeBarrierIncomplete {
		t.Fatalf("got exit %d / %s, want %d / %s", got.Exit, got.ErrCode, CodeBarrierIncomplete, ErrCodeBarrierIncomplete)
	}
	if cp.released == "" {
		t.Error("the lease must be released even when the barrier failed")
	}
}

// stale_epoch on renew is terminal by contract: it is never retried, because a
// retry is the fenced writer still believing it holds the volume. It is also
// the out-of-band case — the epoch was taken away, not allowed to run out — so
// the typed identifier is E_FENCED_OUT_OF_BAND and the stop uploads nothing.
func TestStaleEpochOnRenewFencesImmediately(t *testing.T) {
	vol := healthyVolume()
	cp := &fakeCP{renewErr: &CPError{Status: 409, Code: CPCodeStaleEpoch, Msg: "epoch 3 was moved past"}}
	sup := newSup(t, testSpec(), &fakeFS{vol: vol}, cp, &fakeReplicator{}, &fakeFencer{})
	got := sup.Run(context.Background(), make(chan os.Signal))
	if got.Exit != CodeFenced || got.ErrCode != ErrCodeFencedOutOfBand {
		t.Fatalf("got exit %d / %s, want %d / %s", got.Exit, got.ErrCode, CodeFenced, ErrCodeFencedOutOfBand)
	}
	if !vol.Fenced() {
		t.Error("writes must be fenced before the process exits")
	}
	// Sealed FIRST, before anything else in the stop: the epoch belongs to
	// somebody else from the instant the renew came back.
	if order := vol.order(); len(order) == 0 || order[len(order)-3:][0] != "fence" {
		t.Errorf("volume call order %v: the seal must come before the detach", order)
	}
	renews := 0
	for _, c := range cp.order() {
		if c == "renew" {
			renews++
		}
	}
	if renews != 1 {
		t.Errorf("renew was attempted %d times; stale_epoch must not be retried", renews)
	}
}

// With the control-plane unreachable there is no response to move the deadline
// forward, so the worker must stop itself at the margin rather than keep
// writing until someone tells it to stop.
func TestUnreachableControlPlaneFencesAtTheMargin(t *testing.T) {
	vol := healthyVolume()
	cp := &fakeCP{renewErr: errors.New("dial tcp: connection refused")}
	spec := testSpec()
	spec.LeaseExpiresAt = time.Now().UTC().Add(1200 * time.Millisecond)
	spec.WriteStopMargin = Duration(900 * time.Millisecond)
	sup := newSup(t, spec, &fakeFS{vol: vol}, cp, &fakeReplicator{}, &fakeFencer{})

	done := make(chan *Fatal, 1)
	go func() { done <- sup.Run(context.Background(), make(chan os.Signal)) }()
	select {
	case got := <-done:
		if got.Exit != CodeFenced {
			t.Fatalf("exit = %d, want %d (%v)", got.Exit, CodeFenced, got.Err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the worker kept running past its write-stop margin")
	}
}

// The durable point persisted for the next generation is T_before, captured
// BEFORE the barrier — not the barrier's completion time
// (crash-consistency.md §5).
func TestDurablePointIsThePreBarrierInstant(t *testing.T) {
	vol := healthyVolume()
	var barrierStarted time.Time
	vol.barrier = func(context.Context) (BarrierResult, error) {
		barrierStarted = time.Now().UTC()
		time.Sleep(30 * time.Millisecond)
		return BarrierResult{BarrierAt: time.Now().UTC()}, nil
	}
	sup := newSup(t, testSpec(), &fakeFS{vol: vol}, &fakeCP{}, &fakeReplicator{}, &fakeFencer{})
	stop := stopOnReady(sup)
	if got := sup.Run(context.Background(), stop); got.Exit != CodeOK {
		t.Fatalf("exit = %d: %v", got.Exit, got.Err)
	}
	dp, err := ReadDurablePoint(sup.Paths.DurablePointPath())
	if err != nil || dp == nil {
		t.Fatalf("read durable point: %v", err)
	}
	if !dp.DurableAt.Before(barrierStarted.Add(time.Millisecond)) {
		t.Errorf("durable_at %s is not the pre-barrier instant (barrier started %s)", dp.DurableAt, barrierStarted)
	}
	if !dp.BarrierAt.After(dp.DurableAt) {
		t.Errorf("barrier_at %s must be after durable_at %s", dp.BarrierAt, dp.DurableAt)
	}
}

func firstDiff(got, want []string) string {
	gi := 0
	for _, w := range want {
		found := false
		for ; gi < len(got); gi++ {
			if got[gi] == w {
				found = true
				gi++
				break
			}
		}
		if !found {
			return "missing " + w + " in " + join(got)
		}
	}
	return ""
}

func join(s []string) string {
	out := "["
	for i, v := range s {
		if i > 0 {
			out += " "
		}
		out += v
	}
	return out + "]"
}

// The metadata root is partitioned per writer epoch, so this epoch's prefix is
// empty by construction: restoring from it would look like a brand new volume
// and be answered with a format. The source has to be the previous
// generation's prefix. This is the failure the end-to-end run found.
func TestRestoreReadsThePreviousGenerationsPrefix(t *testing.T) {
	spec := testSpec()
	spec.FenceEpoch = 4
	spec.MetaPrefix = "agents-meta/550e8400-e29b-41d4-a716-446655440000/g4/"
	spec.FenceMarkerKey = spec.MetaPrefix + "fence"
	prior := "agents-meta/550e8400-e29b-41d4-a716-446655440000/g3/"

	rep := &fakeReplicator{}
	fs := &fakeFS{vol: healthyVolume()}
	sup := newSup(t, spec, fs, &fakeCP{}, rep, &fakeFencer{prior: prior})
	stop := stopOnReady(sup)
	if got := sup.Run(context.Background(), stop); got.Exit != CodeOK {
		t.Fatalf("exit = %d: %v", got.Exit, got.Err)
	}
	if rep.restoredFrom != prior {
		t.Errorf("restored from %q, want the previous generation %q", rep.restoredFrom, prior)
	}
	if fs.formatted {
		t.Error("a volume with a previous generation must never be formatted")
	}
	if got, want := spec.MetaRoot(), "agents-meta/550e8400-e29b-41d4-a716-446655440000/"; got != want {
		t.Errorf("MetaRoot() = %q, want %q", got, want)
	}
}

// The candidate order includes the worker's OWN epoch, newest first. A worker
// restarted at the same epoch after a crash finds its own prefix populated and
// must restore from it; restoring from `epoch - 1` there drops everything that
// epoch already wrote (PLO-323 F-6c). A fresh epoch's prefix holds nothing but
// its own fence marker, and prefixHasReplica skips it, so this ordering costs a
// fresh mount nothing.
func TestPriorPrefixCandidatesAreNewestFirstAtOrBelowTheEpoch(t *testing.T) {
	root := "agents-meta/v1/"
	got := priorPrefixCandidates([]string{
		root + "g1/", root + "g10/", root + "g2/", root + "g11/", root + "g12/", root + "gnope/", "elsewhere/g9/",
	}, root, 11)
	want := []string{root + "g11/", root + "g10/", root + "g2/", root + "g1/"}
	if len(got) != len(want) {
		t.Fatalf("candidates = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("candidates = %v, want %v", got, want)
		}
	}
	// A LATER epoch is still never a candidate: that prefix belongs to a writer
	// that superseded this one, and reading it would be this worker restoring
	// from its own successor.
	if got := priorPrefixCandidates([]string{root + "g12/"}, root, 11); len(got) != 0 {
		t.Errorf("candidates = %v, want none: a writer must never restore from a later epoch", got)
	}
	// Its own epoch is a candidate, and on a fresh mount it is harmlessly
	// empty: the caller only accepts a prefix that prefixHasReplica says holds
	// more than a fence marker.
	if got := priorPrefixCandidates([]string{root + "g1/"}, root, 1); len(got) != 1 {
		t.Errorf("candidates = %v, want the writer's own epoch after a crash-restart", got)
	}
}

// The restore-time repair is the crux fix (crash-consistency.md §7 d3): it has
// to run exactly when the previous generation died mid-flight, and never when
// it stopped cleanly, because a full scan on every warm start would pay 870 ms
// and 12 LIST operations for nothing.
func TestRepairRunsOnlyAfterAnUncleanGeneration(t *testing.T) {
	t.Run("unclean generation repairs before replication starts", func(t *testing.T) {
		vol := healthyVolume()
		vol.repair = RepairReport{Scanned: 12, Checked: 30, Missing: 1, Files: 1, Truncated: 1}
		sup := newSup(t, testSpec(), &fakeFS{vol: vol}, &fakeCP{}, &fakeReplicator{}, &fakeFencer{})
		// No clean marker: the previous writer did not finish its stop.
		if err := sup.start(context.Background()); err != nil {
			t.Fatalf("startup: %+v", err)
		}
		if vol.repaired != 1 {
			t.Fatalf("repair ran %d times, want 1", vol.repaired)
		}
		order := vol.order()
		repairAt, purgeAt := indexOf(order, "repair"), indexOf(order, "purge_sessions")
		if repairAt < 0 || purgeAt < 0 || repairAt < purgeAt {
			t.Fatalf("repair must follow the session purge, got %v", order)
		}
	})

	t.Run("clean generation does not repair", func(t *testing.T) {
		vol := healthyVolume()
		sup := newSup(t, testSpec(), &fakeFS{vol: vol}, &fakeCP{}, &fakeReplicator{}, &fakeFencer{})
		if err := os.MkdirAll(sup.Paths.StateDir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(sup.Paths.CleanStopPath(), []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := sup.start(context.Background()); err != nil {
			t.Fatalf("startup: %+v", err)
		}
		if vol.repaired != 0 {
			t.Fatalf("a clean stop must not trigger a repair, ran %d times", vol.repaired)
		}
	})

	t.Run("a freshly formatted volume does not repair", func(t *testing.T) {
		spec := bootstrapSpec()
		vol := healthyVolume()
		sup := newSup(t, spec, &fakeFS{vol: vol}, &fakeCP{},
			&fakeReplicator{restoreErr: ErrReplicaEmpty}, &fakeFencer{})
		if err := sup.start(context.Background()); err != nil {
			t.Fatalf("startup: %+v", err)
		}
		if vol.repaired != 0 {
			t.Fatalf("a volume with no history has nothing to repair, ran %d times", vol.repaired)
		}
	})

	t.Run("a repair failure refuses the mount", func(t *testing.T) {
		vol := healthyVolume()
		vol.repairErr = errors.New("object store said 503")
		sup := newSup(t, testSpec(), &fakeFS{vol: vol}, &fakeCP{}, &fakeReplicator{}, &fakeFencer{})
		err := sup.start(context.Background())
		if err == nil {
			t.Fatal("a failed repair must refuse the mount")
		}
		if f := Classify(err); f.ErrCode != ErrCodeRestoredToBarrier {
			t.Fatalf("got %v / %s, want %s", err, f.ErrCode, ErrCodeRestoredToBarrier)
		}
	})
}

func indexOf(a []string, want string) int {
	for i, s := range a {
		if s == want {
			return i
		}
	}
	return -1
}

// The reclaim decides on two proofs and refuses on anything else. Each row here
// is a way the 412 could be someone else's claim, or could be unprovable — and
// every one of them must stay fenced out, because the marker is the only
// store-side fence this design has.
func TestTheMarkerReclaimFailsClosedOnEveryUnprovenCase(t *testing.T) {
	spec := testSpec()
	ours := FenceMarker{Volume: spec.StorageVolumeID, Epoch: spec.FenceEpoch, ClaimedAt: "2026-09-02T00:00:00Z"}

	tests := map[string]struct {
		fencer *fakeFencer
		cp     *fakeCP
	}{
		"the marker names another volume": {
			fencer: &fakeFencer{err: ErrFenceMarkerHeld, marker: FenceMarker{
				Volume: "99999999-9999-9999-9999-999999999999", Epoch: spec.FenceEpoch,
			}},
			cp: &fakeCP{},
		},
		"the marker names another epoch": {
			fencer: &fakeFencer{err: ErrFenceMarkerHeld, marker: FenceMarker{
				Volume: spec.StorageVolumeID, Epoch: spec.FenceEpoch + 1,
			}},
			cp: &fakeCP{},
		},
		"the marker vanished between the PUT and the GET": {
			fencer: &fakeFencer{err: ErrFenceMarkerHeld, getErr: ErrFenceMarkerMissing},
			cp:     &fakeCP{},
		},
		"the control-plane says this pod does not hold the epoch": {
			fencer: &fakeFencer{err: ErrFenceMarkerHeld, marker: ours},
			cp: &fakeCP{renewErr: &CPError{
				Status: 403, Code: CPCodeIdentityMismatch, Msg: "volume is not held by this pod",
			}},
		},
		"the control-plane moved past this epoch": {
			fencer: &fakeFencer{err: ErrFenceMarkerHeld, marker: ours},
			cp:     &fakeCP{renewErr: &CPError{Status: 409, Code: CPCodeStaleEpoch, Msg: "epoch moved past"}},
		},
		"the control-plane cannot be reached to prove it": {
			fencer: &fakeFencer{err: ErrFenceMarkerHeld, marker: ours},
			cp:     &fakeCP{renewErr: context.DeadlineExceeded},
		},
		// A 200 is only a proof if it is a proof about THIS volume. The reclaim
		// asks the control-plane "am I the live holder of this epoch", and an
		// answer that names another volume answers a question nobody asked
		// (PLO-520).
		"the control-plane answered about another volume": {
			fencer: &fakeFencer{err: ErrFenceMarkerHeld, marker: ours},
			cp: &fakeCP{renewEcho: func(_ string, epoch int64) (string, int64) {
				return "99999999-9999-9999-9999-999999999999", epoch
			}},
		},
		"the control-plane answered about another epoch": {
			fencer: &fakeFencer{err: ErrFenceMarkerHeld, marker: ours},
			cp: &fakeCP{renewEcho: func(volumeID string, epoch int64) (string, int64) {
				return volumeID, epoch + 1
			}},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			rep := &fakeReplicator{}
			sup := newSup(t, testSpec(), &fakeFS{vol: healthyVolume()}, tc.cp, rep, tc.fencer)
			got := sup.Run(context.Background(), make(chan os.Signal))
			if got.Exit != CodeFenced || got.ErrCode != ErrCodeFenceMarkerHeld {
				t.Fatalf("exit %d/%s, want %d/%s", got.Exit, got.ErrCode, CodeFenced, ErrCodeFenceMarkerHeld)
			}
			if n := len(rep.order()); n != 0 {
				t.Errorf("a worker that could not prove the claim touched the replica: %v", rep.order())
			}
		})
	}
}

// The other direction: both proofs hold, so the epoch is already ours and the
// mount proceeds. Without this a worker crash costs the Agent a full lease TTL
// of downtime on the failure this supervisor exists to survive (PLO-323 F-6).
func TestTheMarkerReclaimProceedsWhenBothProofsHold(t *testing.T) {
	spec := testSpec()
	fencer := &fakeFencer{err: ErrFenceMarkerHeld, marker: FenceMarker{
		Volume: spec.StorageVolumeID, Epoch: spec.FenceEpoch, ClaimedAt: "2026-09-02T00:00:00Z",
	}}
	cp := &fakeCP{}
	sup := newSup(t, spec, &fakeFS{vol: healthyVolume()}, cp, &fakeReplicator{}, fencer)
	stop := stopOnReady(sup)
	if got := sup.Run(context.Background(), stop); got.Exit != CodeOK {
		t.Fatalf("exit = %d/%s (%v), want a clean stop", got.Exit, got.ErrCode, got.Err)
	}
	// The proof is a renew, and it is the same route the loop uses — so a
	// reclaim leaves no second control-plane call to review.
	if order := cp.order(); len(order) == 0 || order[0] != "renew" {
		t.Errorf("control-plane call order %v must open with the renew that proves the holder", order)
	}
}
