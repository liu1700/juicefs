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
	"syscall"
	"testing"
	"time"
)

// ------------------------------------------------------------------ fakes ---

type fakeVolume struct {
	mu        sync.Mutex
	id        FormatIdentity
	storedID  string
	storedErr error
	integrity error
	purgeErr  error
	purged    int
	barrier   func(context.Context) (BarrierResult, error)
	usage     Usage
	fenced    bool
	calls     []string
	serve     chan error
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
func (f *fakeVolume) PendingBlocks() uint64                { return 0 }
func (f *fakeVolume) Usage(context.Context) (Usage, error) { return f.usage, nil }
func (f *fakeVolume) ApplyGrant(context.Context, int64, int64) error {
	f.record("apply_grant")
	return nil
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
func (f *fakeVolume) Close() error                  { f.record("close"); return nil }

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
	mu       sync.Mutex
	calls    []string
	renewErr error
	expiry   func() time.Time
	grant    GrantSpec
	released string
}

func (c *fakeCP) record(name string) { c.mu.Lock(); c.calls = append(c.calls, name); c.mu.Unlock() }
func (c *fakeCP) order() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.calls...)
}
func (c *fakeCP) RenewLease(context.Context, string, int64) (LeaseResponse, error) {
	c.record("renew")
	if c.renewErr != nil {
		return LeaseResponse{}, c.renewErr
	}
	exp := time.Now().UTC().Add(2 * time.Minute)
	if c.expiry != nil {
		exp = c.expiry()
	}
	return LeaseResponse{LeaseExpiresAt: exp, Grant: c.grant}, nil
}
func (c *fakeCP) ReleaseLease(_ context.Context, _ string, _ int64, reason string) error {
	c.mu.Lock()
	c.calls = append(c.calls, "release")
	c.released = reason
	c.mu.Unlock()
	return nil
}
func (c *fakeCP) ReportUsage(context.Context, string, int64, Usage, time.Time) error {
	c.record("usage")
	return nil
}
func (c *fakeCP) ReportDurablePoint(context.Context, string, int64, BarrierResult, string) error {
	c.record("durable_point")
	return nil
}
func (c *fakeCP) AckGrant(context.Context, string, int64, int64) error {
	c.record("grant_ack")
	return nil
}

type fakeReplicator struct {
	mu         sync.Mutex
	calls      []string
	restoreErr error
	syncErr    error
}

func (r *fakeReplicator) record(n string) { r.mu.Lock(); r.calls = append(r.calls, n); r.mu.Unlock() }
func (r *fakeReplicator) order() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}
func (r *fakeReplicator) Restore(context.Context, time.Time) error {
	r.record("restore")
	return r.restoreErr
}
func (r *fakeReplicator) Start(context.Context) error       { r.record("start"); return nil }
func (r *fakeReplicator) SyncAndWait(context.Context) error { r.record("sync"); return r.syncErr }
func (r *fakeReplicator) TxID(context.Context) (string, error) {
	return "0000000000000009", nil
}
func (r *fakeReplicator) Stop(context.Context) error { r.record("stop"); return nil }

type fakeFencer struct{ err error }

func (f *fakeFencer) Claim(context.Context, string, []byte) error { return f.err }

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
	spec.BarrierInterval = Duration(30 * time.Millisecond)
	spec.UsageReportEvery = 2
	return spec
}

func newSup(t *testing.T, spec *MountSpec, fs *fakeFS, cp *fakeCP, rep *fakeReplicator, fencer Fencer) *Supervisor {
	t.Helper()
	dir := t.TempDir()
	return &Supervisor{
		Spec:  spec,
		Paths: Paths{StateDir: filepath.Join(dir, "state"), CacheDir: filepath.Join(dir, "cache"), MountPoint: filepath.Join(dir, "mnt")},
		Deps: Deps{
			FS: fs, CP: cp, Replicator: rep, Fencer: fencer,
			ControlGateInstalled: func() bool { return true },
		},
	}
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
	if cp.released != "fenced" {
		t.Errorf("release reason = %q, want fenced", cp.released)
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
		spec := testSpec()
		spec.Generation = 1
		spec.VolumeState = VolumeStateFormatted
		spec.FormatUUID = ""
		fs := &fakeFS{vol: healthyVolume()}
		rep := &fakeReplicator{restoreErr: ErrReplicaEmpty}
		sup := newSup(t, spec, fs, &fakeCP{}, rep, &fakeFencer{})
		stop := make(chan os.Signal, 1)
		stop <- syscall.SIGTERM
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
// of calls: fence, barrier, unmount, close, final sync, stop, then release.
func TestSigtermRunsTheOrderedShutdown(t *testing.T) {
	vol := healthyVolume()
	fs := &fakeFS{vol: vol}
	cp := &fakeCP{}
	rep := &fakeReplicator{}
	sup := newSup(t, testSpec(), fs, cp, rep, &fakeFencer{})

	stop := make(chan os.Signal, 1)
	stop <- syscall.SIGTERM
	got := sup.Run(context.Background(), stop)
	if got.Exit != CodeOK {
		t.Fatalf("exit = %d, want 0 (%v)", got.Exit, got.Err)
	}
	want := []string{"purge_sessions", "fence", "barrier", "unmount", "close"}
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
	stop := make(chan os.Signal, 1)
	stop <- syscall.SIGTERM
	got := sup.Run(context.Background(), stop)
	if got.Exit != CodeBarrierIncomplete || got.ErrCode != ErrCodeBarrierIncomplete {
		t.Fatalf("got exit %d / %s, want %d / %s", got.Exit, got.ErrCode, CodeBarrierIncomplete, ErrCodeBarrierIncomplete)
	}
	if cp.released == "" {
		t.Error("the lease must be released even when the barrier failed")
	}
}

// stale_epoch on renew is terminal by contract: it is never retried, because a
// retry is the fenced writer still believing it holds the volume.
func TestStaleEpochOnRenewFencesImmediately(t *testing.T) {
	vol := healthyVolume()
	cp := &fakeCP{renewErr: &CPError{Status: 409, Code: CPCodeStaleEpoch, Msg: "epoch 3 was moved past"}}
	sup := newSup(t, testSpec(), &fakeFS{vol: vol}, cp, &fakeReplicator{}, &fakeFencer{})
	got := sup.Run(context.Background(), make(chan os.Signal))
	if got.Exit != CodeFenced || got.ErrCode != ErrCodeLeaseLost {
		t.Fatalf("got exit %d / %s, want %d / %s", got.Exit, got.ErrCode, CodeFenced, ErrCodeLeaseLost)
	}
	if !vol.Fenced() {
		t.Error("writes must be fenced before the process exits")
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
	stop := make(chan os.Signal, 1)
	stop <- syscall.SIGTERM
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
