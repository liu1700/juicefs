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
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// armedExpiry is the write-gate instant the supervisor last published.
func (f *fakeVolume) armedExpiry() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.writeExpiry
}

// ------------------------------------------------ a replicator that stalls ---

// stallingNodeReplicator is a node-level Litestream control socket that
// registers databases and then never answers a /sync or an /unregister: the
// shape of the PLO-690 stall, where the daemon stopped serving and every
// request queued behind it. It is a real net/http server on a unix socket, so
// the worker's own client, deadlines and connection handling are what run.
type stallingNodeReplicator struct {
	socket string

	mu        sync.Mutex
	active    int
	registers int
	probes    int
	onWait    func()
}

func newStallingNodeReplicator(t *testing.T) *stallingNodeReplicator {
	t.Helper()
	dir, err := os.MkdirTemp("", "stall")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	f := &stallingNodeReplicator{socket: filepath.Join(dir, "s.sock")}
	ln, err := net.Listen("unix", f.socket)
	if err != nil {
		t.Fatalf("listen on %s: %v", f.socket, err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(f.serve)}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return f
}

func (f *stallingNodeReplicator) serve(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)
	// Reading to EOF is what lets the server notice the client hanging up,
	// which is how this handler learns its call was abandoned.
	_, _ = io.Copy(io.Discard, r.Body)
	f.mu.Lock()
	f.active++
	onWait := f.onWait
	switch {
	case r.URL.Path == "/register":
		f.registers++
	case r.URL.Path == "/sync" && body["wait"] != true:
		f.probes++
	}
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.active--
		f.mu.Unlock()
	}()
	if r.URL.Path == "/register" {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "registered", "path": body["path"]})
		return
	}
	if r.URL.Path == "/sync" && body["wait"] == true && onWait != nil {
		onWait()
	}
	<-r.Context().Done()
}

func (f *stallingNodeReplicator) counts() (active, registers, probes int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.active, f.registers, f.probes
}

// restoredNodeReplicator is a NodeReplicator whose restore has already
// happened, so a supervisor can start against a real control socket without a
// Litestream binary. Every other call is the production client.
type restoredNodeReplicator struct{ n *NodeReplicator }

func (r restoredNodeReplicator) Restore(context.Context, string, RestoreOptions) error {
	return nil
}

func (r restoredNodeReplicator) Start(ctx context.Context) error {
	return r.n.Start(ctx)
}

func (r restoredNodeReplicator) SyncAndWait(ctx context.Context) error {
	return r.n.SyncAndWait(ctx)
}

func (r restoredNodeReplicator) TxID(ctx context.Context) (string, error) {
	return r.n.TxID(ctx)
}

func (r restoredNodeReplicator) Stop(ctx context.Context) error {
	return r.n.Stop(ctx)
}

func (r restoredNodeReplicator) Abort(ctx context.Context) error {
	return r.n.Abort(ctx)
}

func (r restoredNodeReplicator) Probe(ctx context.Context) error {
	return r.n.Probe(ctx)
}

func (r restoredNodeReplicator) Restart(ctx context.Context) error {
	return r.n.Restart(ctx)
}

// TestAStalledReplicatorStopsTheMountWithoutStarvingTheLease is Phase A's
// requirement end to end: a replicator that stops answering stops the mount as
// a replication failure, with nobody killing a child by hand, while the lease
// keeps renewing underneath.
//
// Before the workers the periodic barrier's `/sync -wait` held the run loop for
// the rest of the lease. Nothing renewed, the probe that would have seen the
// stall could not run, and the mount died as a lease loss (E_LEASE_LOST) once
// the call ran into the expiry — the wrong cause, reached by the wrong route.
func TestAStalledReplicatorStopsTheMountWithoutStarvingTheLease(t *testing.T) {
	daemon := newStallingNodeReplicator(t)
	spec := testSpec()
	spec.LeaseRenewInterval = Duration(50 * time.Millisecond)
	spec.WriteStopMargin = Duration(300 * time.Millisecond)
	spec.LeaseExpiresAt = time.Now().UTC().Add(2 * time.Second)
	cp := &fakeCP{expiry: func() time.Time { return time.Now().UTC().Add(2 * time.Second) }}
	vol := healthyVolume()
	sup := newSup(t, spec, &fakeFS{vol: vol}, cp, &fakeReplicator{}, &fakeFencer{})
	node := &NodeReplicator{SocketPath: daemon.socket, DBPath: filepath.Join(t.TempDir(), "meta.db")}
	if err := node.Configure(spec, ParseMountOptions(nil)); err != nil {
		t.Fatalf("configure: %v", err)
	}
	sup.Deps.Replicator = restoredNodeReplicator{node}
	sup.Options.BarrierInterval = 400 * time.Millisecond

	var renewsAtStall atomic.Int64
	renewsAtStall.Store(-1)
	daemon.mu.Lock()
	daemon.onWait = func() { renewsAtStall.CompareAndSwap(-1, int64(len(cp.renewRequests()))) }
	daemon.mu.Unlock()

	done := make(chan *Fatal, 1)
	go func() { done <- sup.Run(context.Background(), make(chan os.Signal)) }()
	f := waitFatal(t, done, 15*time.Second, "a stalled replicator never stopped the mount")

	if f.Exit != CodeBarrierIncomplete || f.ErrCode != ErrCodeReplicationFailed {
		t.Fatalf("exit = %d/%s (%v), want %d/%s: the stall must stop the mount as a replication failure, not starve the lease into a lease loss",
			f.Exit, f.ErrCode, f.Err, CodeBarrierIncomplete, ErrCodeReplicationFailed)
	}
	at := renewsAtStall.Load()
	if at < 0 {
		t.Fatal("no waiting sync ever reached the stalled replicator")
	}
	if got := int64(len(cp.renewRequests())); got < at+3 {
		t.Errorf("%d renewals after the barrier's sync stalled, want at least 3: the lease must not wait behind replication", got-at)
	}
	h := readHealth(t, sup)
	if !h.ReplicationFailed {
		t.Error("health.json does not record the replication failure the mount stopped for")
	}
	if h.ObservedAt.IsZero() || h.ReplicationCheckedAt.IsZero() {
		t.Errorf("observed_at = %s, replication_checked_at = %s, want both set", h.ObservedAt, h.ReplicationCheckedAt)
	}
	if n := countCalls(cp.order(), "durable_point"); n != 0 {
		t.Errorf("%d durable points reported from syncs that never answered, want 0", n)
	}
	if exists(t, sup.Paths.CleanStopPath()) {
		t.Error("a stop whose final sync never answered wrote the clean marker")
	}
	if _, registers, probes := daemon.counts(); probes == 0 || registers < 2 {
		t.Errorf("probes = %d, registrations = %d, want a probe and a re-registration after the first", probes, registers)
	}
	waitFor(t, 2*time.Second, func() bool {
		active, _, _ := daemon.counts()
		return active == 0
	}, "a control call to the stalled replicator outlived the stop")
}

// ---------------------------------------------------- the deadline and renew ---

// The periodic barrier can block for as long as the lease lets it. The stop at
// the write-stop margin must not wait for it: it begins on time, and the
// blocked barrier is cancelled by the stop, before the lease expires.
func TestABlockedBarrierDoesNotDeferTheDeadlineStop(t *testing.T) {
	type ending struct {
		err error
		at  time.Time
	}
	vol := healthyVolume()
	ended := make(chan ending, 1)
	var calls atomic.Int32
	vol.barrier = func(ctx context.Context) (BarrierResult, error) {
		if calls.Add(1) > 1 {
			return BarrierResult{BarrierAt: time.Now().UTC()}, nil
		}
		<-ctx.Done()
		ended <- ending{err: ctx.Err(), at: time.Now()}
		return BarrierResult{}, ctx.Err()
	}
	spec := testSpec()
	spec.LeaseRenewInterval = Duration(50 * time.Millisecond)
	spec.WriteStopMargin = Duration(900 * time.Millisecond)
	spec.LeaseExpiresAt = time.Now().UTC().Add(1500 * time.Millisecond)
	expiry := spec.LeaseExpiresAt
	cp := &fakeCP{renewErr: errors.New("dial tcp: connection refused")}
	sup := newSup(t, spec, &fakeFS{vol: vol}, cp, &fakeReplicator{}, &fakeFencer{})

	done := make(chan *Fatal, 1)
	go func() { done <- sup.Run(context.Background(), make(chan os.Signal)) }()
	f := waitFatal(t, done, 10*time.Second, "the worker kept running past its write-stop margin")
	if f.Exit != CodeFenced || f.ErrCode != ErrCodeLeaseLost {
		t.Fatalf("exit = %d/%s (%v), want %d/%s", f.Exit, f.ErrCode, f.Err, CodeFenced, ErrCodeLeaseLost)
	}
	select {
	case e := <-ended:
		if !errors.Is(e.err, context.Canceled) {
			t.Errorf("the blocked barrier ended with %v, want the stop's cancellation rather than its own lease budget", e.err)
		}
		if !e.at.Before(expiry) {
			t.Errorf("the blocked barrier was released at %s, not before the lease expiry %s", e.at.UTC(), expiry)
		}
	default:
		t.Fatal("the periodic barrier never ran")
	}
	if !vol.Fenced() {
		t.Error("the deadline stop did not seal the volume")
	}
}

// stalledRenewCP holds its first renewal until the caller gives up, and then
// answers it with a success naming a lease an hour long: a slow control-plane
// whose answer arrives after all.
type stalledRenewCP struct {
	*fakeCP
	entered chan struct{}
	ended   chan error
	once    sync.Once
}

func (c *stalledRenewCP) RenewLease(ctx context.Context, volumeID string, epoch int64, _ RenewRequest) (LeaseResponse, error) {
	c.record("renew")
	c.once.Do(func() { close(c.entered) })
	<-ctx.Done()
	select {
	case c.ended <- ctx.Err():
	default:
	}
	return LeaseResponse{StorageVolumeID: volumeID, FenceEpoch: epoch, LeaseExpiresAt: time.Now().UTC().Add(time.Hour)}, nil
}

// A SIGTERM while a renewal hangs used to wait for the renewal: the stop began
// only at the ordered-stop instant, and then as a deadline fence. It now begins
// at once, the hung renewal is abandoned, and its late success moves neither
// the deadline nor the write gate.
func TestASigtermDuringAStalledRenewStopsAtOnceAndTheLateAnswerMovesNothing(t *testing.T) {
	vol := healthyVolume()
	base := &fakeCP{}
	spec := testSpec()
	spec.LeaseRenewInterval = Duration(50 * time.Millisecond)
	spec.WriteStopMargin = Duration(300 * time.Millisecond)
	spec.LeaseExpiresAt = time.Now().UTC().Add(3 * time.Second)
	sup := newSup(t, spec, &fakeFS{vol: vol}, base, &fakeReplicator{}, &fakeFencer{})
	cp := &stalledRenewCP{fakeCP: base, entered: make(chan struct{}), ended: make(chan error, 1)}
	sup.Deps.CP = cp

	stop := make(chan os.Signal, 1)
	done := make(chan *Fatal, 1)
	go func() { done <- sup.Run(context.Background(), stop) }()
	select {
	case <-cp.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("no renewal was sent")
	}
	armed := vol.armedExpiry()
	sent := time.Now()
	stop <- syscall.SIGTERM
	f := waitFatal(t, done, 5*time.Second, "the stop waited behind the stalled renewal")
	if f.Exit != CodeOK {
		t.Fatalf("exit = %d/%s (%v), want a clean stop", f.Exit, f.ErrCode, f.Err)
	}
	if waited := time.Since(sent); waited > time.Second {
		t.Errorf("the stop took %s behind a hung renewal", waited)
	}
	select {
	case err := <-cp.ended:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("the hung renewal ended with %v, want the stop's cancellation", err)
		}
	default:
		t.Error("the hung renewal was still running when Run returned")
	}
	if got := vol.armedExpiry(); !got.Equal(armed) {
		t.Errorf("the write gate moved from %s to %s after the stop began", armed, got)
	}
	if wall := sup.deadline.WallExpiry(); !wall.Equal(spec.LeaseExpiresAt) {
		t.Errorf("deadline = %s, want the spec's %s: a renewal answered after the stop is not authority", wall, spec.LeaseExpiresAt)
	}
}

type quotaDuringRenewCP struct {
	*fakeCP
	requests chan RenewRequest
	release  chan struct{}
	calls    atomic.Int32
}

func (c *quotaDuringRenewCP) RenewLease(ctx context.Context, volume string, epoch int64, req RenewRequest) (LeaseResponse, error) {
	c.requests <- req
	if c.calls.Add(1) == 1 {
		select {
		case <-c.release:
		case <-ctx.Done():
			return LeaseResponse{}, ctx.Err()
		}
	}
	return c.fakeCP.RenewLease(ctx, volume, epoch, req)
}

func TestQuotaAdmissionDuringRenewKeepsOneRequestInFlight(t *testing.T) {
	spec := testSpec()
	spec.LeaseRenewInterval = Duration(500 * time.Millisecond)
	spec.Grant = GrantSpec{Bytes: 64 << 20, Inodes: 16384, Epoch: 2, AckedEpoch: 2}
	cp := &quotaDuringRenewCP{
		fakeCP: &fakeCP{grant: spec.Grant, onGrow: func(g GrantSpec) GrantSpec {
			g.Bytes += 64 << 20
			g.Epoch++
			return g
		}},
		requests: make(chan RenewRequest, 32), release: make(chan struct{}),
	}
	sup := newSup(t, spec, &fakeFS{vol: healthyVolume()}, cp.fakeCP, &fakeReplicator{}, &fakeFencer{})
	sup.Deps.CP = cp
	stop := make(chan os.Signal, 1)
	done := make(chan *Fatal, 1)
	go func() { done <- sup.Run(context.Background(), stop) }()
	t.Cleanup(func() { stop <- syscall.SIGTERM; waitFatal(t, done, 5*time.Second, "quota test did not stop") })
	select {
	case req := <-cp.requests:
		if req.Grow {
			t.Fatal("ordinary first renewal unexpectedly requested growth")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ordinary renewal never started")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	admitted := make(chan syscall.Errno, 1)
	go func() { admitted <- sup.Admit(ctx) }()
	waitFor(t, time.Second, sup.admissionPending, "quota admission did not start")
	select {
	case <-cp.requests:
		t.Fatal("quota signal started a second renewal before the first returned")
	case <-time.After(250 * time.Millisecond):
	}
	close(cp.release)
	select {
	case req := <-cp.requests:
		if !req.Grow {
			t.Fatal("pending quota admission was not included in the next renewal")
		}
	case <-time.After(400 * time.Millisecond):
		t.Fatal("pending quota admission waited for the full ordinary renewal interval")
	}
	select {
	case result := <-admitted:
		if result != 0 {
			t.Fatalf("quota admission failed: %v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("applied growth did not release the quota waiter")
	}
}

// Once a stop has begun, no renewal answer may act: not a success that would
// extend the lease, and not a refusal that would run a second stop.
func TestARenewAnswerAfterTheStopBeganMovesNothing(t *testing.T) {
	vol := healthyVolume()
	cp := &fakeCP{}
	sup := newSup(t, testSpec(), &fakeFS{vol: vol}, cp, &fakeReplicator{}, &fakeFencer{})
	sup.vol = vol
	sup.drain = NewDrainModel(DefaultDrainPerBlock)
	now := time.Now()
	sup.deadline = NewDeadline(now.UTC().Add(time.Minute), time.Second, now)
	if err := os.MkdirAll(sup.Paths.StateDir, 0o700); err != nil {
		t.Fatalf("state dir: %v", err)
	}
	sup.publishWriteExpiry()
	armed, wall := vol.armedExpiry(), sup.deadline.WallExpiry()
	sup.mu.Lock()
	sup.fenced = true
	sup.mu.Unlock()

	answers := map[string]renewObservation{
		"success": {
			reportUsage: true,
			resp: LeaseResponse{
				StorageVolumeID: sup.Spec.StorageVolumeID,
				FenceEpoch:      sup.Spec.FenceEpoch,
				LeaseExpiresAt:  now.UTC().Add(time.Hour),
			},
		},
		"stale epoch": {err: &CPError{Status: http.StatusConflict, Code: CPCodeStaleEpoch}},
	}
	for name, obs := range answers {
		obs.before, obs.due = time.Now(), time.Now().Add(time.Hour)
		res := sup.applyRenew(context.Background(), obs, func() {
			t.Errorf("a %s answer after the stop started a usage observation", name)
		})
		if res.fatal != nil || res.retry || !res.renewedAt.IsZero() {
			t.Errorf("a %s answer after the stop produced %+v, want nothing", name, res)
		}
	}
	if got := vol.armedExpiry(); !got.Equal(armed) {
		t.Errorf("write gate = %s, want the %s armed before the stop", got, armed)
	}
	if got := sup.deadline.WallExpiry(); !got.Equal(wall) {
		t.Errorf("deadline = %s, want %s", got, wall)
	}
	if cp.released != "" || vol.Fenced() {
		t.Errorf("a late answer ran a stop of its own: release %q, sealed %t", cp.released, vol.Fenced())
	}
	if _, err := os.Stat(sup.Paths.HealthPath()); !os.IsNotExist(err) {
		t.Errorf("a late answer rewrote health.json (stat: %v)", err)
	}
}

// ------------------------------------------------------------- health.json ---

// health.json keeps being rewritten while the periodic barrier is blocked, and
// what it says about the barrier stays true: last_barrier_at does not move for
// a barrier that has not returned.
func TestHealthStaysFreshWhileABarrierIsBlocked(t *testing.T) {
	vol := healthyVolume()
	entered := make(chan time.Time, 1)
	var calls atomic.Int32
	vol.barrier = func(ctx context.Context) (BarrierResult, error) {
		if calls.Add(1) > 1 {
			return BarrierResult{BarrierAt: time.Now().UTC()}, nil
		}
		entered <- time.Now().UTC()
		<-ctx.Done()
		return BarrierResult{}, ctx.Err()
	}
	cp := &fakeCP{}
	sup := newSup(t, testSpec(), &fakeFS{vol: vol}, cp, &fakeReplicator{}, &fakeFencer{})
	stop := make(chan os.Signal, 1)
	done := make(chan *Fatal, 1)
	go func() { done <- sup.Run(context.Background(), stop) }()

	var blockedAt time.Time
	select {
	case blockedAt = <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the periodic barrier never ran")
	}
	waitFor(t, 5*time.Second, func() bool {
		h, ok := healthWhenWritten(sup)
		return ok && h.ObservedAt.After(blockedAt.Add(200*time.Millisecond))
	}, "health.json stopped being rewritten while a barrier was blocked")
	h := readHealth(t, sup)
	if !h.LastRenewOK {
		t.Error("renewals stopped succeeding while a barrier was blocked")
	}
	if h.LastBarrierAt.After(blockedAt) {
		t.Errorf("last_barrier_at = %s advanced past a barrier that has not returned (blocked at %s)", h.LastBarrierAt, blockedAt)
	}
	stop <- syscall.SIGTERM
	if f := waitFatal(t, done, 5*time.Second, "the supervisor did not stop"); f.Exit != CodeOK {
		t.Fatalf("exit = %d/%s (%v), want a clean stop", f.Exit, f.ErrCode, f.Err)
	}
}

// ------------------------------------------------------- stops and overlaps ---

// fenceWhenCP answers stale_epoch from the moment gate closes.
type fenceWhenCP struct {
	*fakeCP
	gate <-chan struct{}
}

func (c *fenceWhenCP) RenewLease(ctx context.Context, volumeID string, epoch int64, req RenewRequest) (LeaseResponse, error) {
	select {
	case <-c.gate:
		c.record("renew")
		return LeaseResponse{}, &CPError{Status: http.StatusConflict, Code: CPCodeStaleEpoch, Msg: "epoch moved past"}
	default:
		return c.fakeCP.RenewLease(ctx, volumeID, epoch, req)
	}
}

// An out-of-band fence can now arrive while a periodic barrier is running. The
// barrier is cancelled before the seal, no barrier starts after it, and a
// barrier that completes as the stop lands reports nothing: the epoch is
// somebody else's.
func TestAnOutOfBandFenceDuringABarrierStartsNoBarrierAndReportsNothing(t *testing.T) {
	vol := healthyVolume()
	blocked := make(chan struct{})
	var calls atomic.Int32
	vol.barrier = func(ctx context.Context) (BarrierResult, error) {
		if calls.Add(1) == 1 {
			close(blocked)
			<-ctx.Done()
		}
		return BarrierResult{BarrierAt: time.Now().UTC()}, nil
	}
	base := &fakeCP{}
	sup := newSup(t, testSpec(), &fakeFS{vol: vol}, base, &fakeReplicator{}, &fakeFencer{})
	sup.Deps.CP = &fenceWhenCP{fakeCP: base, gate: blocked}

	done := make(chan *Fatal, 1)
	go func() { done <- sup.Run(context.Background(), make(chan os.Signal)) }()
	f := waitFatal(t, done, 10*time.Second, "a stale epoch during a blocked barrier never stopped the mount")
	if f.Exit != CodeFenced || f.ErrCode != ErrCodeFencedOutOfBand {
		t.Fatalf("exit = %d/%s (%v), want %d/%s", f.Exit, f.ErrCode, f.Err, CodeFenced, ErrCodeFencedOutOfBand)
	}
	sealed := false
	for _, c := range vol.order() {
		if c == "fence" {
			sealed = true
		}
		if sealed && c == "barrier" {
			t.Fatalf("a barrier ran after the seal: %v", vol.order())
		}
	}
	if !sealed {
		t.Fatalf("the out-of-band fence did not seal the volume: %v", vol.order())
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("barrier calls = %d, want only the one the fence interrupted", n)
	}
	if n := countCalls(base.order(), "durable_point"); n != 0 {
		t.Errorf("%d durable points reported by a writer that lost its epoch, want 0", n)
	}
}

// sealGateVolume holds FenceWrites open after it has sealed the fake metadata
// engine. This makes the ordering at an out-of-band fence observable: worker
// cancellation must be issued before the seal, but the seal itself must not
// wait for an uncooperative worker.
type sealGateVolume struct {
	*fakeVolume
	sealed      chan struct{}
	allowReturn chan struct{}
	once        sync.Once
}

func (v *sealGateVolume) FenceWrites() {
	v.fakeVolume.FenceWrites()
	v.once.Do(func() { close(v.sealed) })
	<-v.allowReturn
}

type sealAwareReplicator struct {
	fakeReplicator
	txidStarted chan struct{}
	sealed      <-chan struct{}
	cancelled   chan bool
}

func (r *sealAwareReplicator) TxID(ctx context.Context) (string, error) {
	close(r.txidStarted)
	<-r.sealed
	r.cancelled <- ctx.Err() != nil
	return "", ctx.Err()
}

func TestAnOutOfBandFenceCancelsABarrierBeforeItSealsWrites(t *testing.T) {
	base := &fakeCP{}
	vol := &sealGateVolume{
		fakeVolume:  healthyVolume(),
		sealed:      make(chan struct{}),
		allowReturn: make(chan struct{}),
	}
	rep := &sealAwareReplicator{
		txidStarted: make(chan struct{}),
		sealed:      vol.sealed,
		cancelled:   make(chan bool, 1),
	}
	spec := testSpec()
	spec.LeaseExpiresAt = time.Now().UTC().Add(time.Second)
	sup := newSup(t, spec, &fakeFS{vol: vol.fakeVolume}, base, &fakeReplicator{}, &fakeFencer{})
	sup.vol = vol
	sup.Deps.Replicator = rep
	sup.deadline = NewDeadline(spec.LeaseExpiresAt, spec.WriteStopMargin.D(), time.Now())
	w := sup.startWorkers(context.Background())
	w.barrierJobs <- struct{}{}
	select {
	case <-rep.txidStarted:
	case <-time.After(10 * time.Second):
		t.Fatal("the periodic barrier did not begin its replica-position read")
	}

	done := make(chan *Fatal, 1)
	go func() {
		done <- sup.fenceAndStop(fatalf(CodeFenced, ErrCodeFencedOutOfBand, false, "test fence"), ReasonFencedOutOfBand)
	}()

	select {
	case cancelled := <-rep.cancelled:
		if !cancelled {
			t.Fatal("the barrier context was live when FenceWrites had already sealed the volume")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the barrier never observed the fence seal")
	}
	close(vol.allowReturn)

	f := waitFatal(t, done, 10*time.Second, "the out-of-band fence did not stop the mount")
	if f.Exit != CodeFenced || f.ErrCode != ErrCodeFencedOutOfBand {
		t.Fatalf("exit = %d/%s (%v), want %d/%s", f.Exit, f.ErrCode, f.Err, CodeFenced, ErrCodeFencedOutOfBand)
	}
	for _, call := range vol.order() {
		if call == "barrier" {
			t.Fatalf("a cancelled periodic barrier began a flush after the seal: %v", vol.order())
		}
	}
}

type heldTxIDReplicator struct {
	fakeReplicator
	started chan struct{}
	release chan struct{}
}

func (r *heldTxIDReplicator) TxID(context.Context) (string, error) {
	close(r.started)
	<-r.release
	return "", nil
}

// TestAnOutOfBandFenceSealsBeforeANonCooperativeBarrierReturns exercises the
// opposite ordering from the cooperative case above. A stale writer must lose
// its metadata gate now; waiting for an arbitrary worker would let it keep
// committing until that worker happens to return.
func TestAnOutOfBandFenceSealsBeforeANonCooperativeBarrierReturns(t *testing.T) {
	vol := &sealGateVolume{
		fakeVolume:  healthyVolume(),
		sealed:      make(chan struct{}),
		allowReturn: make(chan struct{}),
	}
	rep := &heldTxIDReplicator{started: make(chan struct{}), release: make(chan struct{})}
	spec := testSpec()
	spec.LeaseExpiresAt = time.Now().UTC().Add(time.Second)
	sup := newSup(t, spec, &fakeFS{vol: vol.fakeVolume}, &fakeCP{}, &fakeReplicator{}, &fakeFencer{})
	sup.vol = vol
	sup.Deps.Replicator = rep
	sup.deadline = NewDeadline(spec.LeaseExpiresAt, spec.WriteStopMargin.D(), time.Now())
	w := sup.startWorkers(context.Background())
	w.barrierJobs <- struct{}{}
	select {
	case <-rep.started:
	case <-time.After(10 * time.Second):
		t.Fatal("the periodic barrier did not begin its replica-position read")
	}

	done := make(chan *Fatal, 1)
	go func() {
		done <- sup.fenceAndStop(fatalf(CodeFenced, ErrCodeFencedOutOfBand, false, "test fence"), ReasonFencedOutOfBand)
	}()
	select {
	case <-vol.sealed:
		if !vol.Fenced() {
			t.Fatal("FenceWrites returned without sealing the volume")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("out-of-band fence waited for a non-cooperative barrier")
	}
	close(vol.allowReturn)

	f := waitFatal(t, done, 5*time.Second, "the fenced supervisor did not return within its shutdown budget")
	if f.Exit != CodeBarrierIncomplete || f.ErrCode != ErrCodeBarrierIncomplete {
		t.Fatalf("exit = %d/%s (%v), want %d/%s", f.Exit, f.ErrCode, f.Err, CodeBarrierIncomplete, ErrCodeBarrierIncomplete)
	}
	for _, call := range vol.order() {
		if call == "detach" || call == "close" {
			t.Fatalf("unsafe teardown %q ran while the barrier was live: %v", call, vol.order())
		}
	}
	close(rep.release)
	w.wg.Wait()
	sealed := false
	for _, call := range vol.order() {
		if call == "fence" {
			sealed = true
		}
		if sealed && call == "barrier" {
			t.Fatalf("a barrier committed after the metadata seal: %v", vol.order())
		}
	}
}

// overlapLedger records which calls are in flight, and each call that began
// while a call it must never overlap was still running.
type overlapLedger struct {
	mu        sync.Mutex
	active    map[string]int
	conflicts []string
}

func newOverlapLedger() *overlapLedger {
	return &overlapLedger{active: map[string]int{}}
}

func (l *overlapLedger) enter(name string, excludes ...string) func() {
	l.mu.Lock()
	for _, other := range excludes {
		if l.active[other] > 0 {
			l.conflicts = append(l.conflicts, fmt.Sprintf("%s began while %s was in flight", name, other))
		}
	}
	l.active[name]++
	l.mu.Unlock()
	return func() {
		l.mu.Lock()
		l.active[name]--
		l.mu.Unlock()
	}
}

func (l *overlapLedger) settle() (inFlight, conflicts []string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for name, n := range l.active {
		if n != 0 {
			inFlight = append(inFlight, fmt.Sprintf("%s x%d", name, n))
		}
	}
	return inFlight, append([]string(nil), l.conflicts...)
}

var (
	// ledgerLoopWork is everything the run loop starts on its workers.
	ledgerLoopWork = []string{"barrier", "txid", "probe", "restart", "reload", "renew", "usage_walk", "usage_report"}
	// ledgerLifecycle acts on the replicator's process or registration.
	ledgerLifecycle = []string{"probe", "restart", "reload", "final_sync", "stop", "abort"}
)

// ledgerJitter holds a call open for up to three milliseconds, so the calls
// the loop starts interleave with the stop in as many orders as possible.
func ledgerJitter(ctx context.Context) {
	select {
	case <-time.After(time.Duration(rand.Intn(3000)) * time.Microsecond):
	case <-ctx.Done():
	}
}

type ledgerVolume struct {
	*fakeVolume
	ledger *overlapLedger
}

func (v ledgerVolume) Barrier(ctx context.Context) (BarrierResult, error) {
	defer v.ledger.enter("barrier", "barrier")()
	ledgerJitter(ctx)
	// The real ploriVolume runs FlushAll through the metadata write gate. A
	// barrier invocation that loses the race with FenceWrites therefore returns
	// EROFS before it can commit any staged slice. This fake models that owning
	// boundary under the same lock, so the concurrent-loop test asserts commits,
	// not call entry.
	v.fakeVolume.mu.Lock()
	defer v.fakeVolume.mu.Unlock()
	if v.fakeVolume.fenced {
		return BarrierResult{}, syscall.EROFS
	}
	v.fakeVolume.calls = append(v.fakeVolume.calls, "barrier")
	return BarrierResult{BarrierAt: time.Now().UTC()}, nil
}

func (v ledgerVolume) Usage(ctx context.Context, withTrash bool) (Usage, error) {
	if withTrash {
		defer v.ledger.enter("usage_walk")()
		ledgerJitter(ctx)
	}
	return v.fakeVolume.Usage(ctx, withTrash)
}

func (v ledgerVolume) Unmount(ctx context.Context) error {
	defer v.ledger.enter("unmount", ledgerLoopWork...)()
	return v.fakeVolume.Unmount(ctx)
}

func (v ledgerVolume) Detach(ctx context.Context) error {
	defer v.ledger.enter("detach", ledgerLoopWork...)()
	return v.fakeVolume.Detach(ctx)
}

func (v ledgerVolume) Close() error {
	defer v.ledger.enter("close", ledgerLoopWork...)()
	return v.fakeVolume.Close()
}

type ledgerReplicator struct {
	fakeReplicator
	ledger         *overlapLedger
	failFirstProbe bool
	probes         atomic.Int32
}

// heldReloadReplicator deliberately violates ReplicatorReloader's cancellation
// contract until the test releases it. It proves shutdown fences and leaves the
// volume untouched rather than racing a live worker through unmount or close.
type heldReloadReplicator struct {
	fakeReplicator
	started chan struct{}
	release chan struct{}
}

func (r *heldReloadReplicator) ReloadCredentials(context.Context) error {
	close(r.started)
	<-r.release
	return nil
}

func TestHeldCredentialReloadFencesWithoutTeardownOrCleanMarker(t *testing.T) {
	path := credentialFile(t, testKeyID, testSecret)
	vol := healthyVolume()
	rep := &heldReloadReplicator{started: make(chan struct{}), release: make(chan struct{})}
	spec := testSpec()
	spec.LeaseRenewInterval = Duration(time.Second)
	spec.LeaseExpiresAt = time.Now().UTC().Add(200 * time.Millisecond)
	spec.WriteStopMargin = Duration(50 * time.Millisecond)
	sup := newCloseoutSup(t, spec, vol, &fakeCP{}, rep, &fakeFencer{})
	sup.Deps.Credentials = testWatcher(t, path, (&capturedLog{}).fn)
	stop := make(chan os.Signal, 1)
	done := make(chan *Fatal, 1)
	go func() { done <- sup.Run(context.Background(), stop) }()
	waitFor(t, 10*time.Second, func() bool { return exists(t, sup.Paths.ReadyPath()) }, "mount never became ready")
	writeCredentialFile(t, path, testRotatedID, testSecret)
	select {
	case <-rep.started:
	case <-time.After(10 * time.Second):
		t.Fatal("credential reload never started")
	}
	stop <- syscall.SIGTERM
	f := waitFatal(t, done, 5*time.Second, "held reload retained the supervisor")
	if f.Exit != CodeBarrierIncomplete || f.ErrCode != ErrCodeBarrierIncomplete {
		t.Fatalf("exit = %d/%s, want incomplete fenced stop", f.Exit, f.ErrCode)
	}
	if !vol.Fenced() {
		t.Fatal("held worker stop did not fence writes")
	}
	for _, call := range vol.order() {
		if call == "barrier" || call == "unmount" || call == "detach" || call == "close" {
			t.Fatalf("unsafe teardown %q ran while reload was live: %v", call, vol.order())
		}
	}
	if exists(t, sup.Paths.CleanStopPath()) {
		t.Fatal("held worker stop wrote a clean marker")
	}
	close(rep.release)
}

func (r *ledgerReplicator) TxID(ctx context.Context) (string, error) {
	defer r.ledger.enter("txid")()
	ledgerJitter(ctx)
	return r.fakeReplicator.TxID(ctx)
}

func (r *ledgerReplicator) SyncAndWait(ctx context.Context) error {
	defer r.ledger.enter("final_sync", append(append([]string(nil), ledgerLoopWork...), ledgerLifecycle...)...)()
	return r.fakeReplicator.SyncAndWait(ctx)
}

func (r *ledgerReplicator) Stop(ctx context.Context) error {
	defer r.ledger.enter("stop", append(append([]string(nil), ledgerLoopWork...), ledgerLifecycle...)...)()
	return r.fakeReplicator.Stop(ctx)
}

func (r *ledgerReplicator) Abort(ctx context.Context) error {
	defer r.ledger.enter("abort", append(append([]string(nil), ledgerLoopWork...), ledgerLifecycle...)...)()
	return r.fakeReplicator.Abort(ctx)
}

func (r *ledgerReplicator) Probe(ctx context.Context) error {
	defer r.ledger.enter("probe", ledgerLifecycle...)()
	ledgerJitter(ctx)
	if r.probes.Add(1) == 1 && r.failFirstProbe {
		return errors.New("litestream stopped answering")
	}
	return nil
}

func (r *ledgerReplicator) Restart(ctx context.Context) error {
	defer r.ledger.enter("restart", ledgerLifecycle...)()
	ledgerJitter(ctx)
	return nil
}

func (r *ledgerReplicator) ReloadCredentials(ctx context.Context) error {
	defer r.ledger.enter("reload", ledgerLifecycle...)()
	ledgerJitter(ctx)
	return nil
}

type ledgerCP struct {
	*fakeCP
	ledger     *overlapLedger
	staleAfter int32
	renews     atomic.Int32
}

func (c *ledgerCP) RenewLease(ctx context.Context, volumeID string, epoch int64, req RenewRequest) (LeaseResponse, error) {
	defer c.ledger.enter("renew")()
	ledgerJitter(ctx)
	if n := c.renews.Add(1); c.staleAfter > 0 && n > c.staleAfter {
		c.record("renew")
		return LeaseResponse{}, &CPError{Status: http.StatusConflict, Code: CPCodeStaleEpoch}
	}
	return c.fakeCP.RenewLease(ctx, volumeID, epoch, req)
}

func (c *ledgerCP) ReportUsage(ctx context.Context, volumeID string, epoch int64, u Usage, at time.Time) error {
	defer c.ledger.enter("usage_report")()
	ledgerJitter(ctx)
	return c.fakeCP.ReportUsage(ctx, volumeID, epoch, u, at)
}

func (c *ledgerCP) ReleaseLease(ctx context.Context, volumeID string, epoch int64, reason string) error {
	defer c.ledger.enter("release", ledgerLoopWork...)()
	return c.fakeCP.ReleaseLease(ctx, volumeID, epoch, reason)
}

// TestNoLoopWorkOverlapsTheStopOrOutlivesIt drives the three ways a running
// mount stops — a SIGTERM, a replication failure, an out-of-band fence — at
// varying moments, with barriers, probes, a repair, a credential reload,
// renewals and usage walks all running on the workers, and checks the two
// things the workers must never break: no stop step overlaps work the loop
// started, no two replicator lifecycle calls overlap each other, and nothing
// is still running once Run has returned.
func TestNoLoopWorkOverlapsTheStopOrOutlivesIt(t *testing.T) {
	path := credentialFile(t, testKeyID, testSecret)
	ids := []string{testKeyID, testRotatedID}
	modes := []string{"sigterm", "replication", "fenced"}
	for i := 0; i < 12; i++ {
		mode := i % len(modes)
		t.Run(fmt.Sprintf("run%02d-%s", i, modes[mode]), func(t *testing.T) {
			ledger := newOverlapLedger()
			vol := ledgerVolume{fakeVolume: healthyVolume(), ledger: ledger}
			vol.setUsage(Usage{Bytes: 1 << 20, Inodes: 3}, nil)
			rep := &ledgerReplicator{ledger: ledger, failFirstProbe: mode == 1}
			cp := &ledgerCP{fakeCP: &fakeCP{}, ledger: ledger}
			if mode == 2 {
				cp.staleAfter = int32(3 + rand.Intn(10))
			}
			spec := testSpec()
			spec.LeaseRenewInterval = Duration(20 * time.Millisecond)
			sup := newCloseoutSup(t, spec, vol, cp, rep, &fakeFencer{})
			sup.Options.BarrierInterval = 40 * time.Millisecond
			sup.Deps.Credentials = testWatcher(t, path, (&capturedLog{}).fn)

			stop := make(chan os.Signal, 1)
			done := make(chan *Fatal, 1)
			go func() { done <- sup.Run(context.Background(), stop) }()
			waitFor(t, 10*time.Second, func() bool { return exists(t, sup.Paths.ReadyPath()) }, "the mount never became ready")
			time.Sleep(time.Duration(rand.Intn(300)) * time.Millisecond)
			writeCredentialFile(t, path, ids[(i+1)%2], testSecret)
			if mode == 0 {
				time.Sleep(time.Duration(rand.Intn(400)) * time.Millisecond)
				stop <- syscall.SIGTERM
			}
			f := waitFatal(t, done, 15*time.Second, "the supervisor did not stop")

			switch mode {
			case 0:
				if f.Exit != CodeOK {
					t.Errorf("exit = %d/%s (%v), want a clean stop", f.Exit, f.ErrCode, f.Err)
				}
			case 1:
				if f.Exit != CodeBarrierIncomplete || f.ErrCode != ErrCodeReplicationFailed {
					t.Errorf("exit = %d/%s (%v), want %d/%s", f.Exit, f.ErrCode, f.Err, CodeBarrierIncomplete, ErrCodeReplicationFailed)
				}
			case 2:
				if f.Exit != CodeFenced || f.ErrCode != ErrCodeFencedOutOfBand {
					t.Errorf("exit = %d/%s (%v), want %d/%s", f.Exit, f.ErrCode, f.Err, CodeFenced, ErrCodeFencedOutOfBand)
				}
				sealed := false
				for _, c := range vol.order() {
					if c == "fence" {
						sealed = true
					}
					if sealed && c == "barrier" {
						t.Errorf("a barrier ran after the seal: %v", vol.order())
						break
					}
				}
				if contains(rep.order(), "sync") {
					t.Errorf("an out-of-band stop synced the replica: %v", rep.order())
				}
			}
			inFlight, conflicts := ledger.settle()
			if len(inFlight) > 0 {
				t.Errorf("still in flight after Run returned: %v", inFlight)
			}
			for _, c := range conflicts {
				t.Error(c)
			}
		})
	}
}

// ----------------------------------------------------- SIGSTOP and the gate ---

const sigstopChildEnv = "PLORI_MOUNT_SIGSTOP_CHILD"

// TestAWriteGateArmedBeforeASigstopRefusesAfterTheThaw is threat-model.md
// §7.2 against a real stopped process rather than a fake clock. The child arms
// the write gate the way the supervisor does, is SIGSTOPped across its whole
// lease and continued, and the first operation after the thaw is refused: the
// gate reads the monotonic clock, which kept counting while the process was
// stopped, so nothing the renew loop did or did not do while frozen can let the
// write through (meta.PloriSetWriteExpiry).
func TestAWriteGateArmedBeforeASigstopRefusesAfterTheThaw(t *testing.T) {
	if os.Getenv(sigstopChildEnv) == "1" {
		sigstopChild()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestAWriteGateArmedBeforeASigstopRefusesAfterTheThaw$", "-test.count=1")
	cmd.Env = append(os.Environ(), sigstopChildEnv+"=1")
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start the child: %v", err)
	}
	defer func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Signal(syscall.SIGCONT)
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	lines := make(chan string, 8)
	go func() {
		defer close(lines)
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			if rest, ok := strings.CutPrefix(sc.Text(), "sigstop-child: "); ok {
				lines <- rest
			}
		}
	}()
	next := func() string {
		select {
		case line, ok := <-lines:
			if !ok {
				t.Fatal("the child exited without reporting")
			}
			return line
		case <-time.After(20 * time.Second):
			t.Fatal("the child did not report")
		}
		return ""
	}

	if line := next(); line != "armed" {
		t.Fatalf("child: %s", line)
	}
	if err := syscall.Kill(cmd.Process.Pid, syscall.SIGSTOP); err != nil {
		t.Fatalf("SIGSTOP: %v", err)
	}
	time.Sleep(1500 * time.Millisecond)
	if err := syscall.Kill(cmd.Process.Pid, syscall.SIGCONT); err != nil {
		t.Fatalf("SIGCONT: %v", err)
	}
	line := next()
	for _, want := range []string{"write_allowed=false", "expired=true", "stop_due=true"} {
		if !strings.Contains(line, want) {
			t.Errorf("after a 1.5 s SIGSTOP across a 1 s lease the child reported %q, want %s", line, want)
		}
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("child: %v", err)
	}
}

// sigstopChild is the stopped process. It prints `armed` once the gate is
// published, then waits to observe a gap in its own loop long enough to be the
// parent's SIGSTOP, and reports what the gate and the deadline say straight
// after it.
func sigstopChild() {
	say := func(format string, args ...any) { fmt.Printf("sigstop-child: "+format+"\n", args...) }
	vol := healthyVolume()
	now := time.Now()
	sup := &Supervisor{Spec: testSpec(), vol: vol, deadline: NewDeadline(now.UTC().Add(time.Second), 200*time.Millisecond, now)}
	sup.publishWriteExpiry()
	gate := vol.armedExpiry()
	// What the metadata engine does before every gated operation
	// (meta.ploriWriteRevoked): one monotonic clock read against the instant.
	write := func() bool { return time.Now().Before(gate) }
	if !write() {
		say("refused before the stop")
		return
	}
	say("armed")
	last := time.Now()
	for giveUp := time.Now().Add(10 * time.Second); time.Now().Before(giveUp); {
		time.Sleep(5 * time.Millisecond)
		gap := time.Since(last)
		last = time.Now()
		if gap < 1200*time.Millisecond {
			continue
		}
		at := time.Now()
		say("thawed gap=%s write_allowed=%t expired=%t stop_due=%t",
			gap.Round(time.Millisecond), write(), sup.deadline.Expired(at), sup.deadline.StopDue(at, 0))
		return
	}
	say("never stopped")
}
