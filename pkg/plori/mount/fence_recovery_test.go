//go:build plori
// +build plori

package mount

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"
)

// recordedPointAuthority is the lease double used by the recovery cases below.
// It keeps the last point accepted before a promotion and rejects a late report
// from the displaced epoch, as storagevol does for other holder mutations.
type recordedPointAuthority struct {
	*leaseAuthority
	mu       sync.Mutex
	dp       *DurablePointSpec
	attempts int
	rejected int
}

func (a *recordedPointAuthority) ReportDurablePoint(_ context.Context, _ string, epoch int64, r BarrierResult, txid string) error {
	a.mu.Lock()
	a.attempts++
	a.mu.Unlock()
	a.leaseAuthority.mu.Lock()
	current := a.leaseAuthority.current
	a.leaseAuthority.mu.Unlock()
	if epoch < current {
		a.mu.Lock()
		a.rejected++
		a.mu.Unlock()
		return &CPError{Status: 409, Code: CPCodeStaleEpoch, Msg: "the presented epoch was moved past"}
	}
	a.mu.Lock()
	a.dp = &DurablePointSpec{DurableAt: r.DurableAt, ReplicaTxID: txid, FenceEpoch: epoch}
	a.mu.Unlock()
	return nil
}

func (a *recordedPointAuthority) reports() (attempts, rejected int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.attempts, a.rejected
}

func (a *recordedPointAuthority) durablePoint() *DurablePointSpec {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.dp == nil {
		return nil
	}
	copy := *a.dp
	return &copy
}

// repairGateReplicator records the successor's restore request and refuses to
// start replication until the unclean-restore repair has run. It deliberately
// does not emulate a filesystem: Volume.RepairAfterRestore remains the real
// production seam for the JuiceFS fsck and missing-block repair.
type repairGateReplicator struct {
	fakeReplicator
	vol *fakeVolume
	mu  sync.Mutex
	src string
	opt RestoreOptions
}

// seedReportRejectedCP models a transient failure of the best-effort
// durable-point post. AckFormat remains available, which is the reachable
// first-generation state where a real filesystem is Ready but the control
// plane has no durable point for it.
type seedReportRejectedCP struct {
	*fakeCP
	mu      sync.Mutex
	reports int
}

func (c *seedReportRejectedCP) ReportDurablePoint(context.Context, string, int64, BarrierResult, string) error {
	c.mu.Lock()
	c.reports++
	c.mu.Unlock()
	return &CPError{Status: 503, Code: CPCodeInternal, Msg: "temporary durable-point outage"}
}

func (c *seedReportRejectedCP) reportCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reports
}

func (r *repairGateReplicator) Restore(_ context.Context, src string, opt RestoreOptions) error {
	r.mu.Lock()
	r.src, r.opt = src, opt
	r.mu.Unlock()
	r.record("restore")
	return nil
}

func (r *repairGateReplicator) Start(context.Context) error {
	r.vol.mu.Lock()
	repaired := r.vol.repaired
	r.vol.mu.Unlock()
	if repaired == 0 {
		return errors.New("replication started before unclean-restore repair")
	}
	r.record("start")
	return nil
}

func (r *repairGateReplicator) restored() (string, RestoreOptions) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.src, r.opt
}

// TestAFenceDuringABarrierKeepsThePriorRecordedPointForTheSuccessor models
// the reachable ordering: authority changes while Barrier is blocked, then the
// old worker's late durable-point report is rejected and its next serial renew
// observes stale_epoch. The supervisor has one loop, so it cannot observe that
// fence until the barrier returns; this test does not call fenceAndStop
// concurrently.
func TestAFenceDuringABarrierKeepsThePriorRecordedPointForTheSuccessor(t *testing.T) {
	auth := &recordedPointAuthority{leaseAuthority: newLeaseAuthority(11, 2*time.Minute)}
	prior := &DurablePointSpec{
		DurableAt:   time.Date(2026, 9, 7, 6, 40, 0, 0, time.UTC),
		ReplicaTxID: "000000000000000a",
		FenceEpoch:  10,
	}
	auth.dp = prior

	oldSpec := specAtEpoch(11, 2*time.Minute)
	oldSpec.LeaseRenewInterval = Duration(20 * time.Millisecond)
	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce sync.Once
	var releaseOnce sync.Once
	releaseBarrier := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseBarrier)
	oldVol := healthyVolume()
	oldVol.barrier = func(context.Context) (BarrierResult, error) {
		enteredOnce.Do(func() { close(entered) })
		<-release
		return BarrierResult{BarrierAt: time.Now().UTC()}, nil
	}
	oldRep := &countingReplicator{}
	old := newCloseoutSup(t, oldSpec, oldVol, auth, oldRep, newSharedFencer())
	done := make(chan *Fatal, 1)
	go func() { done <- old.Run(context.Background(), make(chan os.Signal)) }()

	waitFor(t, time.Second, func() bool { return readyExists(t, old) }, "old writer never became ready")
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("barrier did not begin")
	}
	// The authority is external to this process. The worker remains blocked in
	// Barrier until it returns, then receives stale_epoch on its next renew.
	auth.promote(12)
	releaseBarrier()
	f := waitFatal(t, done, 3*time.Second, "old writer did not observe stale_epoch")
	if f.Exit != CodeFenced || f.ErrCode != ErrCodeFencedOutOfBand {
		t.Fatalf("old writer exit = %d / %s (%v), want out-of-band fence", f.Exit, f.ErrCode, f.Err)
	}
	if !oldVol.Fenced() || !oldRep.has("abort") {
		t.Fatalf("out-of-band stop did not seal and abort: fenced=%t replica=%v", oldVol.Fenced(), oldRep.events)
	}
	if got := auth.durablePoint(); got == nil || got.FenceEpoch != prior.FenceEpoch || got.ReplicaTxID != prior.ReplicaTxID || !got.DurableAt.Equal(prior.DurableAt) {
		t.Fatalf("accepted durable point = %#v, want unchanged prior %#v", got, prior)
	}
	if attempts, rejected := auth.reports(); attempts != 1 || rejected != 1 {
		t.Fatalf("late durable-point reports = %d attempted / %d rejected, want 1 / 1", attempts, rejected)
	}

	// A different node has no old state-dir durable-point.json. It must use the
	// point the control plane accepted before the promotion, not the late tail
	// that the old node's NodeReplicator may have uploaded while unregistering.
	newSpec := specAtEpoch(12, 2*time.Minute)
	newSpec.RestoreFromPrefix = testVolumeRoot + "g10/"
	newSpec.DurablePoint = auth.durablePoint()
	newVol := healthyVolume()
	newRep := &repairGateReplicator{vol: newVol}
	newer := newCloseoutSup(t, newSpec, newVol, auth, newRep, newSharedFencer())
	if got := newer.Run(context.Background(), stopOnReady(newer)); got.Exit != CodeOK {
		t.Fatalf("successor exit = %d / %s: %v", got.Exit, got.ErrCode, got.Err)
	}
	if got, opt := newRep.restored(); got != newSpec.RestoreFromPrefix || opt.TXID != prior.ReplicaTxID {
		t.Fatalf("successor restore = %q / %#v, want %q at %q", got, opt, newSpec.RestoreFromPrefix, prior.ReplicaTxID)
	}
	if !contains(newVol.order(), "repair") {
		t.Fatalf("successor did not repair its unclean restore: %v", newVol.order())
	}
}

// TestAFirstGenerationSuccessorWithoutADurablePointRepairsBeforeReady covers
// the remaining no-DP state. New format seeds a replica before Ready, but the
// durable-point post is best effort: a transient failure can leave a valid,
// Ready filesystem without a control-plane point. Its successor restores the
// newest populated prefix without an anchor, and must finish the unconditional
// repair before it starts replication or publishes Ready.
func TestAFirstGenerationSuccessorWithoutADurablePointRepairsBeforeReady(t *testing.T) {
	// First prove the reachable source state, rather than assuming that no
	// writes can exist before Ready. The seed synchronises, its report fails,
	// and the later format acknowledgement still publishes Ready.
	sourceCP := &seedReportRejectedCP{fakeCP: &fakeCP{}}
	sourceVol := healthyVolume()
	source := newCloseoutSup(t, bootstrapSpec(), sourceVol, sourceCP,
		&fakeReplicator{restoreErr: ErrReplicaEmpty}, &fakeFencer{})
	if got := source.Run(context.Background(), stopOnReady(source)); got.Exit != CodeOK {
		t.Fatalf("seed-report-rejected source exit = %d / %s: %v", got.Exit, got.ErrCode, got.Err)
	}
	if !readyExists(t, source) || sourceCP.reportCount() == 0 {
		t.Fatalf("source ready=%t durable-point attempts=%d, want Ready after a rejected seed report", readyExists(t, source), sourceCP.reportCount())
	}

	spec := testSpec()
	spec.FenceEpoch = 2
	spec.MetaPrefix = testVolumeRoot + "g2/"
	spec.FenceMarkerKey = spec.MetaPrefix + "fence"
	spec.Generation = 1
	if spec.DurablePoint != nil || spec.RestoreFromPrefix != "" {
		t.Fatal("no-DP successor must not carry a restore anchor")
	}
	fencer := &countingFencer{prior: testVolumeRoot + "g1/"}
	vol := healthyVolume()
	rep := &repairGateReplicator{vol: vol}
	sup := newCloseoutSup(t, spec, vol, &fakeCP{}, rep, fencer)
	if got := sup.Run(context.Background(), stopOnReady(sup)); got.Exit != CodeOK {
		t.Fatalf("no-DP successor exit = %d / %s: %v", got.Exit, got.ErrCode, got.Err)
	}
	if got, opt := rep.restored(); got != fencer.prior || !opt.Timestamp.IsZero() || opt.TXID != "" {
		t.Fatalf("no-DP restore = %q / %#v, want latest populated %q with no anchor", got, opt, fencer.prior)
	}
	if fencer.lists() != 1 {
		t.Fatalf("metadata-prefix list calls = %d, want one", fencer.lists())
	}
	if !contains(vol.order(), "repair") {
		t.Fatalf("no-DP successor did not repair before ready: %v", vol.order())
	}
}
