//go:build plori
// +build plori

package mount

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// advancingReplicator answers a different transaction id every time it is
// asked, which is what a replicator attached to a live filesystem does: the
// mount keeps serving throughout the barrier, so the replica keeps moving.
type advancingReplicator struct {
	fakeReplicator
	txid atomic.Int64
}

func (r *advancingReplicator) TxID(context.Context) (string, error) {
	return formatTXID(uint64(r.txid.Load())), nil
}

func formatTXID(n uint64) string {
	const hex = "0123456789abcdef"
	out := make([]byte, 16)
	for i := 15; i >= 0; i-- {
		out[i] = hex[n&0xf]
		n >>= 4
	}
	return string(out)
}

// PLO-416. The durable point promises that a restore to it lands on a tree
// whose every block exists in the object store, and since fork #47 a restore
// PREFERS the txid over the timestamp. The barrier makes durable exactly the
// blocks staged when it started; a transaction committed while it ran
// references blocks staged after that, which the barrier never waited on. So
// the anchor has to be the replica position at T_before, and reading it after
// the barrier — which is what the code did — recorded a position that can name
// blocks the object store does not have.
func TestTheDurablePointsTxIDIsReadBeforeTheBarrierNotAfter(t *testing.T) {
	rep := &advancingReplicator{}
	rep.txid.Store(7)

	vol := healthyVolume()
	// The write that races the barrier. It happens INSIDE the barrier, which is
	// the only place the two readings can differ.
	vol.barrier = func(context.Context) (BarrierResult, error) {
		rep.txid.Store(9)
		return BarrierResult{BarrierAt: time.Now().UTC()}, nil
	}

	cp := &fakeCP{}
	sup := newSup(t, testSpec(), &fakeFS{vol: vol}, cp, &rep.fakeReplicator, &fakeFencer{})
	sup.Deps.Replicator = rep
	sup.vol = vol
	sup.drain = NewDrainModel(DefaultDrainPerBlock)
	sup.deadline = NewDeadline(time.Now().UTC().Add(time.Hour), 0, time.Now())
	if err := os.MkdirAll(sup.Paths.StateDir, 0o700); err != nil {
		t.Fatalf("state dir: %v", err)
	}

	sup.runBarrier(context.Background())

	data, err := os.ReadFile(sup.Paths.DurablePointPath())
	if err != nil {
		t.Fatalf("read durable-point.json: %v", err)
	}
	var dp DurablePoint
	if err := json.Unmarshal(data, &dp); err != nil {
		t.Fatalf("decode durable-point.json: %v", err)
	}
	if want := formatTXID(7); dp.ReplicaTxID != want {
		t.Fatalf("recorded replica txid = %q, want %q — the anchor includes a transaction committed during the barrier, whose blocks the barrier never waited on",
			dp.ReplicaTxID, want)
	}
	cp.mu.Lock()
	reported := cp.durableTxID
	cp.mu.Unlock()
	if reported != formatTXID(7) {
		t.Errorf("the control-plane was told txid %q, want %q — the local copy and the remote one must name the same instant",
			reported, formatTXID(7))
	}
	if dp.DurableAt.IsZero() {
		t.Error("the durable point carries no T_before; the timestamp fallback would have nothing to restore to")
	}
}

// A failed metadata sync proves that this generation has no readable replica.
// The barrier still drains staged blocks and health still advances, but neither
// the local file nor the control plane may be overwritten with an unrecoverable
// epoch. This is distinct from a successful sync that has no transaction yet.
func TestAFailedMetadataSyncKeepsThePriorDurablePoint(t *testing.T) {
	rep := &silentTxIDReplicator{}
	vol := healthyVolume()
	vol.barrier = func(context.Context) (BarrierResult, error) {
		return BarrierResult{BarrierAt: time.Now().UTC()}, nil
	}
	cp := &fakeCP{}
	sup := newSup(t, testSpec(), &fakeFS{vol: vol}, cp, &rep.fakeReplicator, &fakeFencer{})
	sup.Deps.Replicator = rep
	sup.vol = vol
	sup.drain = NewDrainModel(DefaultDrainPerBlock)
	sup.deadline = NewDeadline(time.Now().UTC().Add(time.Hour), 0, time.Now())
	if err := os.MkdirAll(sup.Paths.StateDir, 0o700); err != nil {
		t.Fatalf("state dir: %v", err)
	}
	prior := DurablePoint{Volume: sup.Spec.StorageVolumeID, FenceEpoch: sup.Spec.FenceEpoch - 1,
		DurableAt: time.Date(2026, 9, 6, 20, 16, 25, 0, time.UTC), BarrierAt: time.Date(2026, 9, 6, 20, 16, 26, 0, time.UTC), ReplicaTxID: formatTXID(7)}
	if err := writeJSONAtomic(sup.Paths.DurablePointPath(), prior); err != nil {
		t.Fatalf("write prior durable point: %v", err)
	}

	sup.runBarrier(context.Background())

	got, err := ReadDurablePoint(sup.Paths.DurablePointPath())
	if err != nil {
		t.Fatalf("read durable point: %v", err)
	}
	if got == nil || got.FenceEpoch != prior.FenceEpoch || !got.DurableAt.Equal(prior.DurableAt) || got.ReplicaTxID != prior.ReplicaTxID {
		t.Fatalf("durable point = %#v, want prior %#v", got, prior)
	}
	if got := countCalls(cp.order(), "durable_point"); got != 0 {
		t.Errorf("control-plane durable reports = %d, want 0 after failed metadata sync", got)
	}
	if sup.lastBarrier.BarrierAt.IsZero() {
		t.Error("failed metadata sync prevented the completed barrier from updating health")
	}
}

func TestSuccessfulSyncWithoutReplicaPositionUsesTimestampFallback(t *testing.T) {
	rep := &emptyTxIDReplicator{}
	vol := healthyVolume()
	vol.barrier = func(context.Context) (BarrierResult, error) { return BarrierResult{BarrierAt: time.Now().UTC()}, nil }
	cp := &fakeCP{}
	sup := newSup(t, testSpec(), &fakeFS{vol: vol}, cp, &rep.fakeReplicator, &fakeFencer{})
	sup.Deps.Replicator = rep
	sup.vol = vol
	sup.drain = NewDrainModel(DefaultDrainPerBlock)
	sup.deadline = NewDeadline(time.Now().UTC().Add(time.Hour), 0, time.Now())
	if err := os.MkdirAll(sup.Paths.StateDir, 0o700); err != nil {
		t.Fatal(err)
	}

	sup.runBarrier(context.Background())
	got, err := ReadDurablePoint(sup.Paths.DurablePointPath())
	if err != nil || got == nil {
		t.Fatalf("read timestamp fallback point: %v", err)
	}
	if got.ReplicaTxID != "" {
		t.Errorf("replica txid = %q, want empty after a successful positionless sync", got.ReplicaTxID)
	}
	if got.DurableAt.IsZero() {
		t.Error("successful positionless sync did not record its timestamp anchor")
	}
	if calls := countCalls(cp.order(), "durable_point"); calls != 1 {
		t.Errorf("control-plane durable reports = %d, want 1", calls)
	}
}

type silentTxIDReplicator struct {
	fakeReplicator
	mu sync.Mutex
}

func (r *silentTxIDReplicator) TxID(context.Context) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return "", errReplicaUnavailable
}

var errReplicaUnavailable = &controlStatusError{Route: "/sync", Status: 500, Body: "replica unavailable"}

type emptyTxIDReplicator struct{ fakeReplicator }

func (r *emptyTxIDReplicator) TxID(context.Context) (string, error) { return "", nil }

func TestShutdownAfterFailedMetadataSyncKeepsThePriorDurablePoint(t *testing.T) {
	rep := &silentTxIDReplicator{}
	vol := healthyVolume()
	cp := &fakeCP{}
	sup := newSup(t, testSpec(), &fakeFS{vol: vol}, cp, &rep.fakeReplicator, &fakeFencer{})
	sup.Deps.Replicator = rep
	sup.vol = vol
	sup.drain = NewDrainModel(DefaultDrainPerBlock)
	sup.deadline = NewDeadline(time.Now().UTC().Add(time.Hour), 0, time.Now())
	if err := os.MkdirAll(sup.Paths.StateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	prior := DurablePoint{Volume: sup.Spec.StorageVolumeID, FenceEpoch: sup.Spec.FenceEpoch - 1,
		DurableAt: time.Date(2026, 9, 6, 20, 16, 25, 0, time.UTC), BarrierAt: time.Date(2026, 9, 6, 20, 16, 26, 0, time.UTC), ReplicaTxID: formatTXID(7)}
	if err := writeJSONAtomic(sup.Paths.DurablePointPath(), prior); err != nil {
		t.Fatalf("write prior durable point: %v", err)
	}

	if got := sup.shutdown(context.Background(), ReasonShutdown); got.Exit != CodeOK {
		t.Fatalf("shutdown exit = %d, want %d (%v)", got.Exit, CodeOK, got.Err)
	}
	got, err := ReadDurablePoint(sup.Paths.DurablePointPath())
	if err != nil || got == nil {
		t.Fatalf("read durable point: %v", err)
	}
	if got.FenceEpoch != prior.FenceEpoch || !got.DurableAt.Equal(prior.DurableAt) || got.ReplicaTxID != prior.ReplicaTxID {
		t.Fatalf("durable point = %#v, want prior %#v", got, prior)
	}
	if calls := countCalls(cp.order(), "durable_point"); calls != 0 {
		t.Errorf("control-plane durable reports = %d, want 0 after failed shutdown metadata sync", calls)
	}
}
