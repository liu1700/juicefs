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

// The barrier is worth running even when the anchor cannot be read: the blocks
// still become durable, and T_before alone is the pre-#47 restore point. A
// replicator that cannot answer must therefore degrade, not abort.
func TestAnUnreadableAnchorStillRecordsTheDurablePoint(t *testing.T) {
	rep := &silentTxIDReplicator{}
	vol := healthyVolume()
	vol.barrier = func(context.Context) (BarrierResult, error) {
		return BarrierResult{BarrierAt: time.Now().UTC()}, nil
	}
	sup := newSup(t, testSpec(), &fakeFS{vol: vol}, &fakeCP{}, &rep.fakeReplicator, &fakeFencer{})
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
		t.Fatalf("a barrier whose anchor could not be read wrote no durable point at all: %v", err)
	}
	var dp DurablePoint
	if err := json.Unmarshal(data, &dp); err != nil {
		t.Fatalf("decode durable-point.json: %v", err)
	}
	if dp.ReplicaTxID != "" {
		t.Errorf("replica txid = %q, want empty rather than a guess", dp.ReplicaTxID)
	}
	if dp.DurableAt.IsZero() {
		t.Error("T_before is missing, so the timestamp fallback has nothing to restore to either")
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
