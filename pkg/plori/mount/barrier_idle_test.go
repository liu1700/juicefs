//go:build plori
// +build plori

package mount

import (
	"context"
	"encoding/json"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

// fixedTxIDReplicator is a replica that never moves: the mount is up, nothing
// is being written to it, and every anchor read answers the same transaction.
type fixedTxIDReplicator struct {
	fakeReplicator
	txid atomic.Value // string
}

func (r *fixedTxIDReplicator) TxID(context.Context) (string, error) {
	r.record("txid")
	v, _ := r.txid.Load().(string)
	return v, nil
}

func idleSup(t *testing.T, vol *fakeVolume, cp *fakeCP, rep *fixedTxIDReplicator) *Supervisor {
	t.Helper()
	sup := newSup(t, testSpec(), &fakeFS{vol: vol}, cp, &rep.fakeReplicator, &fakeFencer{})
	sup.Deps.Replicator = rep
	sup.vol = vol
	sup.drain = NewDrainModel(DefaultDrainPerBlock)
	sup.deadline = NewDeadline(time.Now().UTC().Add(time.Hour), 0, time.Now())
	if err := os.MkdirAll(sup.Paths.StateDir, 0o700); err != nil {
		t.Fatalf("state dir: %v", err)
	}
	return sup
}

func countCalls(calls []string, name string) int {
	n := 0
	for _, c := range calls {
		if c == name {
			n++
		}
	}
	return n
}

func readDurablePoint(t *testing.T, sup *Supervisor) DurablePoint {
	t.Helper()
	data, err := os.ReadFile(sup.Paths.DurablePointPath())
	if err != nil {
		t.Fatalf("read durable-point.json: %v", err)
	}
	var dp DurablePoint
	if err := json.Unmarshal(data, &dp); err != nil {
		t.Fatalf("decode durable-point.json: %v", err)
	}
	return dp
}

// PLO-552. The barrier period is the acknowledged-write loss window, so it was
// cut from 60 s to 5 s — which is only affordable if a mount that nothing is
// writing to costs nothing to barrier. It already cost nothing in the object
// store (a fence on an empty queue completes in tens of milliseconds and issues
// no PUT, ADR §4 B4), but it reported a durable point every single tick: one
// control-plane call per mount per period, forever, naming the same replica
// transaction and the same empty backlog as the tick before it. At 5 s across
// the 100-mount target that is 20 calls/s of traffic carrying no information.
//
// So an idle tick reports nothing and the last durable point stands. The
// barrier itself still runs — it is the drain measurement and the freshness of
// last_barrier_at — and the moment anything moves, the next tick reports.
func TestAnIdleBarrierMakesNoRequest(t *testing.T) {
	rep := &fixedTxIDReplicator{}
	rep.txid.Store(formatTXID(7))
	vol := healthyVolume()
	cp := &fakeCP{}
	sup := idleSup(t, vol, cp, rep)

	// The first barrier of a generation always reports: it is this epoch's
	// anchor, and there is nothing yet for it to stand on.
	sup.runBarrier(context.Background())
	if got := countCalls(cp.order(), "durable_point"); got != 1 {
		t.Fatalf("the first barrier reported %d durable points, want exactly 1 — the epoch has no anchor without it", got)
	}
	first := readDurablePoint(t, sup)

	// Nothing writes to the mount. Every tick from here finds an empty backlog
	// and an unmoved replica.
	for i := 0; i < 5; i++ {
		sup.runBarrier(context.Background())
	}

	if got := countCalls(cp.order(), "durable_point"); got != 1 {
		t.Errorf("after five idle barriers the control-plane had been called %d times, want the 1 from the first barrier — an idle mount must cost nothing", got)
	}
	if got := readDurablePoint(t, sup); !got.DurableAt.Equal(first.DurableAt) {
		t.Errorf("durable-point.json was rewritten by an idle barrier (durable_at %s -> %s); the last durable point stands",
			first.DurableAt, got.DurableAt)
	}
	if got, want := countCalls(vol.order(), "barrier"), 6; got != want {
		t.Errorf("the volume was barriered %d times, want %d — skipping the REPORT must not skip the barrier: it is the drain measurement", got, want)
	}
	if n := countCalls(rep.order(), "sync"); n != 0 {
		t.Errorf("an idle barrier forced %d replica syncs beyond the anchor read, want 0", n)
	}
	// health.json is the plugin's and the alert's view of whether this mount is
	// still barriering at all. It must keep moving even when no durable point
	// does, or an idle mount reads as one whose T_before has stalled.
	sup.writeHealth()
	data, err := os.ReadFile(sup.Paths.HealthPath())
	if err != nil {
		t.Fatalf("read health.json: %v", err)
	}
	var h Health
	if err := json.Unmarshal(data, &h); err != nil {
		t.Fatalf("decode health.json: %v", err)
	}
	if !h.LastBarrierAt.After(first.BarrierAt) {
		t.Errorf("health.json last_barrier_at = %s, want later than the first barrier at %s — an idle mount must not look stalled",
			h.LastBarrierAt, first.BarrierAt)
	}
}

// The other half of the same condition: anything that actually moved still
// reports, on the tick it moved. Two ways it can move, and the skip must miss
// neither — a metadata transaction (the replica advances) and a writeback
// backlog the barrier drained (the replica may not advance at all, because the
// transaction that referenced those blocks committed before the last report).
func TestADirtyBarrierStillReportsTheDurablePoint(t *testing.T) {
	for _, tc := range []struct {
		name    string
		dirty   func(rep *fixedTxIDReplicator, vol *fakeVolume)
		wantWhy string
	}{
		{
			name: "the replica advanced",
			dirty: func(rep *fixedTxIDReplicator, _ *fakeVolume) {
				rep.txid.Store(formatTXID(8))
			},
			wantWhy: "a committed metadata transaction is a new durable point",
		},
		{
			name: "the barrier drained a backlog",
			dirty: func(_ *fixedTxIDReplicator, vol *fakeVolume) {
				vol.pending.Store(12)
			},
			wantWhy: "blocks that were staged and are now durable are a new durable point even when the replica did not move",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rep := &fixedTxIDReplicator{}
			rep.txid.Store(formatTXID(7))
			vol := healthyVolume()
			cp := &fakeCP{}
			sup := idleSup(t, vol, cp, rep)

			sup.runBarrier(context.Background())
			sup.runBarrier(context.Background())
			if got := countCalls(cp.order(), "durable_point"); got != 1 {
				t.Fatalf("setup: %d reports before anything changed, want 1", got)
			}

			tc.dirty(rep, vol)
			sup.runBarrier(context.Background())

			if got := countCalls(cp.order(), "durable_point"); got != 2 {
				t.Errorf("%d durable points reported, want 2: %s", got, tc.wantWhy)
			}
		})
	}
}

// An anchor the worker cannot read is not an anchor that matches. A replicator
// answering nothing must never be mistaken for a replica that has not moved,
// or a mount whose Litestream control socket is wedged would stop reporting
// durable points entirely and nobody would learn it from the traffic.
func TestAnUnreadableAnchorIsNeverTreatedAsIdle(t *testing.T) {
	rep := &silentTxIDReplicator{}
	vol := healthyVolume()
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
	sup.runBarrier(context.Background())
	sup.runBarrier(context.Background())

	if got := countCalls(cp.order(), "durable_point"); got != 3 {
		t.Errorf("%d durable points reported, want 3 — an empty anchor matched an empty anchor and the mount went silent", got)
	}
}

// The period is the loss window, and it is the number the whole ruling is
// about. A drift here is a durability change, so it is asserted rather than
// left to a comment.
func TestTheDefaultBarrierPeriodIsTheLossWindowWeDocument(t *testing.T) {
	if DefaultBarrierInterval != 5*time.Second {
		t.Fatalf("DefaultBarrierInterval = %s, want 5s (PLO-552): it is the acknowledged-write loss window on node loss, and the contract doc and the control-plane's golden MountSpec say 5",
			DefaultBarrierInterval)
	}
	var s Supervisor
	if got := s.barrierInterval(); got != 5*time.Second {
		t.Errorf("a spec that omits barrier_interval falls back to %s, want 5s", got)
	}
}
