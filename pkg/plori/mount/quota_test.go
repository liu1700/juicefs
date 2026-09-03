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
	"syscall"
	"testing"
	"time"
)

// The grant conversation, from the supervisor's side. What the metadata engine
// does with a ceiling is proved in pkg/meta and pkg/vfs; this is the state
// machine around it — when a grant is applied, when the acknowledgement is
// sent, and when the worker asks for more.
//
// The whole conversation rides the lease renewal. That is the design decision
// worth testing rather than describing: renewal is the only regular round trip
// a live mount makes, it is already authorised as the lease holder, and both
// halves of the grant exchange are facts that are only true while the holder
// holds the lease.

// runUntil starts the supervisor, waits for `ready`, then stops it cleanly and
// returns the exit. A test that just slept would be timing-coupled to the renew
// interval; this one is coupled to the thing it is actually waiting for.
func runUntil(t *testing.T, sup *Supervisor, what string, ready func() bool) *Fatal {
	t.Helper()
	stop := make(chan os.Signal, 1)
	done := make(chan *Fatal, 1)
	go func() { done <- sup.Run(context.Background(), stop) }()

	deadline := time.Now().Add(10 * time.Second)
	for !ready() {
		select {
		case got := <-done:
			t.Fatalf("the supervisor exited (%d / %v) before %s", got.Exit, got.Err, what)
		default:
		}
		if time.Now().After(deadline) {
			stop <- syscall.SIGTERM
			<-done
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
	stop <- syscall.SIGTERM
	select {
	case got := <-done:
		return got
	case <-time.After(10 * time.Second):
		t.Fatal("the supervisor did not stop")
		return nil
	}
}

func readHealth(t *testing.T, sup *Supervisor) Health {
	t.Helper()
	data, err := os.ReadFile(sup.Paths.HealthPath())
	if err != nil {
		t.Fatalf("read health.json: %s", err)
	}
	var h Health
	if err := json.Unmarshal(data, &h); err != nil {
		t.Fatalf("decode health.json: %s", err)
	}
	return h
}

// healthWhenWritten is readHealth for a POLLING assertion: health.json appears
// at the end of the first renew, so a poll that started before it must be able
// to say "not yet" instead of failing the test from inside the loop.
func healthWhenWritten(sup *Supervisor) (Health, bool) {
	data, err := os.ReadFile(sup.Paths.HealthPath())
	if err != nil {
		return Health{}, false
	}
	var h Health
	if err := json.Unmarshal(data, &h); err != nil {
		return Health{}, false
	}
	return h, true
}

func countGrows(reqs []RenewRequest) int {
	n := 0
	for _, r := range reqs {
		if r.Grow {
			n++
		}
	}
	return n
}

// TestTheSpecsGrantIsAppliedBeforeTheMountServes closes the window a resumed
// Agent used to run in. The restored replica's Format carries whatever ceiling
// the PREVIOUS generation persisted; the allocator may have reclaimed or raised
// it while the volume had no writer, and the spec is the authority. Applying it
// during startup rather than on the first renew removes a whole renew interval
// of enforcing a stale number.
func TestTheSpecsGrantIsAppliedBeforeTheMountServes(t *testing.T) {
	vol := healthyVolume()
	cp := &fakeCP{grant: GrantSpec{Bytes: 10 << 30, Inodes: 1000000, Epoch: 2, AckedEpoch: 1}}
	sup := newSup(t, testSpec(), &fakeFS{vol: vol}, cp, &fakeReplicator{}, &fakeFencer{})

	runUntil(t, sup, "the first renew", func() bool { return len(cp.renewRequests()) > 0 })

	grants := vol.appliedGrants()
	if len(grants) == 0 {
		t.Fatal("the spec's grant was never applied")
	}
	if grants[0] != [2]int64{10 << 30, 1000000} {
		t.Errorf("first applied ceiling = %v, want the spec's {10737418240 1000000}", grants[0])
	}
	// The first renew carries the acknowledgement, because that is the round
	// trip the ack rides. Nothing else on the wire says "applied".
	if got := cp.renewRequests()[0].AckedGrantEpoch; got != 2 {
		t.Errorf("first renew acked epoch %d, want the spec's grant epoch 2", got)
	}
}

// TestAGrantIssuedMidFlightIsAppliedAndAckedOnce is the live path: the
// allocator moves a running mount's ceiling, the renew response carries it, the
// worker enforces it, and the NEXT renew tells the control-plane so — once.
//
// The "once" matters. The allocator uses the acknowledgement to tell an issued
// ceiling from an enforced one; re-sending it every tick would be harmless but
// would also mean the worker was not tracking what the server confirmed, which
// is exactly what makes a lost response recoverable.
func TestAGrantIssuedMidFlightIsAppliedAndAckedOnce(t *testing.T) {
	vol := healthyVolume()
	spec := testSpec()
	spec.Grant = GrantSpec{Bytes: 256 << 20, Inodes: 65536, Epoch: 2, AckedEpoch: 2}
	cp := &fakeCP{grant: spec.Grant}
	sup := newSup(t, spec, &fakeFS{vol: vol}, cp, &fakeReplicator{}, &fakeFencer{})

	stop := make(chan os.Signal, 1)
	done := make(chan *Fatal, 1)
	go func() { done <- sup.Run(context.Background(), stop) }()
	t.Cleanup(func() { stop <- syscall.SIGTERM; <-done })

	// Wait for the mount to be renewing, then move the ceiling.
	waitFor(t, 10*time.Second, func() bool { return len(cp.renewRequests()) > 0 }, "timed out waiting for the first renew")
	cp.mu.Lock()
	cp.grant = GrantSpec{Bytes: 512 << 20, Inodes: 131072, Epoch: 3, AckedEpoch: 2}
	cp.mu.Unlock()

	waitFor(t, 10*time.Second, func() bool {
		for _, r := range cp.renewRequests() {
			if r.AckedGrantEpoch == 3 {
				return true
			}
		}
		return false
	}, "timed out waiting for the new grant to be acknowledged")

	grants := vol.appliedGrants()
	if last := grants[len(grants)-1]; last != [2]int64{512 << 20, 131072} {
		t.Errorf("last applied ceiling = %v, want {536870912 131072}", last)
	}

	// Let several more renews go by and check the ack was not repeated.
	before := len(cp.renewRequests())
	waitFor(t, 10*time.Second, func() bool { return len(cp.renewRequests()) >= before+3 }, "timed out waiting for three more renews")
	acks := 0
	for _, r := range cp.renewRequests() {
		if r.AckedGrantEpoch == 3 {
			acks++
		}
	}
	if acks != 1 {
		t.Errorf("epoch 3 was acknowledged %d times, want 1", acks)
	}
}

// TestAQuotaTripAsksToGrowOncePerEpoch is the no-storm rule.
//
// The ceiling refuses EVERY write of a full filesystem — a `git clone` against
// a full volume trips it thousands of times a second — so the trigger has to be
// "something was refused since I last asked", not "something is refusing". One
// request per grant epoch is enough, because the answer to the request is a new
// epoch.
func TestAQuotaTripAsksToGrowOncePerEpoch(t *testing.T) {
	vol := healthyVolume()
	// An allocator that hears the request and cannot answer it yet.
	cp := &fakeCP{grant: GrantSpec{Bytes: 64 << 20, Inodes: 16384, Epoch: 2, AckedEpoch: 2}}
	sup := newSup(t, testSpec(), &fakeFS{vol: vol}, cp, &fakeReplicator{}, &fakeFencer{})

	stop := make(chan os.Signal, 1)
	done := make(chan *Fatal, 1)
	go func() { done <- sup.Run(context.Background(), stop) }()
	t.Cleanup(func() { stop <- syscall.SIGTERM; <-done })

	waitFor(t, 10*time.Second, func() bool { return len(cp.renewRequests()) > 0 }, "timed out waiting for the first renew")
	if got := countGrows(cp.renewRequests()); got != 0 {
		t.Fatalf("a mount that has not been refused anything asked to grow %d times", got)
	}

	// The filesystem fills up, and keeps being refused.
	for range 5000 {
		vol.quotaTrips.Add(1)
	}
	waitFor(t, 10*time.Second, func() bool { return countGrows(cp.renewRequests()) > 0 }, "timed out waiting for the grow request")

	before := len(cp.renewRequests())
	waitFor(t, 10*time.Second, func() bool { return len(cp.renewRequests()) >= before+5 }, "timed out waiting for five more renews")
	for range 5000 {
		vol.quotaTrips.Add(1)
	}
	waitFor(t, 10*time.Second, func() bool { return len(cp.renewRequests()) >= before+8 }, "timed out waiting for three more renews")

	if got := countGrows(cp.renewRequests()); got != 1 {
		t.Errorf("%d grow requests across %d renews on one grant epoch, want 1",
			got, len(cp.renewRequests()))
	}
	if h := readHealth(t, sup); !h.QuotaExhausted {
		t.Error("health.json must report quota_exhausted while the ceiling is refusing writes")
	}
}

// TestAGrowTheAccountCannotFundIsAskedAgain is the other half of the no-storm
// rule, and the reason it is not simply "once, ever".
//
// The way out of a full account is the user buying disk, and the billing hook
// that follows a purchase reclaims and compacts — it does not GROW a volume
// that is already at its ceiling (storagequota.Rebalance). So a worker that
// asked once, was told the account was full, and never asked again would stay
// stuck after the user paid to unstick it.
func TestAGrowTheAccountCannotFundIsAskedAgain(t *testing.T) {
	vol := healthyVolume()
	// The spec's ceiling and the allocator's are the same number, because they
	// are the same grant: "larger" below has to be larger than what this mount
	// is actually enforcing, and the spec is what it started from.
	spec := testSpec()
	spec.Grant = GrantSpec{Bytes: 64 << 20, Inodes: 16384, Epoch: 2, AckedEpoch: 2}
	cp := &fakeCP{grant: spec.Grant, overBudget: true}
	sup := newSup(t, spec, &fakeFS{vol: vol}, cp, &fakeReplicator{}, &fakeFencer{})

	stop := make(chan os.Signal, 1)
	done := make(chan *Fatal, 1)
	go func() { done <- sup.Run(context.Background(), stop) }()
	t.Cleanup(func() { stop <- syscall.SIGTERM; <-done })

	waitFor(t, 10*time.Second, func() bool { return len(cp.renewRequests()) > 0 }, "timed out waiting for the first renew")
	vol.quotaTrips.Add(1)
	waitFor(t, 10*time.Second, func() bool { return countGrows(cp.renewRequests()) >= 2 }, "timed out waiting for a second grow request")

	// And the account paying up ends it: a new epoch closes the exhausted
	// state, and nothing asks again.
	cp.mu.Lock()
	cp.overBudget = false
	cp.grant = GrantSpec{Bytes: 128 << 20, Inodes: 32768, Epoch: 3, AckedEpoch: 2}
	cp.mu.Unlock()

	waitFor(t, 10*time.Second, func() bool {
		g := vol.appliedGrants()
		return len(g) > 0 && g[len(g)-1] == [2]int64{128 << 20, 32768}
	}, "timed out waiting for the larger grant to be applied")
	settled := countGrows(cp.renewRequests())
	before := len(cp.renewRequests())
	waitFor(t, 10*time.Second, func() bool { return len(cp.renewRequests()) >= before+5 }, "timed out waiting for five more renews")
	if got := countGrows(cp.renewRequests()); got != settled {
		t.Errorf("%d grow requests after the grant landed, want no more than the %d already sent", got, settled)
	}
	if h := readHealth(t, sup); h.QuotaExhausted {
		t.Error("quota_exhausted must clear when a larger grant is applied")
	}
	if h := readHealth(t, sup); h.GrantEpochApplied != 3 {
		t.Errorf("grant_epoch_applied = %d, want 3", h.GrantEpochApplied)
	}
}

// TestAFailedApplyIsNotAcknowledged is the fail-closed direction. A ceiling the
// worker could not write is a ceiling it is not enforcing, and telling the
// allocator otherwise would let it hand the difference to a sibling.
func TestAFailedApplyIsNotAcknowledged(t *testing.T) {
	vol := healthyVolume()
	vol.grantErr = errors.New("metadata is read-only")
	cp := &fakeCP{grant: GrantSpec{Bytes: 10 << 30, Inodes: 1000000, Epoch: 2, AckedEpoch: 1}}
	sup := newSup(t, testSpec(), &fakeFS{vol: vol}, cp, &fakeReplicator{}, &fakeFencer{})

	stop := make(chan os.Signal, 1)
	done := make(chan *Fatal, 1)
	go func() { done <- sup.Run(context.Background(), stop) }()
	t.Cleanup(func() { stop <- syscall.SIGTERM; <-done })

	waitFor(t, 10*time.Second, func() bool { return len(cp.renewRequests()) >= 4 }, "timed out waiting for four renews")
	for i, r := range cp.renewRequests() {
		if r.AckedGrantEpoch != 0 {
			t.Fatalf("renew %d acknowledged epoch %d although every apply failed", i, r.AckedGrantEpoch)
		}
	}
	// And it keeps trying: the ceiling the control-plane issued is not
	// enforced until one of these succeeds.
	applies := 0
	for _, c := range vol.order() {
		if c == "apply_grant" {
			applies++
		}
	}
	if applies < 2 {
		t.Errorf("apply was attempted %d times; a failed apply must be retried on the next renew", applies)
	}
	if h := readHealth(t, sup); h.GrantEpochApplied != 0 {
		t.Errorf("grant_epoch_applied = %d after only failed applies, want 0", h.GrantEpochApplied)
	}
}

// TestAFullVolumeAtTheAccountBudgetReportsQuotaExhausted is the flag PLO-406
// republishes as `plori_mount_quota_exhausted`, on the condition it exists for:
// a volume that is full and an account that cannot fund one more increment.
//
// The second staging end-to-end found it false with `dd` answering ENOSPC and
// `df` at 100 %, so the alert could not fire on the one state it is meant to
// name. Both halves are asserted here, in order — an account at its budget with
// nothing full is NOT exhausted, and the same account with a full volume is.
func TestAFullVolumeAtTheAccountBudgetReportsQuotaExhausted(t *testing.T) {
	vol := healthyVolume()
	spec := testSpec()
	spec.Grant = GrantSpec{Bytes: 64 << 20, Inodes: 16384, Epoch: 2, AckedEpoch: 2}
	cp := &fakeCP{grant: spec.Grant, overBudget: true}
	sup := newSup(t, spec, &fakeFS{vol: vol}, cp, &fakeReplicator{}, &fakeFencer{})

	stop := make(chan os.Signal, 1)
	done := make(chan *Fatal, 1)
	go func() { done <- sup.Run(context.Background(), stop) }()
	t.Cleanup(func() { stop <- syscall.SIGTERM; <-done })

	// An account at its budget, on its own, is a normal state: nothing here is
	// full, so nothing is stuck.
	waitFor(t, 10*time.Second, func() bool {
		_, ok := healthWhenWritten(sup)
		return ok && len(cp.renewRequests()) >= 2
	}, "timed out waiting for two renews")
	if h := readHealth(t, sup); h.QuotaExhausted {
		t.Error("quota_exhausted is set on an account at its budget whose volume has refused nothing")
	}

	// Now the filesystem fills up and the ceiling starts refusing writes.
	vol.quotaTrips.Add(1)
	waitFor(t, 10*time.Second, func() bool {
		h, ok := healthWhenWritten(sup)
		return ok && h.QuotaExhausted
	}, "health.json never reported quota_exhausted for a full volume on an account at its budget")
}

// TestAGrantThatIsNotLargerIsNotAWayOut is the mechanism behind the staging
// finding, isolated.
//
// The allocator caps a grow at `current + available`, so an account with
// nothing left answers the request with the ceiling the volume already had —
// under a NEW epoch, because it re-issued the grant. Every renew therefore
// carried what looked like a fresh grant, and clearing the exhausted state on
// "a newer epoch" wiped the flag as fast as the refusals set it.
//
// The epoch is not the answer; the numbers are. And because a same-size answer
// is not an answer, the request has to be asked again — the way out of a full
// account is the user buying disk, and nothing else will ask on their behalf.
func TestAGrantThatIsNotLargerIsNotAWayOut(t *testing.T) {
	vol := healthyVolume()
	spec := testSpec()
	spec.Grant = GrantSpec{Bytes: 64 << 20, Inodes: 16384, Epoch: 2, AckedEpoch: 2}
	// An allocator with nothing to give: every grow is answered with the same
	// ceiling under the next epoch.
	cp := &fakeCP{grant: spec.Grant, onGrow: func(g GrantSpec) GrantSpec {
		g.Epoch++
		return g
	}}
	sup := newSup(t, spec, &fakeFS{vol: vol}, cp, &fakeReplicator{}, &fakeFencer{})

	stop := make(chan os.Signal, 1)
	done := make(chan *Fatal, 1)
	go func() { done <- sup.Run(context.Background(), stop) }()
	t.Cleanup(func() { stop <- syscall.SIGTERM; <-done })

	waitFor(t, 10*time.Second, func() bool { return len(cp.renewRequests()) > 0 },
		"timed out waiting for the first renew")
	vol.quotaTrips.Add(1)
	waitFor(t, 10*time.Second, func() bool {
		h, ok := healthWhenWritten(sup)
		return ok && h.QuotaExhausted
	}, "health.json never reported quota_exhausted while the allocator kept re-issuing the same ceiling")

	// And it stays reported across the epochs that keep arriving.
	before := len(cp.renewRequests())
	waitFor(t, 10*time.Second, func() bool { return len(cp.renewRequests()) >= before+4 },
		"timed out waiting for four more renews")
	h := readHealth(t, sup)
	if !h.QuotaExhausted {
		t.Error("quota_exhausted cleared on a re-issued ceiling that gave the volume no more room")
	}
	if h.GrantEpochApplied < 3 {
		t.Errorf("grant_epoch_applied = %d; the fixture must have applied at least one re-issued epoch", h.GrantEpochApplied)
	}
	if got := countGrows(cp.renewRequests()); got < 2 {
		t.Errorf("%d grow requests; a re-issued ceiling must leave the request outstanding, not satisfied", got)
	}

	// Buying disk is the way out, and it is the only thing that clears it.
	cp.mu.Lock()
	cp.onGrow = nil
	// Next epoch after whatever the re-issuing allocator has reached by now —
	// it moved the epoch on every grow, and reading it back under the same lock
	// RenewLease takes is the only way to be sure this one is newer.
	cp.grant = GrantSpec{Bytes: 256 << 20, Inodes: 65536, Epoch: cp.grant.Epoch + 1, AckedEpoch: 2}
	cp.mu.Unlock()
	waitFor(t, 10*time.Second, func() bool {
		h, ok := healthWhenWritten(sup)
		return ok && !h.QuotaExhausted
	}, "quota_exhausted never cleared after a genuinely larger ceiling was applied")
}
