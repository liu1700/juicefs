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
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
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

type joinedAdmissionContext struct {
	context.Context
	joined chan struct{}
	once   sync.Once
}

func (c *joinedAdmissionContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.joined) })
	return c.Context.Done()
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

	if got := countGrows(cp.renewRequests()); got < 2 {
		t.Errorf("%d grow requests after transient unchanged answers, want retries", got)
	}
	if h := readHealth(t, sup); h.QuotaExhausted {
		t.Error("unchanged grant without over_budget must not report account exhaustion")
	}
}

func TestQuotaFlightKeepsItsDenialWhenANewFlightStarts(t *testing.T) {
	sup := newSup(t, testSpec(), &fakeFS{vol: healthyVolume()}, &fakeCP{}, &fakeReplicator{}, &fakeFencer{})
	sup.admissionRenew = make(chan struct{}, 1)
	old := make(chan syscall.Errno, 1)
	go func() { old <- sup.Admit(context.Background()) }()
	<-sup.admissionRenew
	sup.mu.Lock()
	sup.finishQuotaFlightLocked(syscall.ENOSPC)
	sup.mu.Unlock()
	// A new blocked operation may immediately probe after a purchase; it must
	// not overwrite the result held by the old flight's done channel.
	newDone := make(chan syscall.Errno, 1)
	go func() { newDone <- sup.Admit(context.Background()) }()
	<-sup.admissionRenew
	if got := <-old; got != syscall.ENOSPC {
		t.Fatalf("old flight = %s", got)
	}
	sup.mu.Lock()
	sup.finishQuotaFlightLocked(0)
	sup.mu.Unlock()
	if got := <-newDone; got != 0 {
		t.Fatalf("new flight = %s", got)
	}
}

func TestQuotaFlightConcurrentWaitersShareOneRenew(t *testing.T) {
	sup := newSup(t, testSpec(), &fakeFS{vol: healthyVolume()}, &fakeCP{}, &fakeReplicator{}, &fakeFencer{})
	sup.admissionRenew = make(chan struct{}, 1)
	results := make(chan syscall.Errno, 2)
	// Admit evaluates Done only after it has released sup.mu and captured the
	// shared flight. These notifications therefore prove both waiters joined
	// before the test completes the flight.
	newJoinedContext := func() *joinedAdmissionContext {
		return &joinedAdmissionContext{Context: context.Background(), joined: make(chan struct{})}
	}
	first, second := newJoinedContext(), newJoinedContext()
	go func() { results <- sup.Admit(first) }()
	go func() { results <- sup.Admit(second) }()
	<-sup.admissionRenew
	<-first.joined
	<-second.joined
	select {
	case <-sup.admissionRenew:
		t.Fatal("two waiters queued two renews")
	default:
	}
	sup.mu.Lock()
	sup.finishQuotaFlightLocked(0)
	sup.mu.Unlock()
	if <-results != 0 || <-results != 0 {
		t.Fatal("shared flight did not admit both waiters")
	}
}

// TestProactiveQuotaGrowthUsesTheSameCoalescedRenew shows the 80%-usage hook
// does not wait for an ENOSPC and does not create a separate control-plane
// path. Several committed operations can signal it, but one grant generation
// produces one renewal carrying Grow.
func TestProactiveQuotaGrowthUsesTheSameCoalescedRenew(t *testing.T) {
	vol := healthyVolume()
	spec := testSpec()
	// The immediate admission/proactive signal must bypass this normal lease
	// cadence; a short test interval cannot prove that property.
	spec.LeaseRenewInterval = Duration(20 * time.Second)
	spec.Grant = GrantSpec{Bytes: 256 << 20, Inodes: 16384, Epoch: 2, AckedEpoch: 2}
	cp := &fakeCP{grant: spec.Grant, onGrow: func(g GrantSpec) GrantSpec {
		g.Bytes += 64 << 20
		g.Epoch++
		return g
	}}
	sup := newSup(t, spec, &fakeFS{vol: vol}, cp, &fakeReplicator{}, &fakeFencer{})
	stop := make(chan os.Signal, 1)
	done := make(chan *Fatal, 1)
	go func() { done <- sup.Run(context.Background(), stop) }()
	t.Cleanup(func() { stop <- syscall.SIGTERM; <-done })
	waitFor(t, 10*time.Second, func() bool {
		sup.mu.Lock()
		mounted := sup.mounted
		sup.mu.Unlock()
		return mounted
	}, "timed out waiting for the mount to become ready")
	started := time.Now()
	for range 100 {
		sup.Proactive()
	}
	waitFor(t, 10*time.Second, func() bool { return countGrows(cp.renewRequests()) > 0 }, "timed out waiting for proactive grow")
	elapsed := time.Since(started)
	t.Logf("proactive quota growth completed its renewal request in %s with a normal interval of %s", elapsed, spec.LeaseRenewInterval.D())
	if elapsed >= time.Second {
		t.Fatalf("proactive quota grow took %s; it must bypass the 20-second lease interval", elapsed)
	}
	before := countGrows(cp.renewRequests())
	waitFor(t, 10*time.Second, func() bool { return len(vol.appliedGrants()) >= 2 }, "timed out applying proactive grant")
	if got := countGrows(cp.renewRequests()); got != before {
		t.Errorf("coalesced proactive signals produced %d grow renews, want %d", got, before)
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
	cp := &fakeCP{grant: spec.Grant, overBudget: true, onGrow: func(g GrantSpec) GrantSpec {
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
	cp.overBudget = false
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

// Mirrors the real FUSE context: Err is EINTR even while live, Done is nil,
// and Canceled is the cancellation authority.
type fuseAdmissionContext struct {
	context.Context
	stopped atomic.Bool
}

func (c *fuseAdmissionContext) Err() error     { return syscall.EINTR }
func (c *fuseAdmissionContext) Canceled() bool { return c.stopped.Load() }

func TestQuotaAdmissionUsesFUSECancellationContract(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel=%t", cancel), func(t *testing.T) {
			// The admission bound (PLO-873) is AdmissionRenewRounds renew
			// intervals. With testSpec's 50 ms interval it would end the wait at
			// 150 ms, close to the 100 ms cancellation poll. A long interval
			// keeps the bound out of this test, which is about cancellation.
			spec := testSpec()
			spec.LeaseRenewInterval = Duration(10 * time.Second)
			sup := newSup(t, spec, &fakeFS{vol: healthyVolume()}, &fakeCP{}, &fakeReplicator{}, &fakeFencer{})
			sup.admissionRenew = make(chan struct{}, 1)
			ctx := &fuseAdmissionContext{Context: context.Background()}
			done := make(chan syscall.Errno, 1)
			go func() { done <- sup.Admit(ctx) }()
			select {
			case <-sup.admissionRenew:
			case result := <-done:
				t.Fatalf("live FUSE request did not await grant: %s", result)
			case <-time.After(time.Second):
				t.Fatal("admission did not request grant")
			}
			want := syscall.Errno(0)
			if cancel {
				ctx.stopped.Store(true)
				want = syscall.EINTR
			} else {
				sup.mu.Lock()
				sup.finishQuotaFlightLocked(0)
				sup.mu.Unlock()
			}
			select {
			case got := <-done:
				if got != want {
					t.Fatalf("result=%s want=%s", got, want)
				}
			case <-time.After(time.Second):
				t.Fatal("admission did not settle")
			}
			sup.mu.Lock()
			sup.finishQuotaFlightLocked(syscall.EINTR)
			sup.mu.Unlock()
		})
	}
}

// ---------------------------------------------------- the admission bound ---
//
// PLO-873. A write refused by the volume ceiling holds a FUSE request open
// while Admit decides what to tell it, and the data commit path passes
// meta.Background() (pkg/vfs/writer.go), which is never cancelled. So the wait
// has to end on its own. On staging it did not: health.json reported
// quota_exhausted while writers stayed blocked for the life of the mount and
// the Agent's job never finished.

// admitWithin runs Admit on its own goroutine and returns its errno, failing
// the test rather than hanging when the wait never ends. On the parent commit
// every one of these waits is unbounded, so the guard is what turns "the mount
// is wedged" into a test failure with a message.
func admitWithin(t *testing.T, sup *Supervisor, limit time.Duration, what string) syscall.Errno {
	t.Helper()
	got := make(chan syscall.Errno, 1)
	go func() { got <- sup.Admit(context.Background()) }()
	select {
	case st := <-got:
		return st
	case <-time.After(limit):
		t.Fatalf("%s: the write was still parked after %s with nothing left to answer it", what, limit)
		return 0
	}
}

// TestAParkedWriteIsAnsweredWhenNoRenewEverDoes is the invariant the rest of
// this section rests on: Admit always answers.
//
// Nothing here renews, applies a grant or refuses one — the supervisor is not
// running, which is the shape of every stall the state machine can reach (an
// apply that will not persist, a dropped admission poke, a control-plane that
// answers renewals and never the request). The ceiling's own errno is the right
// answer at that point, because it is what the filesystem would have returned
// if nobody had tried to grow it.
func TestAParkedWriteIsAnsweredWhenNoRenewEverDoes(t *testing.T) {
	sup := newSup(t, testSpec(), &fakeFS{vol: healthyVolume()}, &fakeCP{}, &fakeReplicator{}, &fakeFencer{})
	sup.admissionRenew = make(chan struct{}, 1)
	bound := sup.admissionBound()

	started := time.Now()
	got := admitWithin(t, sup, 20*bound+5*time.Second, "an unanswered grow request")
	elapsed := time.Since(started)

	if got != syscall.ENOSPC {
		t.Fatalf("a write parked against the ceiling was answered %s, want ENOSPC", got)
	}
	// The bound is a wait, not a rejection: a mount whose renew is merely slow
	// must still get its grant rather than a full disk.
	if elapsed+5*time.Millisecond < bound {
		t.Errorf("the wait ended after %s, short of the %s bound", elapsed, bound)
	}
}

// TestAWriterArrivingAfterARefusalIsNotParkedAgain is the second half of the
// answer. A bound alone would make every writer behind a refused account pay it
// in turn, so a full volume would serve one ENOSPC per bound instead of one per
// write.
//
// growDenied is what the allocator's last word left behind, and it survives
// only while nothing the worker can observe has changed: applyGrant clears it
// when a larger ceiling lands, noteUsageLocked when a usage reading shows space
// coming back. While it stands, waiting cannot produce a different answer.
func TestAWriterArrivingAfterARefusalIsNotParkedAgain(t *testing.T) {
	sup := newSup(t, testSpec(), &fakeFS{vol: healthyVolume()}, &fakeCP{}, &fakeReplicator{}, &fakeFencer{})
	sup.admissionRenew = make(chan struct{}, 1)
	// What growRefused leaves behind when the account is at its budget.
	sup.mu.Lock()
	sup.ceilingRefused, sup.growDenied = true, true
	sup.mu.Unlock()

	started := time.Now()
	got := admitWithin(t, sup, 20*sup.admissionBound()+5*time.Second,
		"a writer arriving after a refusal")
	elapsed := time.Since(started)

	if got != syscall.ENOSPC {
		t.Fatalf("a writer arriving after a refusal was answered %s, want ENOSPC", got)
	}
	if elapsed >= sup.admissionBound() {
		t.Errorf("waited %s for an answer that was already known; the %s bound must not be paid again", elapsed, sup.admissionBound())
	}
	// Refusing must not also stop asking. The way out of a full account is the
	// user buying disk, and only a renew carrying Grow will notice that they
	// did (TestAGrowTheAccountCannotFundIsAskedAgain).
	select {
	case <-sup.admissionRenew:
	default:
		t.Error("the refusal did not poke the renew loop, so nothing will ask the allocator again")
	}
	if !sup.admissionPending() {
		t.Error("the refusal left no grow request outstanding")
	}
}

// TestALargerGrantAdmitsAWriterARefusalWouldHaveTurnedAway walks the whole
// episode, and is the safety argument for the rule above: the refusal is a
// statement about a state, not a verdict on the volume. A larger ceiling ends
// the state, and the next writer is treated as if the refusal had never
// happened.
func TestALargerGrantAdmitsAWriterARefusalWouldHaveTurnedAway(t *testing.T) {
	vol := healthyVolume()
	sup := newSup(t, testSpec(), &fakeFS{vol: vol}, &fakeCP{}, &fakeReplicator{}, &fakeFencer{})
	sup.vol = vol
	sup.admissionRenew = make(chan struct{}, 1)
	sup.mu.Lock()
	sup.ceilingRefused, sup.growDenied = true, true
	sup.grantBytes, sup.grantInodes = 64<<20, 16384
	sup.mu.Unlock()

	if got := admitWithin(t, sup, 20*sup.admissionBound()+5*time.Second,
		"a writer during the refusal"); got != syscall.ENOSPC {
		t.Fatalf("a writer during the refusal was answered %s, want ENOSPC", got)
	}
	<-sup.admissionRenew

	// The user buys disk and the allocator answers the outstanding request with
	// a ceiling this volume did not have.
	sup.applyGrant(context.Background(), GrantSpec{Bytes: 128 << 20, Inodes: 32768, Epoch: 3})
	sup.mu.Lock()
	refused, denied := sup.ceilingRefused, sup.growDenied
	sup.mu.Unlock()
	if refused || denied {
		t.Fatalf("a larger ceiling left ceiling_refused=%v grow_denied=%v; both describe a state that has ended", refused, denied)
	}

	// A writer arriving now is not turned away on sight: it opens a request and
	// is admitted by the next ceiling that gives it room.
	admitted := make(chan syscall.Errno, 1)
	go func() { admitted <- sup.Admit(context.Background()) }()
	waitFor(t, 5*time.Second, sup.admissionPending, "the writer never opened a grow request")
	sup.applyGrant(context.Background(), GrantSpec{Bytes: 256 << 20, Inodes: 65536, Epoch: 4})
	select {
	case got := <-admitted:
		if got != 0 {
			t.Fatalf("a writer waiting when a larger ceiling landed was answered %s, want admission", got)
		}
	case <-time.After(20*sup.admissionBound() + 5*time.Second):
		t.Fatal("a larger ceiling did not admit the writer waiting for it")
	}
}

// TestAFailedApplyDoesNotLatchTheGrowRequest is PLO-873's first half, and the
// reason the parked writers had nothing to wait for.
//
// growAsked means "an issued grant is on its way into local state, do not ask
// for another"; renewRequest attaches Grow only when it is clear, and renew
// only clears it when the answered grant IS applied. An apply that fails leaves
// neither true: nothing is on its way, and the flag was latched for the life of
// the process. From there no renew carried Grow, so no answer could carry
// OverBudget, so growRefused never ran and the flight never ended.
func TestAFailedApplyDoesNotLatchTheGrowRequest(t *testing.T) {
	vol := healthyVolume()
	vol.grantErr = errors.New("metadata is read-only")
	cp := &fakeCP{grant: GrantSpec{Bytes: 10 << 30, Inodes: 1000000, Epoch: 2, AckedEpoch: 1}}
	sup := newSup(t, testSpec(), &fakeFS{vol: vol}, cp, &fakeReplicator{}, &fakeFencer{})

	stop := make(chan os.Signal, 1)
	done := make(chan *Fatal, 1)
	go func() { done <- sup.Run(context.Background(), stop) }()
	t.Cleanup(func() { stop <- syscall.SIGTERM; <-done })

	waitFor(t, 10*time.Second, func() bool { return len(cp.renewRequests()) > 0 }, "timed out waiting for the first renew")
	vol.quotaTrips.Add(1)
	waitFor(t, 10*time.Second, func() bool { return countGrows(cp.renewRequests()) >= 3 },
		"the grow request was asked once and then never again: a failed apply latched growAsked")
}

// TestSpaceComingBackAfterARefusalClearsTheExhaustedState is the other way out
// of a full volume, and the one the worker had no way to notice.
//
// A grant that cannot grow is not the only thing that makes a write possible
// again — the user deleting files does too, and the ceiling does not move when
// they do. The only figure this process holds is the periodic usage reading
// behind used_bytes, so that is what the test is on: less in the volume than
// the refusal saw, and under the ceiling in force.
//
// plori-runtime's executor reads quota_exhausted and ends the turn with a
// disk-full message, so a flag that stays true after the volume is emptied ends
// healthy turns.
func TestSpaceComingBackAfterARefusalClearsTheExhaustedState(t *testing.T) {
	vol := healthyVolume()
	// A volume against its ceiling, which is where the refusal happens.
	vol.setUsage(Usage{Bytes: 64 << 20, Inodes: 16384}, nil)
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
	waitFor(t, 10*time.Second, func() bool {
		h, ok := healthWhenWritten(sup)
		return ok && h.QuotaExhausted
	}, "health.json never reported quota_exhausted for a full volume on an account at its budget")

	// The user deletes and empties the trash. Nothing tells the worker; the
	// next usage reading is simply smaller.
	vol.setUsage(Usage{Bytes: 8 << 20, Inodes: 64}, nil)
	waitFor(t, 10*time.Second, func() bool {
		h, ok := healthWhenWritten(sup)
		return ok && !h.QuotaExhausted
	}, "quota_exhausted stayed set after the volume emptied; nothing but a larger ceiling can clear it")

	// And it stays cleared: an account still at its budget is not, on its own,
	// an exhausted volume.
	before := len(cp.renewRequests())
	waitFor(t, 10*time.Second, func() bool { return len(cp.renewRequests()) >= before+4 },
		"timed out waiting for four more renews")
	if h := readHealth(t, sup); h.QuotaExhausted {
		t.Error("quota_exhausted came back on a volume with room, without a new refusal")
	}
}

// TestSpaceStillAboveTheCeilingDoesNotClearTheExhaustedState is the other side
// of that reading, and why it is two-sided. A volume compacted from far over
// its ceiling to just over it has had space returned and is still full: a write
// arriving now is refused exactly as before, and an operator told otherwise is
// told the Agent recovered.
func TestSpaceStillAboveTheCeilingDoesNotClearTheExhaustedState(t *testing.T) {
	vol := healthyVolume()
	vol.setUsage(Usage{Bytes: 96 << 20, Inodes: 20000}, nil)
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
	waitFor(t, 10*time.Second, func() bool {
		h, ok := healthWhenWritten(sup)
		return ok && h.QuotaExhausted
	}, "health.json never reported quota_exhausted for a full volume on an account at its budget")

	// Smaller than the refusal saw, still above the ceiling.
	vol.setUsage(Usage{Bytes: 72 << 20, Inodes: 18000}, nil)
	before := len(cp.renewRequests())
	waitFor(t, 10*time.Second, func() bool { return len(cp.renewRequests()) >= before+20 },
		"timed out waiting for twenty more renews")
	if h := readHealth(t, sup); !h.QuotaExhausted {
		t.Error("quota_exhausted cleared on a volume that is still over its ceiling")
	}
}

// TestABoundedAdmissionLeavesNoGoroutineBehind is the production symptom read
// the other way round. Every parked write is a goroutine the mount never gets
// back, and on staging they accumulated for the life of the process; the timer
// that bounds the wait must not replace them with one of its own.
func TestABoundedAdmissionLeavesNoGoroutineBehind(t *testing.T) {
	sup := newSup(t, testSpec(), &fakeFS{vol: healthyVolume()}, &fakeCP{}, &fakeReplicator{}, &fakeFencer{})
	sup.admissionRenew = make(chan struct{}, 1)

	before := settledGoroutines()
	const writers = 8
	var wg sync.WaitGroup
	for range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sup.Admit(context.Background())
		}()
	}
	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(20*sup.admissionBound() + 5*time.Second):
		t.Fatalf("%d writers were still parked; each one is a goroutine this mount never gets back", writers)
	}

	if after := settledGoroutines(); after > before {
		t.Errorf("%d goroutines before %d bounded admissions, %d after", before, writers, after)
	}
}

// settledGoroutines reads the count once it has stopped moving, so a goroutine
// that an earlier test is still winding down is not read as this test's.
func settledGoroutines() int {
	last := runtime.NumGoroutine()
	stable := 0
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		time.Sleep(20 * time.Millisecond)
		n := runtime.NumGoroutine()
		if n == last {
			if stable++; stable == 3 {
				return n
			}
			continue
		}
		last, stable = n, 0
	}
	return last
}
