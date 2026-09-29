//go:build plori
// +build plori

package mount

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// watchedReplicator is a fakeReplicator that can also be asked whether it is
// still replicating, and can be made to stop being able to answer.
type watchedReplicator struct {
	fakeReplicator

	mu         sync.Mutex
	probeErr   error
	probes     int
	restarts   int
	restartErr error
	// healAfterRestart makes Restart clear the failure, which is the
	// difference between a replicator that comes back and one that does not.
	healAfterRestart bool
	// afterRestart, when set, becomes the probe result after a successful
	// Restart: ErrReplicatorStarting models a replacement that has not opened
	// its control socket yet.
	afterRestart error
}

func (r *watchedReplicator) Probe(context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.probes++
	return r.probeErr
}

func (r *watchedReplicator) Restart(context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.restarts++
	if r.restartErr != nil {
		return r.restartErr
	}
	if r.healAfterRestart {
		r.probeErr = nil
	}
	if r.afterRestart != nil {
		r.probeErr = r.afterRestart
	}
	return nil
}

func (r *watchedReplicator) fail(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.probeErr = err
}

func (r *watchedReplicator) counts() (probes, restarts int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.probes, r.restarts
}

func (r *watchedReplicator) RestartCount() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return uint64(r.restarts)
}

// slowProbeReplicator waits for its recovery context to expire. It models a
// local socket call that cannot return before the supervisor's call budget.
type slowProbeReplicator struct {
	fakeReplicator

	mu            sync.Mutex
	probeCanceled error
	restarts      int
}

func (r *slowProbeReplicator) Probe(ctx context.Context) error {
	<-ctx.Done()
	r.mu.Lock()
	r.probeCanceled = ctx.Err()
	r.mu.Unlock()
	return ctx.Err()
}

func (r *slowProbeReplicator) Restart(context.Context) error {
	r.mu.Lock()
	r.restarts++
	r.mu.Unlock()
	return nil
}

func (r *slowProbeReplicator) result() (error, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.probeCanceled, r.restarts
}

// supWithWatchedReplicator builds a supervisor far enough along to answer
// health checks: a volume, a state directory, and a clock the test moves.
func supWithWatchedReplicator(t *testing.T, rep *watchedReplicator) (*Supervisor, *fakeVolume, *time.Time) {
	t.Helper()
	vol := healthyVolume()
	sup := newSup(t, testSpec(), &fakeFS{vol: vol}, &fakeCP{}, &rep.fakeReplicator, &fakeFencer{})
	sup.Deps.Replicator = rep
	// The production barrier interval, not the 30 ms the shared harness uses.
	// The recovery window no longer derives from it (PLO-1172), and keeping the
	// real value here is what shows a failure longer than one barrier period
	// does not stop the mount.
	sup.Options.BarrierInterval = DefaultBarrierInterval
	now := time.Now().UTC()
	clock := &now
	sup.Deps.Now = func() time.Time { return *clock }
	sup.vol = vol
	sup.deadline = NewDeadline(now.Add(time.Hour), 0, time.Now())
	sup.drain = NewDrainModel(DefaultDrainPerBlock)
	if err := os.MkdirAll(sup.Paths.StateDir, 0o700); err != nil {
		t.Fatalf("state dir: %v", err)
	}
	return sup, vol, clock
}

// The bug: nothing outside Stop and Abort ever read the replicator's fate, so
// a Litestream that died on its own left a mount serving writes with no
// metadata replica and a green health file. The first tick after the death has
// to say so.
func TestADeadReplicatorShowsUpInHealthOnTheNextTick(t *testing.T) {
	rep := &watchedReplicator{}
	sup, _, _ := supWithWatchedReplicator(t, rep)

	if f := sup.checkReplication(context.Background()); f != nil {
		t.Fatalf("healthy replicator produced a stop: %v", f.Err)
	}
	sup.writeHealth()
	if readHealth(t, sup).ReplicationFailed {
		t.Fatal("replication_failed was true while the replicator was healthy")
	}

	rep.fail(fmt.Errorf("litestream exited on its own: signal: killed: %w", ErrReplicatorGone))
	if f := sup.checkReplication(context.Background()); f != nil {
		t.Fatalf("the first failing tick must repair, not stop: %v", f.Err)
	}
	sup.writeHealth()
	if !readHealth(t, sup).ReplicationFailed {
		t.Fatal("replication_failed is still false one tick after the replicator died")
	}
}

// A successful repair is not repeated for one uninterrupted failure. A failed
// repair is retried by the bounded guard path until the stop trips.
func TestTheRepairIsAttemptedOncePerFailure(t *testing.T) {
	rep := &watchedReplicator{}
	sup, _, clock := supWithWatchedReplicator(t, rep)
	rep.fail(fmt.Errorf("litestream exited on its own: %w", ErrReplicatorGone))

	for i := 0; i < 3; i++ {
		if f := sup.checkReplication(context.Background()); f != nil {
			t.Fatalf("tick %d stopped early: %v", i, f.Err)
		}
		*clock = clock.Add(time.Second)
	}
	probes, restarts := rep.counts()
	if probes != 3 {
		t.Errorf("probes = %d, want one per tick", probes)
	}
	if restarts != 1 {
		t.Errorf("restarts = %d, want exactly one for one uninterrupted failure", restarts)
	}
	sup.writeHealth()
	if got := readHealth(t, sup).LitestreamRestarts; got != 1 {
		t.Errorf("litestream restarts = %d, want 1", got)
	}
}

// A replicator that comes back is the common case — the child was OOM-killed,
// or the node daemon was rolled — and it must clear the flag and re-arm the
// repair, so the next failure is repaired too.
func TestARecoveredReplicatorClearsTheFlagAndRearmsTheRepair(t *testing.T) {
	rep := &watchedReplicator{healAfterRestart: true}
	sup, _, clock := supWithWatchedReplicator(t, rep)

	rep.fail(fmt.Errorf("litestream exited on its own: %w", ErrReplicatorGone))
	if f := sup.checkReplication(context.Background()); f != nil {
		t.Fatalf("unexpected stop: %v", f.Err)
	}
	*clock = clock.Add(time.Second)
	if f := sup.checkReplication(context.Background()); f != nil {
		t.Fatalf("unexpected stop after the repair: %v", f.Err)
	}
	sup.writeHealth()
	if readHealth(t, sup).ReplicationFailed {
		t.Fatal("replication_failed stayed true after the replicator came back")
	}

	// Second, independent failure: repaired again.
	rep.fail(fmt.Errorf("litestream exited on its own again: %w", ErrReplicatorGone))
	*clock = clock.Add(time.Second)
	if f := sup.checkReplication(context.Background()); f != nil {
		t.Fatalf("unexpected stop on the second failure: %v", f.Err)
	}
	if _, restarts := rep.counts(); restarts != 2 {
		t.Fatalf("restarts = %d, want one per failure", restarts)
	}
}

// The point of the whole issue: replication must never be silently off. Past
// the recovery window with no replica, the mount stops — ORDERED, so the
// barrier and the final sync still run — and reports the loss with its own
// identifier.
func TestReplicationThatStaysDeadStopsTheMountWithItsOwnCode(t *testing.T) {
	rep := &watchedReplicator{restartErr: errors.New("still gone")}
	sup, vol, clock := supWithWatchedReplicator(t, rep)
	rep.fail(fmt.Errorf("litestream exited on its own: signal: killed: %w", ErrReplicatorGone))

	if f := sup.checkReplication(context.Background()); f != nil {
		t.Fatalf("stopped before the recovery window had passed: %v", f.Err)
	}
	*clock = clock.Add(ReplicationRecoveryWindow + time.Second)

	f := sup.checkReplication(context.Background())
	if f == nil {
		t.Fatal("replication has been off for longer than the recovery window and the mount is still running")
	}
	if f.Exit != CodeBarrierIncomplete {
		t.Errorf("exit = %d, want %d (the reported-data-loss class)", f.Exit, CodeBarrierIncomplete)
	}
	if f.ErrCode != ErrCodeReplicationFailed {
		t.Errorf("error code = %s, want %s", f.ErrCode, ErrCodeReplicationFailed)
	}
	if f.Retryable {
		t.Error("a replication failure that outlasted its repair is not retryable")
	}

	// Ordered, not abrupt: the last barrier and the final sync are the only
	// chance this generation has left to make its metadata durable.
	order := rep.order()
	var sawSync bool
	for _, c := range order {
		switch c {
		case "sync":
			sawSync = true
		case "abort":
			t.Fatalf("the stop aborted replication instead of syncing it: %v", order)
		}
	}
	if !sawSync {
		t.Errorf("no final sync ran during the stop: %v", order)
	}
	var sawBarrier bool
	for _, c := range vol.order() {
		if c == "barrier" {
			sawBarrier = true
		}
	}
	if !sawBarrier {
		t.Errorf("no durability barrier ran during the stop: %v", vol.order())
	}
}

// health.json is written before the stop begins, because the ordered stop can
// take the whole write-stop margin and an operator reading the file during it
// should already see why.
func TestTheVerdictIsPublishedBeforeTheStopBegins(t *testing.T) {
	rep := &watchedReplicator{restartErr: errors.New("still gone")}
	sup, _, clock := supWithWatchedReplicator(t, rep)
	rep.fail(fmt.Errorf("litestream exited on its own: %w", ErrReplicatorGone))
	_ = sup.checkReplication(context.Background())
	*clock = clock.Add(ReplicationRecoveryWindow + time.Second)
	_ = sup.checkReplication(context.Background())

	if !readHealth(t, sup).ReplicationFailed {
		t.Fatal("health.json does not record the replication failure the mount stopped for")
	}
}

// A replicator with no opinion about its own liveness — every fake in the
// older tests — must not be treated as failed. The check is opt-in by
// interface, which is what keeps this from turning every existing test into a
// stopping mount.
func TestAReplicatorThatCannotBeProbedIsNotTreatedAsFailed(t *testing.T) {
	rep := &fakeReplicator{}
	sup := newSup(t, testSpec(), &fakeFS{vol: healthyVolume()}, &fakeCP{}, rep, &fakeFencer{})
	if f := sup.checkReplication(context.Background()); f != nil {
		t.Fatalf("a replicator that does not implement the probe produced a stop: %v", f.Err)
	}
}

// A node-replicator replacement can make the first repair race a missing socket.
// That failed call must not consume recovery: the guard tick gets another bounded
// chance to register with the fresh, empty daemon before the recovery window ends.
func TestFailedReplicationRepairRetriesBeforeRecoveryDeadline(t *testing.T) {
	rep := &watchedReplicator{restartErr: errors.New("replicator socket unavailable")}
	sup, _, clock := supWithWatchedReplicator(t, rep)
	rep.fail(fmt.Errorf("litestream control /sync: status 404: database not found: %w", ErrReplicatorGone))

	if f := sup.checkReplication(context.Background()); f != nil {
		t.Fatalf("first failed repair stopped the worker: %v", f.Err)
	}
	if _, restarts := rep.counts(); restarts != 1 {
		t.Fatalf("restarts after unavailable socket = %d, want 1", restarts)
	}

	// The replacement daemon is now up with no registration for this worker.
	*clock = clock.Add(time.Second)
	rep.mu.Lock()
	rep.restartErr = nil
	rep.healAfterRestart = true
	rep.mu.Unlock()
	if f := sup.checkReplication(context.Background()); f != nil {
		t.Fatalf("re-registration before the barrier deadline stopped the worker: %v", f.Err)
	}
	if _, restarts := rep.counts(); restarts != 2 {
		t.Fatalf("restarts after fresh daemon appeared = %d, want 2", restarts)
	}

	*clock = clock.Add(time.Second)
	if f := sup.checkReplication(context.Background()); f != nil {
		t.Fatalf("probe after re-registration stopped the worker: %v", f.Err)
	}
	if !sup.replicationFailureSince().IsZero() {
		t.Fatal("successful probe after re-registration left replication failed")
	}
}

// A timed-out Probe consumes the lease stop budget. The supervisor must read
// time again before attempting Restart; using the time from before Probe would
// start a second network call after authority has expired.
func TestAProbeThatConsumesTheLeaseBudgetDoesNotStartRestart(t *testing.T) {
	rep := &slowProbeReplicator{}
	sup, _, _ := supWithWatchedReplicator(t, &watchedReplicator{})
	sup.Deps.Replicator = rep
	now := time.Now()
	sup.Deps.Now = time.Now
	sup.deadline = NewDeadline(now.UTC().Add(40*time.Millisecond), 0, now)

	f := sup.checkReplication(context.Background())
	probeErr, restarts := rep.result()
	if !errors.Is(probeErr, context.DeadlineExceeded) {
		t.Fatalf("probe context error = %v, want deadline exceeded", probeErr)
	}
	if restarts != 0 {
		t.Fatalf("restarts after exhausted lease stop budget = %d, want 0", restarts)
	}
	if f == nil {
		t.Fatal("probe that exhausted the lease stop budget did not stop")
	}
}

// PLO-1172 (incident B). A probe that times out against a child that is still
// alive is not a reason to kill it: under memory pressure `/sync` can take
// longer than ProbeTimeout, and the SIGKILL the old code sent on the first such
// timeout threw away the child's shutdown sync and left a socket-less
// replacement. Short runs of timeouts are waited out, and the recovery is
// logged when the probe answers again.
func TestProbeTimeoutsOnALiveChildWithinTheWindowDoNotRestart(t *testing.T) {
	rep := &watchedReplicator{}
	sup, _, clock := supWithWatchedReplicator(t, rep)
	log := &capturedLog{}
	sup.Deps.Log = log.fn
	if f := sup.checkReplication(context.Background()); f != nil {
		t.Fatalf("idle replicator stopped the mount: %v", f.Err)
	}
	if !sup.replicationFailureSince().IsZero() {
		t.Fatal("idle replicator started a recovery window")
	}
	firstFailure := *clock
	rep.fail(fmt.Errorf("litestream control /sync: %w", context.DeadlineExceeded))

	for i := 0; i < 2; i++ {
		if f := sup.checkReplication(context.Background()); f != nil {
			t.Fatalf("timed-out probe %d stopped the mount: %v", i, f.Err)
		}
		if got := sup.replicationFailureSince(); !got.Equal(firstFailure) {
			t.Fatalf("recovery window began at %s, want first failed probe at %s", got, firstFailure)
		}
		*clock = clock.Add(6 * time.Second)
	}
	rep.fail(nil)
	if f := sup.checkReplication(context.Background()); f != nil {
		t.Fatalf("a probe that answered stopped the mount: %v", f.Err)
	}
	if got := rep.RestartCount(); got != 0 {
		t.Fatalf("RestartCount() = %d, want 0: a live child whose probe timed out must be kept", got)
	}
	if !strings.Contains(log.all(), "replication_recovered") {
		t.Fatalf("no replication_recovered event after the probe answered again:\n%s", log.all())
	}
	if !sup.replicationFailureSince().IsZero() {
		t.Fatal("replication is still marked failed after a successful probe")
	}
}

// A running replicator that keeps failing its probe is restarted, but only on
// the ReplicationProbeFailuresBeforeRestart-th failure in a row, and only once
// for that uninterrupted failure.
func TestConsecutiveProbeFailuresRestartARunningReplicatorOnce(t *testing.T) {
	rep := &watchedReplicator{}
	sup, _, clock := supWithWatchedReplicator(t, rep)
	log := &capturedLog{}
	sup.Deps.Log = log.fn
	rep.fail(fmt.Errorf("litestream control /sync: %w", context.DeadlineExceeded))

	for i := 1; i <= ReplicationProbeFailuresBeforeRestart+2; i++ {
		if f := sup.checkReplication(context.Background()); f != nil {
			t.Fatalf("probe failure %d stopped the mount inside the window: %v", i, f.Err)
		}
		want := 0
		if i >= ReplicationProbeFailuresBeforeRestart {
			want = 1
		}
		if _, restarts := rep.counts(); restarts != want {
			t.Fatalf("after %d consecutive failures restarts = %d, want %d", i, restarts, want)
		}
		*clock = clock.Add(5 * time.Second)
	}
	if !strings.Contains(log.all(), "replication_restarted reason probe_failures") {
		t.Fatalf("restart was not logged with its reason:\n%s", log.all())
	}
}

// A replicator that exited is restarted on the first probe that sees it, and
// the replacement is left alone while it starts: no second restart, no stop,
// and the recovery is reported once it answers.
func TestAnExitedReplicatorIsRestartedOnceAndLeftAloneWhileItStarts(t *testing.T) {
	rep := &watchedReplicator{afterRestart: fmt.Errorf("litestream has not opened its control socket yet: %w", ErrReplicatorStarting)}
	sup, _, clock := supWithWatchedReplicator(t, rep)
	log := &capturedLog{}
	sup.Deps.Log = log.fn
	rep.fail(fmt.Errorf("litestream exited on its own: signal: killed: %w", ErrReplicatorGone))

	if f := sup.checkReplication(context.Background()); f != nil {
		t.Fatalf("an exited replicator stopped the mount instead of being restarted: %v", f.Err)
	}
	if got := rep.RestartCount(); got != 1 {
		t.Fatalf("RestartCount() after the exit = %d, want 1", got)
	}
	// The replacement takes most of the window to open its socket. One probe a
	// second, as the guard runs them while replication is failed.
	for i := 0; i < 20; i++ {
		*clock = clock.Add(time.Second)
		if f := sup.checkReplication(context.Background()); f != nil {
			t.Fatalf("stopped while the replacement was starting (%d s): %v", i+1, f.Err)
		}
	}
	if got := rep.RestartCount(); got != 1 {
		t.Fatalf("RestartCount() while the replacement started = %d, want 1: a starting child must not be killed", got)
	}
	rep.fail(nil)
	*clock = clock.Add(time.Second)
	if f := sup.checkReplication(context.Background()); f != nil {
		t.Fatalf("a replacement that answered stopped the mount: %v", f.Err)
	}
	if !strings.Contains(log.all(), "replication_restarted reason gone") ||
		!strings.Contains(log.all(), "replication_recovered") {
		t.Fatalf("missing replication_restarted/replication_recovered events:\n%s", log.all())
	}
}

// The window is ReplicationRecoveryWindow, not the barrier interval: a failure
// just inside it keeps the mount, one just past it takes the replication stop.
func TestReplicationFailureStopsOnlyPastTheRecoveryWindow(t *testing.T) {
	rep := &watchedReplicator{}
	sup, _, clock := supWithWatchedReplicator(t, rep)
	rep.fail(errors.New("litestream control /sync: status 500"))
	start := *clock

	if f := sup.checkReplication(context.Background()); f != nil {
		t.Fatalf("first failure stopped the mount: %v", f.Err)
	}
	*clock = start.Add(ReplicationRecoveryWindow - time.Second)
	if f := sup.checkReplication(context.Background()); f != nil {
		t.Fatalf("stopped %s into a %s window: %v", ReplicationRecoveryWindow-time.Second, ReplicationRecoveryWindow, f.Err)
	}
	*clock = start.Add(ReplicationRecoveryWindow)
	f := sup.checkReplication(context.Background())
	if f == nil {
		t.Fatal("replication failed for the whole recovery window and the mount is still running")
	}
	if f.ErrCode != ErrCodeReplicationFailed {
		t.Fatalf("error code = %s, want %s", f.ErrCode, ErrCodeReplicationFailed)
	}
}

// The lease stop instant still caps the window: a failure that is well inside
// ReplicationRecoveryWindow stops once the ordered lease stop is due, and no
// probe or restart runs past it.
func TestReplicationRecoveryNeverRunsPastTheLeaseStopInstant(t *testing.T) {
	rep := &watchedReplicator{}
	sup, _, clock := supWithWatchedReplicator(t, rep)
	sup.deadline = NewDeadline(clock.Add(10*time.Second), 0, time.Now())
	rep.fail(fmt.Errorf("litestream exited on its own: %w", ErrReplicatorGone))
	rep.restartErr = errors.New("spawn failed")

	if f := sup.checkReplication(context.Background()); f != nil {
		t.Fatalf("first failure stopped the mount: %v", f.Err)
	}
	*clock = clock.Add(11 * time.Second)
	probesBefore, restartsBefore := rep.counts()
	f := sup.checkReplication(context.Background())
	if f == nil {
		t.Fatal("replication recovery continued past the lease stop instant")
	}
	if f.ErrCode != ErrCodeReplicationFailed {
		t.Fatalf("error code = %s, want %s", f.ErrCode, ErrCodeReplicationFailed)
	}
	if probes, restarts := rep.counts(); probes != probesBefore || restarts != restartsBefore {
		t.Fatalf("probe/restart ran past the lease stop instant: probes %d->%d, restarts %d->%d",
			probesBefore, probes, restartsBefore, restarts)
	}
}

// litestreamChildReplicator is the fake replicator lifecycle with a real
// Litestream child behind Probe and Restart, so the run loop drives the same
// process handling production does.
type litestreamChildReplicator struct {
	fakeReplicator
	ls *Litestream
}

func (r *litestreamChildReplicator) Probe(ctx context.Context) error   { return r.ls.Probe(ctx) }
func (r *litestreamChildReplicator) Restart(ctx context.Context) error { return r.ls.Restart(ctx) }
func (r *litestreamChildReplicator) RestartCount() uint64              { return r.ls.RestartCount() }

// PLO-1172 item 3. Restart used to wait up to 30 s for the replacement's
// control socket on the supervisor's goroutine, which is the goroutine that
// renews the lease and writes health.json (the PLO-913 stall, reached through
// the replicator instead of the barrier). The replacement here opens its
// socket 3 s after it is spawned; renewals must keep arriving the whole time.
// Renewals stand for every branch of the run loop's select, as in
// TestAPeriodicBarrierDoesNotStopTheRunLoop.
func TestTheRunLoopKeepsRenewingWhileAReplacementLitestreamStarts(t *testing.T) {
	child := &fakeChild{}
	ls := newFakeLitestream(t, child)
	if err := ls.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = ls.Abort(context.Background()) })
	child.set(3*time.Second, "ok")
	if err := ls.cmd.Process.Kill(); err != nil {
		t.Fatalf("kill the first child: %v", err)
	}

	rep := &litestreamChildReplicator{ls: ls}
	cp := &fakeCP{}
	sup := newSup(t, testSpec(), &fakeFS{vol: healthyVolume()}, cp, &rep.fakeReplicator, &fakeFencer{})
	sup.Deps.Replicator = rep
	events := make(chan string, 256)
	sup.Deps.Log = func(event string, kv ...any) {
		if strings.HasPrefix(event, "replication_") {
			select {
			case events <- event:
			default:
			}
		}
	}
	// Enter the loop with the failure already recorded, so the one-second
	// guard probes right away instead of the test waiting for the 10 s health
	// tick to notice the dead child.
	sup.replFailedSince = time.Now()

	stop := make(chan os.Signal, 1)
	done := make(chan *Fatal, 1)
	go func() { done <- sup.Run(context.Background(), stop) }()

	var restarted, recovered bool
	var maxGap time.Duration
	lastCount, lastChange := 0, time.Now()
	deadline := time.Now().Add(20 * time.Second)
	for !recovered {
		select {
		case e := <-events:
			switch e {
			case "replication_restarted":
				restarted = true
			case "replication_recovered":
				recovered = true
			case "replication_failed_stop", "replication_restart_failed":
				t.Fatalf("unexpected %s while the replacement started", e)
			}
		case got := <-done:
			t.Fatalf("the supervisor exited during the restart: exit %d (%v)", got.Exit, got.Err)
		case <-time.After(10 * time.Millisecond):
		}
		if n := len(cp.renewRequests()); n != lastCount {
			if gap := time.Since(lastChange); lastCount > 0 && gap > maxGap {
				maxGap = gap
			}
			lastCount, lastChange = n, time.Now()
		}
		if time.Now().After(deadline) {
			stop <- syscall.SIGTERM
			t.Fatalf("no replication_recovered within 20 s (restarted=%v)", restarted)
		}
	}
	stop <- syscall.SIGTERM
	select {
	case got := <-done:
		if got.Exit != CodeOK {
			t.Fatalf("stop exit = %d (%v), want a clean stop", got.Exit, got.Err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the stop never finished")
	}

	if !restarted {
		t.Fatal("recovered without a replication_restarted event: the dead child was not replaced")
	}
	if got := rep.RestartCount(); got != 1 {
		t.Fatalf("RestartCount() = %d, want 1: the starting replacement must not be killed and started again", got)
	}
	// The spec renews every 50 ms. A loop blocked on the replacement's socket
	// would show a gap of about 3 s.
	if maxGap > time.Second {
		t.Fatalf("longest gap between renewals was %s while the replacement started, want under 1 s", maxGap)
	}
}

// health.json says when it was taken and when replication was last actually
// checked, so a reader can tell a fresh verdict from one a slow probe has left
// standing. A failed probe is still a check: it is what set the verdict.
func TestHealthStampsTheObservationAndTheLastReplicationCheck(t *testing.T) {
	rep := &watchedReplicator{}
	sup, _, clock := supWithWatchedReplicator(t, rep)

	sup.writeHealth()
	h := readHealth(t, sup)
	if !h.ObservedAt.Equal(*clock) {
		t.Errorf("observed_at = %s, want the snapshot instant %s", h.ObservedAt, *clock)
	}
	if !h.ReplicationCheckedAt.IsZero() {
		t.Errorf("replication_checked_at = %s before any probe returned, want zero", h.ReplicationCheckedAt)
	}

	*clock = clock.Add(time.Second)
	if f := sup.checkReplication(context.Background()); f != nil {
		t.Fatalf("healthy replicator produced a stop: %v", f.Err)
	}
	checked := *clock
	*clock = clock.Add(3 * time.Second)
	sup.writeHealth()
	h = readHealth(t, sup)
	if !h.ReplicationCheckedAt.Equal(checked) {
		t.Errorf("replication_checked_at = %s, want the probe's return %s", h.ReplicationCheckedAt, checked)
	}
	if !h.ObservedAt.Equal(*clock) {
		t.Errorf("observed_at = %s, want %s: a snapshot is taken whether or not a probe ran since", h.ObservedAt, *clock)
	}

	rep.fail(fmt.Errorf("litestream exited on its own: signal: killed: %w", ErrReplicatorGone))
	*clock = clock.Add(time.Second)
	if f := sup.checkReplication(context.Background()); f != nil {
		t.Fatalf("the first failing probe must repair, not stop: %v", f.Err)
	}
	failed := *clock
	sup.writeHealth()
	h = readHealth(t, sup)
	if !h.ReplicationFailed || !h.ReplicationCheckedAt.Equal(failed) {
		t.Errorf("replication_failed = %t checked_at = %s, want true at the failing probe %s",
			h.ReplicationFailed, h.ReplicationCheckedAt, failed)
	}
}
