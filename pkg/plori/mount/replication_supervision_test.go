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

// supWithWatchedReplicator builds a supervisor far enough along to answer
// health checks: a volume, a state directory, and a clock the test moves.
func supWithWatchedReplicator(t *testing.T, rep *watchedReplicator) (*Supervisor, *fakeVolume, *time.Time) {
	t.Helper()
	vol := healthyVolume()
	sup := newSup(t, testSpec(), &fakeFS{vol: vol}, &fakeCP{}, &rep.fakeReplicator, &fakeFencer{})
	sup.Deps.Replicator = rep
	// A real barrier interval, not the 30 ms the shared harness uses: the whole
	// rule under test is "failed for longer than a barrier period", and a
	// period shorter than one tick of the test clock makes every failure
	// terminal on its first tick.
	sup.Options.BarrierInterval = time.Minute
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

	rep.fail(errors.New("litestream exited on its own: signal: killed"))
	if f := sup.checkReplication(context.Background()); f != nil {
		t.Fatalf("the first failing tick must repair, not stop: %v", f.Err)
	}
	sup.writeHealth()
	if !readHealth(t, sup).ReplicationFailed {
		t.Fatal("replication_failed is still false one tick after the replicator died")
	}
}

// One repair attempt per failure, not one per tick. A replicator that cannot
// be revived would otherwise be restarted every ten seconds until the stop
// trips, and each attempt on the exec path is a process spawn.
func TestTheRepairIsAttemptedOncePerFailure(t *testing.T) {
	rep := &watchedReplicator{}
	sup, _, clock := supWithWatchedReplicator(t, rep)
	rep.fail(errors.New("gone"))

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
}

// A replicator that comes back is the common case — the child was OOM-killed,
// or the node daemon was rolled — and it must clear the flag and re-arm the
// repair, so the next failure is repaired too.
func TestARecoveredReplicatorClearsTheFlagAndRearmsTheRepair(t *testing.T) {
	rep := &watchedReplicator{healAfterRestart: true}
	sup, _, clock := supWithWatchedReplicator(t, rep)

	rep.fail(errors.New("gone"))
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
	rep.fail(errors.New("gone again"))
	*clock = clock.Add(time.Second)
	if f := sup.checkReplication(context.Background()); f != nil {
		t.Fatalf("unexpected stop on the second failure: %v", f.Err)
	}
	if _, restarts := rep.counts(); restarts != 2 {
		t.Fatalf("restarts = %d, want one per failure", restarts)
	}
}

// The point of the whole issue: replication must never be silently off. Past a
// barrier period with no replica, the mount stops — ORDERED, so the barrier and
// the final sync still run — and reports the loss with its own identifier.
func TestReplicationThatStaysDeadStopsTheMountWithItsOwnCode(t *testing.T) {
	rep := &watchedReplicator{restartErr: errors.New("still gone")}
	sup, vol, clock := supWithWatchedReplicator(t, rep)
	rep.fail(errors.New("litestream exited on its own: signal: killed"))

	if f := sup.checkReplication(context.Background()); f != nil {
		t.Fatalf("stopped before a barrier period had passed: %v", f.Err)
	}
	*clock = clock.Add(sup.barrierInterval() + time.Second)

	f := sup.checkReplication(context.Background())
	if f == nil {
		t.Fatal("replication has been off for longer than a barrier period and the mount is still running")
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
	rep.fail(errors.New("gone"))
	_ = sup.checkReplication(context.Background())
	*clock = clock.Add(sup.barrierInterval() + time.Second)
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
