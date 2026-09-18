//go:build plori
// +build plori

package mount

import (
	"context"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"
)

// blockingBarrier is a volume barrier that holds its FIRST caller until the test
// releases it, and answers every later caller at once. The first call stands in
// for the flush of a saturated writeback backlog: ploriVolume.Barrier calls the
// VFS FlushAll, which takes no context, so a real one runs for as long as the
// flush takes whatever budget the caller set.
type blockingBarrier struct {
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func newBlockingBarrier() *blockingBarrier {
	return &blockingBarrier{started: make(chan struct{}), release: make(chan struct{})}
}

func (b *blockingBarrier) run(context.Context) (BarrierResult, error) {
	first := false
	b.once.Do(func() { first = true })
	if first {
		close(b.started)
		<-b.release
	}
	return BarrierResult{BarrierAt: time.Now().UTC()}, nil
}

// PLO-913. The run loop is one select, and the periodic barrier used to run on
// its goroutine. A barrier draining a saturated backlog therefore stopped the
// lease renewal, the one-second deadline guard and the health write for as long
// as it took to flush — under a sustained write in production, 62-67 s against a
// 60 s health-staleness bound, which fenced three consecutive healthy mounts and
// left the worker not renewing the lease it was flushing under.
//
// The renewal is what this asserts because it is the fastest of the three here
// (the spec renews every 50 ms; HealthWriteInterval is a 10 s constant). They are
// branches of one select, so a loop that renews is a loop that can also write
// health.json and check its deadline — the defect was that the select itself
// could not run at all.
func TestAPeriodicBarrierDoesNotStopTheRunLoop(t *testing.T) {
	barrier := newBlockingBarrier()
	vol := healthyVolume()
	vol.barrier = barrier.run
	cp := &fakeCP{}
	sup := newSup(t, testSpec(), &fakeFS{vol: vol}, cp, &fakeReplicator{}, &fakeFencer{})

	stop := make(chan os.Signal, 1)
	done := make(chan *Fatal, 1)
	go func() { done <- sup.Run(context.Background(), stop) }()

	select {
	case <-barrier.started:
	case got := <-done:
		t.Fatalf("the supervisor exited before a barrier ran: exit %d (%v)", got.Exit, got.Err)
	case <-time.After(10 * time.Second):
		t.Fatal("no barrier ran")
	}

	// Renewals must keep arriving while that first barrier is still flushing.
	before := len(cp.renewRequests())
	deadline := time.Now().Add(10 * time.Second)
	for len(cp.renewRequests()) < before+3 {
		select {
		case got := <-done:
			t.Fatalf("the supervisor exited while the barrier was blocked: exit %d (%v)", got.Exit, got.Err)
		default:
		}
		if time.Now().After(deadline) {
			after := len(cp.renewRequests())
			close(barrier.release)
			stop <- syscall.SIGTERM
			select {
			case <-done:
			case <-time.After(10 * time.Second):
			}
			t.Fatalf("the run loop stopped renewing while a barrier was flushing: %d renewals before, %d after 10 s",
				before, after)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// The ordered stop is still exclusive with the barrier: it fences, then waits
	// for the flush in front of it rather than starting a second one on top.
	stop <- syscall.SIGTERM
	time.Sleep(20 * time.Millisecond)
	close(barrier.release)
	select {
	case got := <-done:
		if got.Exit != CodeOK {
			t.Fatalf("stop exit = %d (%v), want a clean stop behind the barrier it waited for", got.Exit, got.Err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the stop never finished: it is waiting on a barrier that has been released")
	}
}
