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
	"sync"
	"time"
)

// loopWorkers carries the run loop's slow work, so none of it can stand between
// the loop and the lease.
//
// The loop used to be one select that also EXECUTED everything it scheduled: a
// renew, a replication probe and its repair, a credential reload and a barrier,
// each on its own ticker and all run inline. Independent tickers are not
// independent execution. A barrier's `/sync -wait` against a Litestream that had
// stopped answering held the loop for as long as the lease allowed, and for all
// of it nothing renewed, the deadline guard did not run, health.json was not
// rewritten, and the probe that exists to notice that very stall could not
// start. The metadata engine still refused writes at expiry — that check is per
// operation (meta.PloriSetWriteExpiry) — but the mount only learned it had lost
// its lease once the call came back.
//
// The loop now only decides. Slow work runs here and hands back an observation;
// the loop stays the one goroutine that moves the deadline, publishes the write
// expiry, fences, writes health.json and stops the mount:
//
//   - renew: one request at a time, bounded by the ordered-stop instant.
//   - replication lane: probe, repair and credential reload, serialised because
//     all three act on the replicator's process or registration.
//   - barrier lane: the periodic barrier. It reads the replica position through
//     the control socket and nothing else of the replicator's, so it does not
//     share the replication lane: a barrier waiting on a stalled replicator must
//     not hold back the probe that detects the stall.
//   - usage: the trash walk and the /usage report.
//
// All of it is joined before a stop touches the volume or the replicator
// (Supervisor.stopWorkers), so a stop never overlaps work the loop started.
type loopWorkers struct {
	// ctx ends every call started here. repairCtx, its parent, additionally
	// ends a replicator restart or credential reload, and only a stop that
	// uploads nothing cancels it: an ordered stop waits for one in flight rather
	// than hand its final sync a half-restarted replicator.
	ctx          context.Context
	cancel       context.CancelFunc
	repairCtx    context.Context
	cancelRepair context.CancelFunc
	wg           sync.WaitGroup

	// Each channel holds one value, and each lane has at most one job in flight,
	// so no send on any of them blocks.
	renewDone       chan renewObservation
	replicationJobs chan replicationJob
	replicationDone chan replicationObservation
	barrierJobs     chan struct{}
	barrierDone     chan struct{}

	// The rest is the run loop's own: a busy lane keeps what was asked of it,
	// so ticks behind a slow call coalesce and nothing requested is dropped.
	replicating, barriering            bool
	wantProbe, wantReload, wantBarrier bool
}

// renewObservation is one renewal's answer, with what the loop needs to judge
// it: the monotonic instant the request left and the stop instant that bounded
// it.
type renewObservation struct {
	requestedGrowth bool
	before, due     time.Time
	ticks           int
	reportUsage     bool
	resp            LeaseResponse
	err             error
}

type replicationJob struct {
	probe, reload bool
}

// replicationStop is a replication failure that has outlasted its window.
type replicationStop struct {
	err       error
	at, since time.Time
}

type replicationObservation struct {
	stop *replicationStop
	// changed is true when replication_failed flipped, so health.json says so
	// now rather than on the next health tick.
	changed bool
}

// startWorkers starts the two lanes. The renewals and usage work are started on
// demand by the loop.
func (s *Supervisor) startWorkers(parent context.Context) *loopWorkers {
	repairCtx, cancelRepair := context.WithCancel(parent)
	ctx, cancel := context.WithCancel(repairCtx)
	w := &loopWorkers{
		ctx:             ctx,
		cancel:          cancel,
		repairCtx:       repairCtx,
		cancelRepair:    cancelRepair,
		renewDone:       make(chan renewObservation, 1),
		replicationJobs: make(chan replicationJob, 1),
		replicationDone: make(chan replicationObservation, 1),
		barrierJobs:     make(chan struct{}, 1),
		barrierDone:     make(chan struct{}, 1),
	}
	w.wg.Add(2)
	go func() {
		defer w.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case job := <-w.replicationJobs:
				if ctx.Err() != nil {
					return
				}
				obs := s.runReplicationJob(ctx, repairCtx, job)
				select {
				case w.replicationDone <- obs:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	go func() {
		defer w.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-w.barrierJobs:
				if ctx.Err() != nil {
					return
				}
				s.runBarrier(ctx)
				select {
				case w.barrierDone <- struct{}{}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	s.workers = w
	return w
}

// pump hands pending work to an idle lane.
func (w *loopWorkers) pump() {
	if !w.replicating && (w.wantProbe || w.wantReload) {
		w.replicationJobs <- replicationJob{probe: w.wantProbe, reload: w.wantReload}
		w.replicating, w.wantProbe, w.wantReload = true, false, false
	}
	if !w.barriering && w.wantBarrier {
		w.barrierJobs <- struct{}{}
		w.barriering, w.wantBarrier = true, false
	}
}

// stopWorkers ends everything the run loop started and waits for it to return.
// `abandon` is a stop that must upload nothing, which also interrupts a
// replicator restart or credential reload; an ordered stop lets one finish.
//
// It is called by the run loop's goroutine only, and is a no-op when no loop is
// running: before the mount is up, and in tests that drive a step directly.
func (s *Supervisor) stopWorkers(ctx context.Context, abandon bool) bool {
	w := s.workers
	if w == nil {
		return true
	}
	s.workers = nil
	s.cancelWorkers(w, abandon)
	joined := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(joined)
	}()
	select {
	case <-joined:
	case <-ctx.Done():
		// A caller that cannot join must fence and leave every owned resource
		// alone. The mount command exits the process rather than tearing down
		// underneath a live worker.
		w.cancelRepair()
		return false
	}
	w.cancelRepair()
	return true
}

// cancelWorkers makes every worker observe an out-of-band stop before its
// caller seals the volume. Joining is deliberately separate: losing an epoch
// must close the worker contexts immediately, but a worker that ignores its
// context must not retain the process past the shutdown budget.
func (s *Supervisor) cancelWorkers(w *loopWorkers, abandon bool) {
	w.cancel()
	if abandon {
		w.cancelRepair()
	}
}

// spawn runs fn as one of the run loop's workers, so the stop joins it. With no
// loop running it is an ordinary goroutine.
func (s *Supervisor) spawn(fn func()) {
	w := s.workers
	if w == nil {
		go fn()
		return
	}
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		fn()
	}()
}
