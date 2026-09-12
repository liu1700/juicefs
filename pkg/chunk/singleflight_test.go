/*
 * JuiceFS, Copyright 2020 Juicedata, Inc.
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

package chunk

import (
	"bytes"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// joinedCallers reports how many callers are parked on the in-flight request
// for key, or -1 when nothing is in flight for it.
//
// request.dups is the only thing in the singleflight that proves a caller has
// arrived. Execute and TryPiggyback both increment it under the controller
// lock, from inside the call, after they have found the in-flight request and
// committed to waiting on its result -- a caller counted here can no longer
// start a flight of its own. A counter the caller bumps just before it calls
// Execute proves only that it is about to call: it could still arrive after the
// leader's flight has finished, and then it does start a second one. That is
// the difference between this barrier and the 500ms sleep it replaces.
func joinedCallers(con *Controller, key string) int {
	con.Lock()
	defer con.Unlock()
	if c, ok := con.rs[key]; ok {
		return c.dups
	}
	return -1
}

// awaitJoined blocks until want callers have joined key's in-flight request.
func awaitJoined(con *Controller, key string, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		joined := joinedCallers(con, key)
		if joined >= want {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("only %d of %d callers joined the flight for key %s after %s", joined, want, key, timeout)
		}
		time.Sleep(100 * time.Microsecond)
	}
}

// TestSingleFlight drives callersPerKey callers at each of keys keys -- the
// 100000 callers this test has always driven -- and requires the work function
// to have run exactly once per key.
//
// The execution count is asserted exactly, so the test has to guarantee that
// every caller for a key reaches the controller while that key's flight is
// still open. It does that with a barrier rather than a clock: the work
// function blocks on release, and release is not closed until joinedCallers
// reports that every other caller for the key has joined the leader's request.
// Waiting on wall-clock time instead -- the 500ms sleep this replaces -- makes
// the count a scheduling race: on a loaded host under -race a few callers
// arrive after their leader has finished and execute the work a second time.
func TestSingleFlight(t *testing.T) {
	const (
		keys             = 1000
		callersPerKey    = 100
		piggybacksPerKey = 100
		concurrentGroups = 64 // caps how many keys are in flight at once
		barrierTimeout   = time.Minute
	)

	g := NewController()
	var executions int32
	var piggybacked atomic.Int64
	var pages sync.Map

	// The first failure wins; reported from the test goroutine after the groups
	// are done, because t.Fatalf may only be called on that goroutine.
	var failOnce sync.Once
	var firstErr error
	fail := func(format string, args ...interface{}) {
		err := fmt.Errorf(format, args...)
		failOnce.Do(func() { firstErr = err })
	}

	var groups sync.WaitGroup
	running := make(chan struct{}, concurrentGroups)
	for k := 0; k < keys; k++ {
		groups.Add(1)
		go func(k int) {
			defer groups.Done()
			running <- struct{}{}
			defer func() { <-running }()

			key := strconv.Itoa(k)
			want := make([]byte, 100)
			copy(want, key)

			var startOnce sync.Once
			started := make(chan struct{})
			release := make(chan struct{})
			work := func() (*Page, error) {
				startOnce.Do(func() { close(started) })
				<-release
				atomic.AddInt32(&executions, 1)
				page := NewOffPage(100)
				copy(page.Data, make([]byte, 100)) // zeroed
				copy(page.Data, key)
				return page, nil
			}

			var callers sync.WaitGroup
			execute := func() {
				defer callers.Done()
				p, err := g.Execute(key, work)
				if err != nil {
					fail("key %s: Execute: %v", key, err)
					return
				}
				p.Release()
				pages.LoadOrStore(key, p)
			}

			// The leader. The key is fresh, so it takes Execute's own path and
			// enters work, which parks it on release with the request
			// registered on the controller.
			callers.Add(1)
			go execute()
			select {
			case <-started:
			case <-time.After(barrierTimeout):
				fail("key %s: the first Execute never entered the work function", key)
				close(release)
				callers.Wait()
				return
			}

			for i := 1; i < callersPerKey; i++ {
				callers.Add(1)
				go execute()
			}
			// Past this point every other caller for the key is inside Execute
			// waiting on the leader's request, so the key cannot be executed
			// twice however the goroutines are scheduled.
			if err := awaitJoined(g, key, callersPerKey-1, barrierTimeout); err != nil {
				fail("key %s: %v", key, err)
				close(release)
				callers.Wait()
				return
			}

			for i := 0; i < piggybacksPerKey; i++ {
				callers.Add(1)
				go func() {
					defer callers.Done()
					page, err := g.TryPiggyback(key)
					if err != nil {
						fail("key %s: TryPiggyback: %v", key, err)
						return
					}
					if page == nil {
						fail("key %s: TryPiggyback found no request while the flight was held open", key)
						return
					}
					if !bytes.Equal(page.Data, want) {
						fail("got %x, want %x, key: %s", page.Data, want, key)
					}
					page.Release()
					piggybacked.Add(1)
				}()
			}
			// The flight is still open, so every piggyback joins it as well:
			// the piggyback count below is exact, not "at least one".
			if err := awaitJoined(g, key, callersPerKey-1+piggybacksPerKey, barrierTimeout); err != nil {
				fail("key %s: %v", key, err)
			}

			close(release)
			callers.Wait()
		}(k)
	}
	groups.Wait()

	if firstErr != nil {
		t.Fatalf("Test failed: %v", firstErr)
	}
	if nv := int(atomic.LoadInt32(&executions)); nv != keys {
		t.Fatalf("singleflight doesn't take effect: %v", nv)
	}
	if pb := piggybacked.Load(); pb != int64(keys*piggybacksPerKey) {
		t.Fatalf("piggybacked %d times, want %d", pb, keys*piggybacksPerKey)
	}

	// verify the ref: the leader acquires one reference per joined caller, and
	// each caller releases the one it was handed.
	pages.Range(func(key any, value any) bool {
		if value.(*Page).refs != 0 {
			t.Fatalf("refs of page is not 0, got: %d, key: %s", value.(*Page).refs, key)
		}
		return true
	})
}
