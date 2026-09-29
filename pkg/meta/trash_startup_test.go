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

package meta

import (
	"fmt"
	"syscall"
	"testing"
	"time"
)

// Override only the coordination clock; retention still uses the real clock.
type trashStartupEngine struct {
	engine
	now      int64
	attempts chan bool
	running  chan struct{}
}

func (e *trashStartupEngine) setIfSmall(name string, value, diff int64) (bool, error) {
	if name != "lastCleanupTrash" {
		return e.engine.setIfSmall(name, value, diff)
	}
	if e.now != 0 {
		value = e.now
	}
	ok, err := e.engine.setIfSmall(name, value, diff)
	e.attempts <- ok
	return ok, err
}

func (e *trashStartupEngine) doCleanupDelayedSlices(ctx Context, edge int64) (int, error) {
	if e.running != nil {
		close(e.running)
		<-ctx.Done()
		return 0, ctx.Err()
	}
	return e.engine.doCleanupDelayedSlices(ctx, edge)
}

func TestCleanupTrashStartup(t *testing.T) {
	for _, e := range volresEngines {
		t.Run(e.name, func(t *testing.T) {
			for _, scenario := range []string{"overdue", "recent", "restarts", "no-bgjob", "read-only", "canceled", "cancel-running"} {
				t.Run(scenario, func(t *testing.T) {
					m, b := volresOpen(t, e, 0, 1)
					probe := &trashStartupEngine{engine: b.en, now: time.Now().Unix(), attempts: make(chan bool, 10)}
					b.en = probe
					ctx := Background()
					var expired, recent, file Ino
					seed := func(name string, inode *Ino) {
						t.Helper()
						v, err := probe.incrCounter("nextTrash", 1)
						if err != nil {
							t.Fatal(err)
						}
						*inode = TrashInode + Ino(v)
						attr := Attr{Typ: TypeDirectory, Nlink: 2, Length: 4096, Parent: TrashInode, Full: true}
						if st := probe.doMknod(ctx, TrashInode, name, TypeDirectory, 0755, 0, "", inode, &attr); st != 0 {
							t.Fatalf("seed trash bucket: %s", st)
						}
					}
					seed(time.Now().UTC().Add(-48*time.Hour).Format(trashBucketLayout), &expired)
					if scenario != "restarts" {
						seed(time.Now().UTC().Format(trashBucketLayout), &recent)
					}
					newFile := func(name string) {
						t.Helper()
						var err error
						file, err = b.nextInode()
						if err != nil {
							t.Fatal(err)
						}
						attr := Attr{Typ: TypeFile, Nlink: 1, Parent: expired, Full: true}
						if st := probe.doMknod(ctx, expired, name, TypeFile, 0644, 0, "", &file, &attr); st != 0 {
							t.Fatal(st)
						}
					}
					newFile("expired-file")

					prior := probe.now - int64((2 * time.Hour).Seconds())
					if scenario == "recent" {
						prior = probe.now - 60
					}
					if _, err := probe.engine.setIfSmall("lastCleanupTrash", prior, 0); err != nil {
						t.Fatal(err)
					}
					waitAttempt := func(want bool) {
						t.Helper()
						select {
						case got := <-probe.attempts:
							if got != want {
								t.Fatalf("gate granted %v, want %v", got, want)
							}
						case <-time.After(3 * time.Second):
							t.Fatal("startup did not check the persisted gate promptly")
						}
					}
					waitRemoved := func() {
						t.Helper()
						deadline := time.Now().Add(3 * time.Second)
						for probe.doGetAttr(ctx, file, &Attr{}) != syscall.ENOENT {
							if time.Now().After(deadline) {
								t.Fatal("expired file was not cleaned promptly")
							}
							time.Sleep(5 * time.Millisecond)
						}
					}
					stopWorker := func(c Context) {
						t.Helper()
						c.Cancel()
						done := make(chan struct{})
						go func() { b.sessWG.Wait(); close(done) }()
						select {
						case <-done:
						case <-time.After(3 * time.Second):
							t.Fatal("cleanup worker did not stop after cancellation")
						}
					}
					if scenario == "cancel-running" {
						probe.running = make(chan struct{})
						c := Background()
						b.sessWG.Add(1)
						go b.cleanupTrash(c)
						t.Cleanup(func() { stopWorker(c) })
						waitAttempt(true)
						select {
						case <-probe.running:
						case <-time.After(3 * time.Second):
							t.Fatal("delayed slice cleanup did not start")
						}
						stopWorker(c)
					} else if scenario == "canceled" || scenario == "restarts" {
						// Restart just the cleanup worker with the same persisted engine and a
						// fake coordination clock: every lifetime is shorter than its hour timer.
						for i := 0; i < 5; i++ {
							c := Background()
							if scenario == "canceled" {
								c.Cancel()
							}
							b.sessWG.Add(1)
							go b.cleanupTrash(c)
							t.Cleanup(func() { stopWorker(c) })
							if scenario == "canceled" {
								stopWorker(c)
								select {
								case <-probe.attempts:
									t.Fatal("canceled worker claimed the gate")
								default:
								}
								break
							}
							waitAttempt(i%2 == 0)
							if i%2 == 0 {
								waitRemoved()
							}
							stopWorker(c)
							probe.now += int64((40 * time.Minute).Seconds())
							if i%2 == 0 && i < 4 {
								// Supply another expired file for the next due restart.
								seed(time.Now().UTC().Add(-48*time.Hour).Format(trashBucketLayout), &expired)
								newFile(fmt.Sprintf("pending-%d", i))
							}
						}
					} else {
						b.conf.NoBGJob = scenario == "no-bgjob"
						b.conf.ReadOnly = scenario == "read-only"
						if err := m.NewSession(false); err != nil {
							t.Fatal(err)
						}
						closed := false
						t.Cleanup(func() {
							if !closed {
								_ = m.CloseSession()
							}
						})
						if b.conf.NoBGJob || b.conf.ReadOnly {
							select {
							case <-probe.attempts:
								t.Fatal("disabled worker checked the gate")
							case <-time.After(100 * time.Millisecond):
							}
						} else {
							waitAttempt(scenario == "overdue")
							if scenario == "overdue" {
								waitRemoved()
							}
						}
						if err := m.CloseSession(); err != nil {
							t.Fatal(err)
						}
						closed = true
					}
					if scenario == "restarts" {
						seed(time.Now().UTC().Format(trashBucketLayout), &recent)
					}
					if st := probe.doGetAttr(ctx, recent, &Attr{}); st != 0 {
						t.Fatalf("retained bucket removed: %s", st)
					}
					if scenario == "recent" || scenario == "no-bgjob" || scenario == "read-only" || scenario == "canceled" {
						if st := probe.doGetAttr(ctx, file, &Attr{}); st != 0 {
							t.Fatalf("cleanup ran despite gate/eligibility/cancellation: %s", st)
						}
						got, err := probe.getCounter("lastCleanupTrash")
						if kv, ok := probe.engine.(*kvMeta); ok {
							var value []byte
							value, err = kv.get(kv.counterKey("lastCleanupTrash"))
							got = kv.parseInt64(value)
						}
						if err != nil || got != prior {
							t.Fatalf("gate changed: got %d, want %d, err %v", got, prior, err)
						}
					}
				})
			}
		})
	}
}
