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
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPloriSliceIDFloorIsDeterministicAndMonotonic(t *testing.T) {
	at := func(s string) time.Time {
		ts, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			t.Fatalf("parse %s: %s", s, err)
		}
		return ts
	}

	// Deterministic: the same instant always gives the same number, whatever
	// zone it is expressed in.
	epochPlusOneSecond := at("2026-01-01T00:00:01Z")
	if got := PloriSliceIDFloor(epochPlusOneSecond); got != 1_000_000 {
		t.Fatalf("one second after the epoch: got %d, want 1000000", got)
	}
	if got := PloriSliceIDFloor(epochPlusOneSecond.In(time.FixedZone("x", 8*3600))); got != 1_000_000 {
		t.Fatalf("the same instant in another zone: got %d, want 1000000", got)
	}

	// Monotonic in the clock, and separating by a microsecond is enough.
	a := PloriSliceIDFloor(at("2026-09-06T12:00:00.000000Z"))
	b := PloriSliceIDFloor(at("2026-09-06T12:00:00.000001Z"))
	if b != a+1 {
		t.Fatalf("one microsecond apart: %d then %d", a, b)
	}
	if c := PloriSliceIDFloor(at("2026-09-06T12:00:01Z")); c <= b {
		t.Fatalf("a second later is not higher: %d then %d", b, c)
	}

	// Before the epoch there is no floor to apply.
	if got := PloriSliceIDFloor(at("2025-12-31T23:59:59Z")); got != 0 {
		t.Fatalf("before the epoch: got %d, want 0", got)
	}

	// The number stays small enough to be uninteresting for a very long time.
	if got := PloriSliceIDFloor(at("2126-01-01T00:00:00Z")); got > 1<<62 {
		t.Fatalf("a century of microseconds overflows into %d", got)
	}
}

// openSliceFloorVolume formats a SQLite volume at path and opens a session on
// it. `file://` object storage is not involved: NewSlice is pure metadata.
func openSliceFloorVolume(t *testing.T, path string) Meta {
	t.Helper()
	conf := DefaultConf()
	conf.Heartbeat = 100 * time.Millisecond
	m := NewClient("sqlite3://"+path, conf)
	format := &Format{
		Name:      "slice-floor",
		BlockSize: 4096,
		Capacity:  1 << 30,
		TrashDays: 1,
	}
	if err := m.Init(format, false); err != nil {
		t.Fatalf("init %s: %s", path, err)
	}
	if err := m.NewSession(true); err != nil {
		t.Fatalf("new session on %s: %s", path, err)
	}
	return m
}

func closeSliceFloorVolume(t *testing.T, m Meta) {
	t.Helper()
	if err := m.CloseSession(); err != nil {
		t.Fatalf("close session: %s", err)
	}
	if err := m.Shutdown(); err != nil {
		t.Fatalf("shutdown: %s", err)
	}
}

func newSliceID(t *testing.T, m Meta) uint64 {
	t.Helper()
	var id uint64
	if st := m.NewSlice(Background(), &id); st != 0 {
		t.Fatalf("new slice: %s", st)
	}
	return id
}

func copyFile(t *testing.T, from, to string) {
	t.Helper()
	b, err := os.ReadFile(from)
	if err != nil {
		t.Fatalf("read %s: %s", from, err)
	}
	if err := os.WriteFile(to, b, 0o600); err != nil {
		t.Fatalf("write %s: %s", to, err)
	}
}

// TestTwoRestoresFromOnePointGetDifferentSliceIDs is PLO-569.
//
// A restored replica carries the nextChunk counter back to the restored point,
// so two writers that each restore the SAME point are each handed the same next
// slice ID. A slice ID names an object key, so the second writer's blocks land
// on the first writer's keys and the first writer's files read back as somebody
// else's bytes.
//
// The first half of this test reproduces that: two databases restored from one
// snapshot allocate the identical ID. The second half runs the same two
// restores with the floor applied and requires the IDs to differ.
func TestTwoRestoresFromOnePointGetDifferentSliceIDs(t *testing.T) {
	dir := t.TempDir()
	point := filepath.Join(dir, "point.db")

	// The point: a volume that has already allocated some slices.
	origin := filepath.Join(dir, "origin.db")
	m := openSliceFloorVolume(t, origin)
	var lastAtPoint uint64
	for i := 0; i < 3; i++ {
		lastAtPoint = newSliceID(t, m)
	}
	closeSliceFloorVolume(t, m)
	copyFile(t, origin, point)

	// Without the floor: both restores hand out the same ID.
	restore := func(name string, raise bool) uint64 {
		path := filepath.Join(dir, name)
		copyFile(t, point, path)
		m := openSliceFloorVolume(t, path)
		defer closeSliceFloorVolume(t, m)
		if raise {
			floor := PloriSliceIDFloor(time.Now())
			raised, from, to, err := PloriRaiseSliceIDFloor(m, floor)
			if err != nil {
				t.Fatalf("raise the floor on %s: %s", name, err)
			}
			if !raised {
				t.Fatalf("%s: the floor %d did not raise nextChunk from %d", name, floor, from)
			}
			if to < floor {
				t.Fatalf("%s: nextChunk is %d after a floor of %d", name, to, floor)
			}
		}
		return newSliceID(t, m)
	}

	bare1, bare2 := restore("bare-1.db", false), restore("bare-2.db", false)
	if bare1 != bare2 {
		t.Fatalf("the hazard did not reproduce: two restores of one point gave %d and %d", bare1, bare2)
	}
	if bare1 <= lastAtPoint {
		t.Fatalf("a restored volume reissued an ID at or below the point's last one: %d <= %d", bare1, lastAtPoint)
	}

	// With the floor: they do not.
	raised1, raised2 := restore("raised-1.db", true), restore("raised-2.db", true)
	if raised1 == raised2 {
		t.Fatalf("two restores of one point still share slice ID %d", raised1)
	}
	if raised2 <= raised1 {
		t.Fatalf("the later restore did not get the higher ID: %d then %d", raised1, raised2)
	}
	if raised1 <= bare1 {
		t.Fatalf("the floor did not lift the first restore above the point: %d <= %d", raised1, bare1)
	}
}

func TestPloriRaiseSliceIDFloorNeverLowersTheCounter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meta.db")
	m := openSliceFloorVolume(t, path)
	defer closeSliceFloorVolume(t, m)

	high := PloriSliceIDFloor(time.Now())
	raised, _, after, err := PloriRaiseSliceIDFloor(m, high)
	if err != nil || !raised {
		t.Fatalf("first raise: raised=%v err=%v", raised, err)
	}

	// A floor below the counter changes nothing and is not an error: a writer
	// whose clock is behind the previous writer's must not undo the raise.
	raised, from, to, err := PloriRaiseSliceIDFloor(m, high-1_000_000)
	if err != nil {
		t.Fatalf("second raise: %s", err)
	}
	if raised {
		t.Fatalf("a floor below the counter reported a raise: %d -> %d", from, to)
	}
	if to != after {
		t.Fatalf("a floor below the counter moved nextChunk from %d to %d", after, to)
	}

	// Repeating the same floor is a no-op too.
	raised, _, to, err = PloriRaiseSliceIDFloor(m, high)
	if err != nil {
		t.Fatalf("third raise: %s", err)
	}
	if raised || to != after {
		t.Fatalf("repeating the floor moved nextChunk to %d (raised=%v)", to, raised)
	}

	// The next ID issued is at or above the floor.
	if id := newSliceID(t, m); id < uint64(high) {
		t.Fatalf("next slice ID %d is below the floor %d", id, high)
	}

	// A floor of zero or less is refused rather than silently ignored.
	if _, _, _, err := PloriRaiseSliceIDFloor(m, 0); err == nil {
		t.Fatal("a floor of 0 was accepted")
	}
	if _, _, _, err := PloriRaiseSliceIDFloor(m, -1); err == nil {
		t.Fatal("a negative floor was accepted")
	}
}
