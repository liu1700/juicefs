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
	"time"
)

// This file carries NO `plori` build tag, for the same reason plori_trash.go
// does not: it has callers on two builds. `juicefs plori-mount` is built with
// the release tag set, and plori-runtime's services/storage-worker links this
// fork as a library on a plain build. Both restore a metadata replica to a
// named point and then write to it, so both need the floor.

// ploriSliceIDEpoch is the origin the floor counts from. It is a fixed past
// instant rather than the Unix epoch so the counter starts at a number a volume
// can hold for centuries: microseconds since 1970 is 1.8e15 today and would
// jump every volume's slice IDs by that much on its first mount, for nothing.
var ploriSliceIDEpoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// PloriSliceIDFloor is the lowest slice ID a writer that starts at `now` may
// issue: microseconds since 2026-01-01 UTC.
//
// # What it is for
//
// A slice ID names an object. The block key is
// `chunks/<id/1e6>/<id/1000>/<id>_<index>_<blockSize>`
// (pkg/chunk/cached_store.go), so two writes that are given the same slice ID
// and the same length write the same object key.
//
// Slice IDs come from the `nextChunk` counter in the metadata
// (baseMeta.NewSlice), and a restore carries that counter back to whatever it
// was at the restored point. Two writers that each restore the SAME point are
// therefore each handed the same next ID, and the second one's blocks overwrite
// the first one's in the bucket. The first writer's metadata still references
// those keys, so its files read back as somebody else's bytes: no error
// anywhere, and no way to tell afterwards which copy is which.
//
// A wall clock is what breaks the tie, because it is the one coordinate a
// restore cannot roll back. Two writers that restore the same point at
// different times get different floors, and a writer whose clock is behind
// still cannot go below the counter already in the metadata -- the raise is
// one-directional.
//
// # Why microseconds
//
// The floor has to advance faster than any two restores of the same point can
// be separated in time, and slowly enough that the counter stays a small
// number. A microsecond is well under the cost of a restore -- the fastest one
// measured is in the hundreds of milliseconds -- so two restores of one point
// always land on different floors. At that rate the counter reaches 3.2e14 by
// the year 2036, a thousandth of what an int64 counter or a uint64 slice ID
// holds.
//
// It is deterministic: the same instant always produces the same floor, so a
// caller can test it against a fixed clock.
//
// An instant before the epoch yields 0, which raises nothing.
func PloriSliceIDFloor(now time.Time) int64 {
	us := now.UTC().Sub(ploriSliceIDEpoch).Microseconds()
	if us < 0 {
		return 0
	}
	return us
}

// ploriCounterRaiser is the part of the metadata engine the raise needs.
// setIfSmall is the engine's own compare-and-set on a counter: it writes the
// new value inside a transaction and only when the stored one is not already
// above it, which is exactly "raise, never lower" and does not need a read of
// its own.
type ploriCounterRaiser interface {
	getCounter(name string) (int64, error)
	setIfSmall(name string, value, diff int64) (bool, error)
}

// PloriRaiseSliceIDFloor raises the volume's slice-ID allocator to floor and
// reports the counter before and after.
//
// It never lowers the counter: a floor at or below what the metadata already
// holds is a no-op, and `raised` is false. Call it after a restore and before
// the first write of the new generation; calling it twice is harmless.
//
// A floor of 0 or less is refused rather than treated as "no floor", so a
// caller whose clock is broken finds out instead of silently getting the old
// behaviour back.
//
// On Redis the stored counter is one below the ID the allocator hands out
// (redisMeta.incrCounter returns v+1), so `from` and `to` there are one below
// the numbers a SQL volume reports for the same state. The guarantee is
// unaffected: the next ID issued is still at or above floor, and the counter
// still only moves up.
func PloriRaiseSliceIDFloor(m Meta, floor int64) (raised bool, from, to int64, err error) {
	if floor <= 0 {
		return false, 0, 0, fmt.Errorf("plori: refusing a slice-ID floor of %d", floor)
	}
	en, ok := m.(ploriCounterRaiser)
	if !ok {
		return false, 0, 0, fmt.Errorf("plori: metadata engine %T cannot raise a counter", m)
	}
	from, err = en.getCounter("nextChunk")
	if err != nil {
		return false, 0, 0, fmt.Errorf("plori: read nextChunk: %w", err)
	}
	if from >= floor {
		return false, from, from, nil
	}
	// diff 0 makes the engine's guard `stored > floor`, so an equal counter is
	// rewritten with the same number and a higher one is left alone.
	changed, err := en.setIfSmall("nextChunk", floor, 0)
	if err != nil {
		return false, from, from, fmt.Errorf("plori: raise nextChunk to %d: %w", floor, err)
	}
	if !changed {
		// Another writer moved the counter above the floor between the read and
		// the write. That is the outcome the floor exists to produce, so it is
		// not an error -- but the number reported has to be the real one.
		to, err = en.getCounter("nextChunk")
		if err != nil {
			return false, from, from, fmt.Errorf("plori: re-read nextChunk: %w", err)
		}
		return false, from, to, nil
	}
	to, err = en.getCounter("nextChunk")
	if err != nil {
		return true, from, floor, fmt.Errorf("plori: re-read nextChunk: %w", err)
	}
	return true, from, to, nil
}
