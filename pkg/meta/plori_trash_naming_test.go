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
	"strings"
	"testing"
	"time"
)

// These tests need no engine and no Redis: the naming is a pure function, and
// pinning it is the point. plori-runtime's storage-worker derives the Files
// panel's undo handle from these two exported helpers, and a change here that
// nobody notices does not fail its build — it empties the handle. So the
// expected strings below are written out, not derived: an upstream change to the
// trash layout has to walk past a red test here first.

func TestTrashEntryNameIsPinned(t *testing.T) {
	long := strings.Repeat("f", MaxName)
	for _, c := range []struct {
		name   string
		parent Ino
		inode  Ino
		entry  string
		want   string
	}{
		{name: "root", parent: RootInode, inode: 5, entry: "notes.txt", want: "1-5-notes.txt"},
		{name: "nested", parent: 42, inode: 4096, entry: "a b.md", want: "42-4096-a b.md"},
		{name: "empty name", parent: 2, inode: 3, entry: "", want: "2-3-"},
		{name: "dashes in the name survive", parent: 7, inode: 8, entry: "1-2-x", want: "7-8-1-2-x"},
		{
			// Truncation keeps the prefix, which is what makes the entry
			// unique and parseable; the tail of the name is what is lost.
			name:   "too long is cut at MaxName",
			parent: RootInode, inode: 9, entry: long,
			want: ("1-9-" + long)[:MaxName],
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := TrashEntryName(c.parent, c.inode, c.entry); got != c.want {
				t.Errorf("TrashEntryName(%d, %d, %q) = %q, want %q", c.parent, c.inode, c.entry, got, c.want)
			}
		})
	}
}

func TestTrashBucketNameIsPinned(t *testing.T) {
	for _, c := range []struct {
		name string
		at   time.Time
		want string
	}{
		{name: "utc", at: time.Date(2026, 9, 3, 7, 41, 12, 0, time.UTC), want: "2026-09-03-07"},
		{name: "midnight", at: time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), want: "2026-01-02-00"},
		{
			// The bucket is compared against a UTC retention edge in
			// cleanupTrash, so a non-UTC clock must not move the bucket.
			name: "a non-utc clock is converted, not formatted as-is",
			at:   time.Date(2026, 9, 3, 7, 41, 12, 0, time.FixedZone("UTC+8", 8*3600)),
			want: "2026-09-02-23",
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := TrashBucketName(c.at); got != c.want {
				t.Errorf("TrashBucketName(%s) = %q, want %q", c.at, got, c.want)
			}
		})
	}
}

// TestTheEngineNamesTrashThroughTheExportedHelpers is the guard the pinned tables
// cannot be. Two copies of a rule agree on the day they are written; what has to
// stay true is that there is one copy. The engine's unexported trashEntry is a
// call into TrashEntryName, and an upstream merge that restores its old body
// would leave both tables green while the worker drifted away from the engine.
func TestTheEngineNamesTrashThroughTheExportedHelpers(t *testing.T) {
	var m baseMeta
	if got, want := m.trashEntry(9, 10, "x"), TrashEntryName(9, 10, "x"); got != want {
		t.Errorf("the engine files a trash entry as %q but the exported name is %q; "+
			"they must be one derivation (PLO-482)", got, want)
	}

	// checkTrash formats a bucket and cleanupTrash parses it back; a layout that
	// does not round-trip means buckets that never expire ("bad entry as a
	// subTrash"). Both sides read trashBucketLayout, and this is that contract.
	at := time.Date(2026, 9, 3, 7, 41, 12, 0, time.UTC)
	ts, err := time.Parse(trashBucketLayout, TrashBucketName(at))
	if err != nil {
		t.Fatalf("the trash bucket name does not parse back with its own layout: %v", err)
	}
	if want := at.Truncate(time.Hour); !ts.Equal(want) {
		t.Errorf("bucket %q parsed back as %s, want the hour %s", TrashBucketName(at), ts, want)
	}
}
