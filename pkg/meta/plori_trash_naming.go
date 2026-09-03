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

// Where a trashed entry lands, exported once.
//
// This file carries NO `plori` build tag, for the same reason plori_trash.go
// carries none: it has two callers on two different builds. The metadata engine
// itself is one — checkTrash names the hour bucket, doUnlink and doRename name
// the entry inside it, and cleanupTrash parses the bucket back. The other is
// plori-runtime's services/storage-worker, which links this fork as a library on
// a PLAIN build and has to find an entry the engine just created: the Files
// panel's "undo" handle is that path, and it is produced by an unlink whose only
// return value is an errno.
//
// Until PLO-482 the worker restated both rules
// (services/storage-worker/internal/jfsvol/fsops.go), and that copy could only
// fail one way: silently. `findTrashEntry` Lstats the path it derived and returns
// "" when it is not there, so a naming change here — an upstream merge, a
// truncation rule, a different bucket granularity — would not break a build or
// raise an error. It would delete the undo button.
//
// So the derivation is here, in the package that owns the trash, and the engine's
// own unexported `trashEntry` is a call into it rather than a second copy.

// trashBucketLayout is the granularity of the trash: one directory under
// `.trash` per UTC hour. The layout is a constant rather than three literals
// because cleanupTrash PARSES what checkTrash formats — a bucket name that does
// not round-trip is logged as "bad entry as a subTrash" and never expires.
const trashBucketLayout = "2006-01-02-15"

// TrashBucketName is the `.trash` sub-directory an entry deleted at `t` is filed
// under. Always UTC: the buckets are compared to a UTC retention edge in
// cleanupTrash, and a local-time bucket would expire at the wrong hour.
func TrashBucketName(t time.Time) string {
	return t.UTC().Format(trashBucketLayout)
}

// TrashEntryName is the name a deleted entry takes inside its hour bucket.
//
// The parent inode is in the name because the trash is FLAT: one hour bucket
// holds every entry deleted in that hour, from anywhere in the tree, so the
// original directory is not recoverable from the path — only its inode is. Two
// files with the same name deleted from two directories in the same hour are
// distinguished by that prefix, and the same file deleted twice in one hour is
// not (the second unlink overwrites the first entry).
//
// Truncation is at MaxName because the result is an entry name like any other
// and the engine's own limit applies to it; the prefix is what survives, so a
// truncated entry is still unique and still parseable.
func TrashEntryName(parent, inode Ino, name string) string {
	s := fmt.Sprintf("%d-%d-%s", parent, inode, name)
	if len(s) > MaxName {
		s = s[:MaxName]
		logger.Warnf("File name is too long as a trash entry, truncating it: %s -> %s", name, s)
	}
	return s
}
