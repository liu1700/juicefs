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
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// What to do about a metadata database that is already on this node (PLO-422).
//
// The state directory is a hostPath, so it outlives the Pod, and nothing ever
// removed `meta.db`: start removes only `clean`, and the plugin removes only
// `ready` and `health.json`. Restore refused to write over an existing file, so
// the SECOND mount of an Agent on a node it had already run on exited 67, the
// kubelet retried, and each retry burned a writer epoch — 17 of them in eight
// minutes on staging. An Agent could be mounted exactly once per node.
//
// "Refuse when the file exists" was the right instinct at the wrong layer. What
// must never happen is restoring a replica ON TOP of a live or newer local
// database. What was actually happening is that a database this worker's own
// predecessor left behind, cleanly, was treated as an obstacle.
//
// So the decision is made once, here, on evidence:
//
//	adopt      the previous generation on this node stopped cleanly and its
//	           durable point is not behind the one the control-plane knows.
//	           The local database is then at least as durable as the replica,
//	           and restoring would throw away work to gain nothing.
//	set aside  anything else — an unclean stop, a durable point from a volume
//	           or an epoch that does not line up, or no durable point at all.
//	           The file is moved, not deleted, and the restore proceeds.
//
// Adoption is guarded twice: identityMatches runs afterwards and compares the
// opened Format's Name and UUID against the spec and against the object stored
// under the data prefix, so a database belonging to another volume is exit 65
// whatever this function decided.

// supersededSuffix marks the one database this function keeps. Exactly one:
// the metadata database of a large volume is not small, the state directory is
// a host path shared with every other mount on the node, and an unbounded pile
// of them is the disk-full incident this design already has scars from. One
// copy is a forensic artefact for the case where the restore turns out to be
// the wrong call; two would be a policy nobody asked for.
const supersededSuffix = ".superseded"

// localDBVerdict is what reconcileLocalDatabase decided, for the log line.
type localDBVerdict string

const (
	// localDBAbsent — nothing on disk; the restore is the only source.
	localDBAbsent localDBVerdict = "absent"
	// localDBAdopted — the local database is kept and no restore runs.
	localDBAdopted localDBVerdict = "adopted"
	// localDBSetAside — the local database was moved out of the way.
	localDBSetAside localDBVerdict = "set_aside"
)

// reconcileLocalDatabase decides between the database on this node and the one
// in the replica, and leaves the state directory in the shape that decision
// implies. It reports whether the caller should still restore.
//
// `cleanStop` is whether the previous generation here wrote its `clean` marker,
// read before that marker is removed. `local` is this node's durable point (nil
// when there is none) and `serverEpoch` the epoch of the control-plane's, 0
// when the spec carries none.
func reconcileLocalDatabase(paths Paths, volumeID string, cleanStop bool, local *DurablePoint, serverEpoch int64) (localDBVerdict, string, error) {
	if _, err := os.Stat(paths.MetaPath()); err != nil {
		if os.IsNotExist(err) {
			return localDBAbsent, "no local database", nil
		}
		return localDBAbsent, "", fmt.Errorf("stat the local metadata database: %w", err)
	}

	if reason, ok := adoptable(volumeID, cleanStop, local, serverEpoch); ok {
		return localDBAdopted, reason, nil
	} else if err := setAsideLocalDatabase(paths); err != nil {
		return localDBSetAside, "", err
	} else {
		return localDBSetAside, reason, nil
	}
}

// adoptable is the whole rule, separated from the filesystem so it can be read
// as one statement and tested as one.
func adoptable(volumeID string, cleanStop bool, local *DurablePoint, serverEpoch int64) (reason string, ok bool) {
	if !cleanStop {
		// The previous generation died without finishing its stop, so the
		// database on disk is only durable up to its last barrier and may
		// reference blocks that never reached the object store. The replica is
		// the repairable copy: it has a restore point and a repair procedure
		// (crash-consistency.md §7 Rank 1) and this file has neither.
		return "the previous generation did not finish its stop", false
	}
	if local == nil {
		// A clean stop reports a durable point before it writes the marker, so
		// a marker with no point is a state this worker did not produce —
		// hand-editing, a partial copy, a truncated disk. Not evidence.
		return "the previous generation left no durable point to prove what this database contains", false
	}
	if local.Volume != volumeID {
		return fmt.Sprintf("the local durable point names volume %s", local.Volume), false
	}
	if serverEpoch > local.FenceEpoch {
		// Somebody else has run this volume since this node last did. Whatever
		// they wrote is in the replica and not in this file, and adopting would
		// silently drop it.
		return fmt.Sprintf("the control-plane knows a durable point from epoch %d, newer than this node's %d",
			serverEpoch, local.FenceEpoch), false
	}
	return fmt.Sprintf("clean stop at epoch %d, durable at %s", local.FenceEpoch, local.DurableAt.Format("2006-01-02T15:04:05Z07:00")), true
}

// setAsideLocalDatabase moves the database and everything that belongs to it
// out of the way, replacing any copy a previous set-aside left.
//
// The WAL, the shared-memory file and Litestream's own position directory move
// with it. Leaving any of them beside a freshly restored database is the
// specific corruption this is avoiding: SQLite would replay a WAL that belongs
// to a different file, and Litestream would resume from a position that names
// transactions the restored database does not contain.
func setAsideLocalDatabase(paths Paths) error {
	entries, err := os.ReadDir(paths.StateDir)
	if err != nil {
		return fmt.Errorf("read the state directory: %w", err)
	}
	base := filepath.Base(paths.MetaPath())
	for _, e := range entries {
		name := e.Name()
		if strings.HasSuffix(name, supersededSuffix) {
			// Last time's copy. Removed before this one is written, so exactly
			// one ever exists.
			if err := os.RemoveAll(filepath.Join(paths.StateDir, name)); err != nil {
				return fmt.Errorf("remove the previous superseded database: %w", err)
			}
		}
	}
	for _, e := range entries {
		name := e.Name()
		if strings.HasSuffix(name, supersededSuffix) {
			continue
		}
		// `meta.db`, `meta.db-wal`, `meta.db-shm`, and `.meta.db-litestream`
		// (litestream.MetaDirSuffix, db.go:312).
		if !strings.HasPrefix(name, base) && !strings.HasPrefix(name, "."+base) {
			continue
		}
		from := filepath.Join(paths.StateDir, name)
		to := from + supersededSuffix
		if err := os.Rename(from, to); err != nil {
			return fmt.Errorf("set aside %s: %w", name, err)
		}
	}
	return nil
}
