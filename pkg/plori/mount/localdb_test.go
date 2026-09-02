//go:build plori
// +build plori

package mount

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func stateWithDatabase(t *testing.T) Paths {
	t.Helper()
	dir := t.TempDir()
	p := Paths{StateDir: filepath.Join(dir, "state"), CacheDir: filepath.Join(dir, "cache"), MountPoint: filepath.Join(dir, "mnt")}
	if err := os.MkdirAll(p.StateDir, 0o700); err != nil {
		t.Fatalf("state dir: %v", err)
	}
	for name, body := range map[string]string{
		"meta.db":             "database",
		"meta.db-wal":         "wal",
		"meta.db-shm":         "shm",
		".meta.db-litestream": "position",
		"unrelated-file":      "keep me",
		"durable-point.json":  "{}",
	} {
		if err := os.WriteFile(filepath.Join(p.StateDir, name), []byte(body), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	return p
}

func point(volume string, epoch int64) *DurablePoint {
	return &DurablePoint{Volume: volume, FenceEpoch: epoch, DurableAt: time.Now().UTC()}
}

// The bug (PLO-422): the state directory is a hostPath, so the SECOND mount of
// an Agent on a node it already ran on found a `meta.db` there, the restore
// refused to write over it, and the worker exited 67. The kubelet retried, each
// retry burned a writer epoch, and an Agent could be mounted exactly once per
// node. A clean predecessor's database is the newest copy there is.
func TestACleanPredecessorsDatabaseIsAdoptedRatherThanRefused(t *testing.T) {
	p := stateWithDatabase(t)
	verdict, why, err := reconcileLocalDatabase(p, "vol-1", true, point("vol-1", 4), 4)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if verdict != localDBAdopted {
		t.Fatalf("verdict = %s (%s), want %s", verdict, why, localDBAdopted)
	}
	if _, err := os.Stat(p.MetaPath()); err != nil {
		t.Fatalf("the adopted database is gone: %v", err)
	}
}

// The server may know a durable point this node does not: another node ran the
// volume in between. Adopting then silently drops everything that writer did.
func TestADatabaseBehindTheControlPlaneIsSetAside(t *testing.T) {
	p := stateWithDatabase(t)
	verdict, _, err := reconcileLocalDatabase(p, "vol-1", true, point("vol-1", 4), 6)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if verdict != localDBSetAside {
		t.Fatalf("verdict = %s, want %s: epoch 6 ran somewhere else after this node's 4", verdict, localDBSetAside)
	}
}

// An unclean predecessor's database is only durable to its last barrier and may
// reference blocks the object store never received. The replica is the copy
// with a restore point and a repair procedure.
func TestAnUncleanPredecessorsDatabaseIsSetAside(t *testing.T) {
	p := stateWithDatabase(t)
	verdict, _, err := reconcileLocalDatabase(p, "vol-1", false, point("vol-1", 4), 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if verdict != localDBSetAside {
		t.Fatalf("verdict = %s, want %s", verdict, localDBSetAside)
	}
}

// A marker with no durable point behind it is a state this worker does not
// produce, so it is not evidence of anything.
func TestACleanMarkerWithNoDurablePointIsNotEnough(t *testing.T) {
	p := stateWithDatabase(t)
	verdict, _, err := reconcileLocalDatabase(p, "vol-1", true, nil, 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if verdict != localDBSetAside {
		t.Fatalf("verdict = %s, want %s", verdict, localDBSetAside)
	}
}

// A durable point naming another volume is somebody else's database. The
// identity check catches it too, later and with exit 65; catching it here means
// the restore is not skipped on its say-so.
func TestADatabaseFromAnotherVolumeIsSetAside(t *testing.T) {
	p := stateWithDatabase(t)
	verdict, _, err := reconcileLocalDatabase(p, "vol-1", true, point("vol-2", 9), 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if verdict != localDBSetAside {
		t.Fatalf("verdict = %s, want %s", verdict, localDBSetAside)
	}
}

// Set aside means MOVED, with everything that belongs to the database: a WAL
// left beside a freshly restored file is replayed into it, and a Litestream
// position left beside it names transactions the restored file does not have.
func TestSetAsideMovesTheWholeDatabaseAndNothingElse(t *testing.T) {
	p := stateWithDatabase(t)
	if _, _, err := reconcileLocalDatabase(p, "vol-1", false, nil, 0); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	for _, gone := range []string{"meta.db", "meta.db-wal", "meta.db-shm", ".meta.db-litestream"} {
		if _, err := os.Stat(filepath.Join(p.StateDir, gone)); !os.IsNotExist(err) {
			t.Errorf("%s is still in place; a restore would land beside it", gone)
		}
		if _, err := os.Stat(filepath.Join(p.StateDir, gone+supersededSuffix)); err != nil {
			t.Errorf("%s was not kept as %s: %v", gone, gone+supersededSuffix, err)
		}
	}
	for _, kept := range []string{"unrelated-file", "durable-point.json"} {
		if _, err := os.Stat(filepath.Join(p.StateDir, kept)); err != nil {
			t.Errorf("%s was moved and should not have been: %v", kept, err)
		}
	}
}

// Exactly one copy is kept, ever. The state directory is a host path shared by
// every mount on the node, and a metadata database is not small.
func TestOnlyOneSupersededCopyIsEverKept(t *testing.T) {
	p := stateWithDatabase(t)
	for round := 0; round < 3; round++ {
		if _, _, err := reconcileLocalDatabase(p, "vol-1", false, nil, 0); err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		// The next generation restores a database into place again.
		if err := os.WriteFile(p.MetaPath(), []byte("restored"), 0o600); err != nil {
			t.Fatalf("stage a restored database: %v", err)
		}
	}
	entries, err := os.ReadDir(p.StateDir)
	if err != nil {
		t.Fatalf("read state dir: %v", err)
	}
	copies := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == supersededSuffix {
			copies++
		}
	}
	if copies != 1 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Fatalf("%d superseded copies after three set-asides, want 1: %v", copies, names)
	}
}

// A state directory with no database at all is the first mount, and it must not
// be mistaken for either verdict.
func TestNoLocalDatabaseIsNotAVerdict(t *testing.T) {
	dir := t.TempDir()
	p := Paths{StateDir: filepath.Join(dir, "state")}
	if err := os.MkdirAll(p.StateDir, 0o700); err != nil {
		t.Fatalf("state dir: %v", err)
	}
	verdict, _, err := reconcileLocalDatabase(p, "vol-1", true, point("vol-1", 1), 0)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if verdict != localDBAbsent {
		t.Fatalf("verdict = %s, want %s", verdict, localDBAbsent)
	}
}
