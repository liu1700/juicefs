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

package meta

import (
	"sync"
	"sync/atomic"
	"syscall"
	"testing"

	"xorm.io/xorm"
)

func cloneFenceMeta(t *testing.T) (*dbMeta, Context) {
	t.Helper()
	m, _ := openQuotaVolume(t, 8<<20, 1024)
	return m, Background()
}

func cloneFenceMkdir(t *testing.T, m *dbMeta, ctx Context, parent Ino, name string) Ino {
	t.Helper()
	var inode Ino
	if st := m.Mkdir(ctx, parent, name, 0755, 022, 0, &inode, nil); st != 0 {
		t.Fatalf("mkdir %q: %s", name, st)
	}
	return inode
}

func cloneFenceFile(t *testing.T, m *dbMeta, ctx Context, parent Ino, name string) Ino {
	t.Helper()
	var inode Ino
	if st := m.Mknod(ctx, parent, name, TypeFile, 0644, 022, 0, "", &inode, nil); st != 0 {
		t.Fatalf("mknod %q: %s", name, st)
	}
	return inode
}

func cloneFenceAbsent(t *testing.T, m *dbMeta, ctx Context, parent Ino, name string) {
	t.Helper()
	var inode Ino
	var attr Attr
	if st := m.Lookup(ctx, parent, name, &inode, &attr, false); st != syscall.ENOENT {
		t.Fatalf("lookup %q = %s, want ENOENT", name, st)
	}
}

func cloneFencePresent(t *testing.T, m *dbMeta, ctx Context, parent Ino, name string) Ino {
	t.Helper()
	var inode Ino
	var attr Attr
	if st := m.Lookup(ctx, parent, name, &inode, &attr, false); st != 0 {
		t.Fatalf("lookup %q = %s, want success", name, st)
	}
	return inode
}

// cloneFenceEngine injects a test-only boundary around the actual SQL backend.
// It never substitutes metadata operations: every call first reaches dbMeta.
type cloneFenceEngine struct {
	engine
	afterCloneEntry func(top bool)
	afterBatchClone func()
	afterDirList    func(entries []*Entry)
}

func (e *cloneFenceEngine) doCloneEntry(ctx Context, srcIno Ino, parent Ino, name string, ino Ino, attr *Attr, cmode uint8, cumask uint16, top bool) syscall.Errno {
	st := e.engine.doCloneEntry(ctx, srcIno, parent, name, ino, attr, cmode, cumask, top)
	if st == 0 && e.afterCloneEntry != nil {
		e.afterCloneEntry(top)
	}
	return st
}

func (e *cloneFenceEngine) doBatchClone(ctx Context, srcParent Ino, dstParent Ino, entries []*Entry, cmode uint8, cumask uint16, result *batchCloneResult) syscall.Errno {
	st := e.engine.doBatchClone(ctx, srcParent, dstParent, entries, cmode, cumask, result)
	if st == 0 && e.afterBatchClone != nil {
		e.afterBatchClone()
	}
	return st
}

func (e *cloneFenceEngine) newDirHandler(inode Ino, plus bool, entries []*Entry) DirHandler {
	h := e.engine.newDirHandler(inode, plus, entries)
	if e.afterDirList == nil {
		return h
	}
	return &cloneFenceDirHandler{DirHandler: h, afterList: e.afterDirList}
}

type cloneFenceDirHandler struct {
	DirHandler
	afterList func(entries []*Entry)
}

func (h *cloneFenceDirHandler) List(ctx Context, offset int) ([]*Entry, syscall.Errno) {
	entries, st := h.DirHandler.List(ctx, offset)
	if st == 0 {
		h.afterList(entries)
	}
	return entries, st
}

func installCloneFenceEngine(t *testing.T, m *dbMeta, e *cloneFenceEngine) {
	t.Helper()
	old := m.en
	m.en = e
	t.Cleanup(func() { m.en = old })
}

func cloneFenceRun(t *testing.T, m *dbMeta, ctx Context, src Ino, parent Ino, name string, count *uint64) syscall.Errno {
	t.Helper()
	var total uint64
	return m.Clone(ctx, RootInode, src, parent, name, CLONE_MODE_PRESERVE_ATTR, 0, 1, count, &total)
}

func TestPloriWorkspaceCloneTxnCommitsOnHealthyAuthority(t *testing.T) {
	m, ctx := cloneFenceMeta(t)
	const inode = Ino(12345)
	if err := m.cloneTxn(ctx, func(s *xorm.Session) error {
		_, err := s.Insert(&detachedNode{Inode: inode})
		return err
	}, RootInode); err != nil {
		t.Fatalf("clone transaction = %v, want success", err)
	}
	if ok, err := m.db.Exist(&detachedNode{Inode: inode}); err != nil || !ok {
		t.Fatalf("committed detached node = (%t, %v), want (true, nil)", ok, err)
	}
}

// TestPloriWorkspaceCloneTxnFenceAfterBodyRollsBackSQLite inserts a durable
// row, fences through the production atomic predicate, and proves cloneTxn's
// post-body check returns an error before xorm commits the transaction.
func TestPloriWorkspaceCloneTxnFenceAfterBodyRollsBackSQLite(t *testing.T) {
	m, ctx := cloneFenceMeta(t)
	const inode = Ino(12346)
	if err := m.cloneTxn(ctx, func(s *xorm.Session) error {
		if _, err := s.Insert(&detachedNode{Inode: inode}); err != nil {
			return err
		}
		fenceForTest(t)
		return nil
	}, RootInode); err != syscall.EROFS {
		t.Fatalf("clone transaction after post-body fence = %v, want EROFS", err)
	}
	if ok, err := m.db.Exist(&detachedNode{Inode: inode}); err != nil || ok {
		t.Fatalf("rolled-back detached node = (%t, %v), want (false, nil)", ok, err)
	}
}

// TestPloriWorkspaceCloneTxnRechecksAuthorityOnRetry proves a retry cannot
// start its body after authority changed between transaction attempts.
func TestPloriWorkspaceCloneTxnRechecksAuthorityOnRetry(t *testing.T) {
	m, ctx := cloneFenceMeta(t)
	bodies := 0
	err := m.cloneTxn(ctx, func(*xorm.Session) error {
		bodies++
		if bodies == 1 {
			fenceForTest(t)
			return errBusy
		}
		return syscall.EPERM
	}, RootInode)
	if err != syscall.EROFS {
		t.Fatalf("clone transaction retry = %v, want EROFS", err)
	}
	if bodies != 1 {
		t.Fatalf("transaction bodies = %d, want only the initial attempt", bodies)
	}
}

// TestPloriWorkspaceBatchCloneAccountsCommittedFenceResult fences after the
// real SQL batch transaction commits. The caller sees EROFS, but counters and
// quota accounting retain the committed file.
func TestPloriWorkspaceBatchCloneAccountsCommittedFenceResult(t *testing.T) {
	m, ctx := cloneFenceMeta(t)
	src := cloneFenceMkdir(t, m, ctx, RootInode, "source")
	file := cloneFenceFile(t, m, ctx, src, "file")
	dst := cloneFenceMkdir(t, m, ctx, RootInode, "destination")
	installCloneFenceEngine(t, m, &cloneFenceEngine{
		engine: m.en,
		afterBatchClone: func() {
			fenceForTest(t)
		},
	})
	before := atomic.LoadInt64(&m.newInodes)
	var count uint64
	entries := []*Entry{{Inode: file, Name: []byte("file")}}
	if st := m.getBase().BatchClone(ctx, src, dst, entries, CLONE_MODE_PRESERVE_ATTR, 0, &count); st != syscall.EROFS {
		t.Fatalf("batch clone after committed fence = %s, want EROFS", st)
	}
	cloneFencePresent(t, m, ctx, dst, "file")
	if got := atomic.LoadInt64(&m.newInodes); got != before+1 {
		t.Fatalf("new inode accounting = %d, want %d", got, before+1)
	}
	if count != 1 {
		t.Fatalf("batch clone count = %d, want 1", count)
	}
}

func TestPloriWorkspaceBatchCloneAccountsCanceledCommitThroughParentCacheMiss(t *testing.T) {
	m, setup := cloneFenceMeta(t)
	src := cloneFenceMkdir(t, m, setup, RootInode, "source")
	file := cloneFenceFile(t, m, setup, src, "file")
	dst := cloneFenceMkdir(t, m, setup, RootInode, "destination")
	ancestorQuota := &Quota{MaxSpace: -1, MaxInodes: -1}
	m.quotaMu.Lock()
	m.dirQuotas[uint64(RootInode)] = ancestorQuota
	m.quotaMu.Unlock()
	m.parentMu.Lock()
	delete(m.dirParents, dst)
	m.parentMu.Unlock()

	ctx := Background()
	installCloneFenceEngine(t, m, &cloneFenceEngine{
		engine: m.en,
		afterBatchClone: func() {
			ctx.Cancel()
		},
	})
	var count uint64
	entries := []*Entry{{Inode: file, Name: []byte("file")}}
	if st := m.getBase().BatchClone(ctx, src, dst, entries, CLONE_MODE_PRESERVE_ATTR, 0, &count); st != syscall.EINTR {
		t.Fatalf("batch clone after committed cancellation = %s, want EINTR", st)
	}
	cloneFencePresent(t, m, setup, dst, "file")
	if got := atomic.LoadInt64(&ancestorQuota.newInodes); got != 1 {
		t.Fatalf("ancestor quota inodes after parent cache miss = %d, want 1", got)
	}
	if count != 1 {
		t.Fatalf("batch clone count after cancellation = %d, want 1", count)
	}
}

// TestPloriWorkspaceRecursiveCloneFencesBeforeChild fences after the actual
// directory read returns the child. The recursive cloneEntry guard must refuse
// before it starts the child's metadata transaction or publishes the root.
func TestPloriWorkspaceRecursiveCloneFencesBeforeChild(t *testing.T) {
	m, ctx := cloneFenceMeta(t)
	src := cloneFenceMkdir(t, m, ctx, RootInode, "source")
	cloneFenceMkdir(t, m, ctx, src, "child")
	var fenceOnce sync.Once
	installCloneFenceEngine(t, m, &cloneFenceEngine{
		engine: m.en,
		afterDirList: func(entries []*Entry) {
			for _, entry := range entries {
				if string(entry.Name) != "." && string(entry.Name) != ".." {
					fenceOnce.Do(func() { fenceForTest(t) })
					return
				}
			}
		},
	})
	var count uint64
	if st := cloneFenceRun(t, m, ctx, src, RootInode, "destination", &count); st != syscall.EROFS {
		t.Fatalf("recursive clone after child fence = %s, want EROFS", st)
	}
	cloneFenceAbsent(t, m, ctx, RootInode, "destination")
	if count != 1 {
		t.Fatalf("recursive clone count = %d, want only the detached root", count)
	}
}

// TestPloriWorkspaceDirectoryCloneFencesBeforeAttach fences at the completed
// top-level clone-entry boundary. The base guard must refuse before attach.
func TestPloriWorkspaceDirectoryCloneFencesBeforeAttach(t *testing.T) {
	m, ctx := cloneFenceMeta(t)
	src := cloneFenceMkdir(t, m, ctx, RootInode, "source")
	installCloneFenceEngine(t, m, &cloneFenceEngine{
		engine: m.en,
		afterCloneEntry: func(top bool) {
			if top {
				fenceForTest(t)
			}
		},
	})
	var count uint64
	if st := cloneFenceRun(t, m, ctx, src, RootInode, "destination", &count); st != syscall.EROFS {
		t.Fatalf("directory clone before attach fence = %s, want EROFS", st)
	}
	cloneFenceAbsent(t, m, ctx, RootInode, "destination")
	if count != 1 {
		t.Fatalf("directory clone count = %d, want detached root", count)
	}
}

// TestPloriWorkspaceCloneAccountsCommittedCancellation fences no lease. It
// cancels the request after the real file-clone transaction commits, then
// requires EINTR plus accounting for the already named destination.
func TestPloriWorkspaceCloneAccountsCommittedCancellation(t *testing.T) {
	m, setup := cloneFenceMeta(t)
	src := cloneFenceFile(t, m, setup, RootInode, "source")
	ctx := Background()
	installCloneFenceEngine(t, m, &cloneFenceEngine{
		engine: m.en,
		afterCloneEntry: func(top bool) {
			if top {
				ctx.Cancel()
			}
		},
	})
	beforeInodes := atomic.LoadInt64(&m.newInodes)
	beforeRoot, st := m.GetDirStat(setup, RootInode)
	if st != 0 {
		t.Fatalf("root stat before clone: %s", st)
	}
	var count uint64
	if st := cloneFenceRun(t, m, ctx, src, RootInode, "destination", &count); st != syscall.EINTR {
		t.Fatalf("clone after committed cancellation = %s, want EINTR", st)
	}
	cloneFencePresent(t, m, setup, RootInode, "destination")
	if got := atomic.LoadInt64(&m.newInodes); got != beforeInodes+1 {
		t.Fatalf("new inode accounting = %d, want %d", got, beforeInodes+1)
	}
	afterRoot, st := m.GetDirStat(setup, RootInode)
	if st != 0 {
		t.Fatalf("root stat after clone: %s", st)
	}
	if afterRoot.inodes != beforeRoot.inodes+1 {
		t.Fatalf("root inode accounting = %d, want %d", afterRoot.inodes, beforeRoot.inodes+1)
	}
	if count != 1 {
		t.Fatalf("clone count = %d, want 1", count)
	}
}

func TestPloriWorkspaceCloneRefusesCanceledContextBeforeBackend(t *testing.T) {
	m, setup := cloneFenceMeta(t)
	src := cloneFenceMkdir(t, m, setup, RootInode, "source")
	ctx := Background()
	ctx.Cancel()
	var count uint64

	if st := cloneFenceRun(t, m, ctx, src, RootInode, "destination", &count); st != syscall.EINTR {
		t.Fatalf("clone with canceled context = %s, want EINTR", st)
	}
	cloneFenceAbsent(t, m, setup, RootInode, "destination")
	if count != 0 {
		t.Fatalf("canceled clone count = %d, want 0", count)
	}
}

func TestPloriWorkspaceCloneAllowedGuardsEveryEngine(t *testing.T) {
	m := &baseMeta{conf: &Config{}}
	fenceForTest(t)
	if st := m.cloneAllowed(Background()); st != syscall.EROFS {
		t.Fatalf("cloneAllowed after fence = %s, want EROFS", st)
	}
}

func TestPloriWorkspaceRepairRefusesFencedMetadataMutation(t *testing.T) {
	m, ctx := cloneFenceMeta(t)
	inode := cloneFenceMkdir(t, m, ctx, RootInode, "repair")
	var before Attr
	if st := m.GetAttr(ctx, inode, &before); st != 0 {
		t.Fatalf("get repair directory before fence: %s", st)
	}
	repair := before
	repair.Nlink = before.Nlink + 10

	fenceForTest(t)
	if st := m.doRepair(ctx, inode, &repair); st != syscall.EROFS {
		t.Fatalf("repair after fence = %s, want EROFS", st)
	}
	var after Attr
	if st := m.GetAttr(ctx, inode, &after); st != 0 {
		t.Fatalf("get repair directory after fence: %s", st)
	}
	if after.Nlink != before.Nlink {
		t.Fatalf("fenced repair nlink = %d, want %d", after.Nlink, before.Nlink)
	}
}

func TestPloriWorkspaceRepairRefusesCanceledMetadataMutation(t *testing.T) {
	m, setup := cloneFenceMeta(t)
	inode := cloneFenceMkdir(t, m, setup, RootInode, "repair")
	var before Attr
	if st := m.GetAttr(setup, inode, &before); st != 0 {
		t.Fatalf("get repair directory before cancellation: %s", st)
	}
	repair := before
	repair.Nlink = before.Nlink + 10
	ctx := Background()
	ctx.Cancel()

	if st := m.doRepair(ctx, inode, &repair); st != syscall.EINTR {
		t.Fatalf("repair after cancellation = %s, want EINTR", st)
	}
	var after Attr
	if st := m.GetAttr(setup, inode, &after); st != 0 {
		t.Fatalf("get repair directory after cancellation: %s", st)
	}
	if after.Nlink != before.Nlink {
		t.Fatalf("canceled repair nlink = %d, want %d", after.Nlink, before.Nlink)
	}
}
