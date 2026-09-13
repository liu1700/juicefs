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

package fuse

import (
	"path/filepath"
	"syscall"
	"testing"
	"unsafe"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"
)

func TestAccessAppliesAllSquashBeforeVFSChecks(t *testing.T) {
	metaConf := meta.DefaultConf()
	m := meta.NewClient("sqlite3://"+filepath.Join(t.TempDir(), "meta.db"), metaConf)
	defer m.Shutdown()
	format := &meta.Format{Name: "access", UUID: "access", Storage: "mem", BlockSize: 4096}
	if err := m.Init(format, true); err != nil {
		t.Fatal(err)
	}
	conf := &vfs.Config{NonDefaultPermission: true, Meta: metaConf, Format: *format, Chunk: &chunk.Config{BlockSize: format.BlockSize * 1024, BufferSize: 30 << 20, MaxUpload: 2, MaxDownload: 2}, FuseOpts: &vfs.FuseOptions{}}
	v := vfs.NewVFS(conf, m, nil, nil, nil)
	root := vfs.NewLogContext(meta.NewContext(0, 0, []uint32{0}))
	entry, err := v.Mknod(root, 1, "legacy-root-file", 0600|syscall.S_IFREG, 0, 0)
	if err != 0 {
		t.Fatalf("create root-owned file: %s", err)
	}

	request := func() *fuse.AccessIn {
		return &fuse.AccessIn{InHeader: fuse.InHeader{NodeId: uint64(entry.Inode), Caller: fuse.Caller{Owner: fuse.Owner{Uid: 65532, Gid: 65532}}}, Mask: fuse.R_OK}
	}
	plain := newFileSystem(conf, v)
	if got := plain.Access(nil, request()); got != fuse.Status(syscall.EACCES) {
		t.Fatalf("unsquashed uid 65532 access = %s, want EACCES", got)
	}

	squashedConf := *conf
	squashedConf.AllSquash = &vfs.AnonymousAccount{Uid: 0, Gid: 0}
	squashedConf.NonDefaultPermission = true
	squashed := newFileSystem(&squashedConf, v)
	if got := squashed.Access(nil, request()); got != fuse.OK {
		t.Fatalf("all-squashed uid 65532 access = %s, want OK", got)
	}
}

func TestVisibleOwnerProjectsFUSEAttrsWithoutChangingMetadata(t *testing.T) {
	metaConf := meta.DefaultConf()
	m := meta.NewClient("sqlite3://"+filepath.Join(t.TempDir(), "meta.db"), metaConf)
	defer m.Shutdown()
	format := &meta.Format{Name: "visible-owner", UUID: "visible-owner", Storage: "mem", BlockSize: 4096}
	if err := m.Init(format, true); err != nil {
		t.Fatal(err)
	}
	conf := &vfs.Config{
		NonDefaultPermission: true,
		Meta:                 metaConf,
		Format:               *format,
		Chunk:                &chunk.Config{BlockSize: format.BlockSize * 1024, BufferSize: 30 << 20, MaxUpload: 2, MaxDownload: 2},
		FuseOpts:             &vfs.FuseOptions{},
		AllSquash:            &vfs.AnonymousAccount{Uid: 0, Gid: 0},
		VisibleOwner:         &vfs.AnonymousAccount{Uid: 65532, Gid: 65532},
	}
	v := vfs.NewVFS(conf, m, nil, nil, nil)
	root := vfs.NewLogContext(meta.NewContext(0, 0, []uint32{0}))
	entry, err := v.Mknod(root, 1, "legacy-root-file", 0600|syscall.S_IFREG, 0, 0)
	if err != 0 {
		t.Fatal(err)
	}
	fs := newFileSystem(conf, v)
	agentHeader := fuse.InHeader{NodeId: 1, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 65532, Gid: 65532}}}

	var lookup fuse.EntryOut
	if got := fs.Lookup(nil, &agentHeader, "legacy-root-file", &lookup); got != fuse.OK {
		t.Fatalf("lookup = %s", got)
	}
	assertVisibleOwner(t, "lookup", lookup.Attr)

	var getattr fuse.AttrOut
	if got := fs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: uint64(entry.Inode), Caller: agentHeader.Caller}}, &getattr); got != fuse.OK {
		t.Fatalf("getattr = %s", got)
	}
	assertVisibleOwner(t, "getattr", getattr.Attr)

	var create fuse.CreateOut
	if got := fs.Create(nil, &fuse.CreateIn{InHeader: agentHeader, Flags: syscall.O_RDWR, Mode: 0600}, "created-by-agent", &create); got != fuse.OK {
		t.Fatalf("create = %s", got)
	}
	assertVisibleOwner(t, "create", create.EntryOut.Attr)
	var createdGetattr fuse.AttrOut
	if got := fs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: create.EntryOut.NodeId, Caller: agentHeader.Caller}}, &createdGetattr); got != fuse.OK {
		t.Fatalf("getattr created inode = %s", got)
	}
	assertVisibleOwner(t, "getattr created inode", createdGetattr.Attr)

	dir, err := v.Mkdir(root, 1, "readdirplus", 0755, 0)
	if err != 0 {
		t.Fatal(err)
	}
	if _, err = v.Mknod(root, dir.Inode, "child", 0600|syscall.S_IFREG, 0, 0); err != 0 {
		t.Fatal(err)
	}
	var opened fuse.OpenOut
	if got := fs.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: uint64(dir.Inode), Caller: agentHeader.Caller}}, &opened); got != fuse.OK {
		t.Fatalf("opendir = %s", got)
	}
	buf := make([]byte, 4096)
	entries := fuse.NewDirEntryList(buf, 0)
	if got := fs.ReadDirPlus(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: uint64(dir.Inode), Caller: agentHeader.Caller}, Fh: opened.Fh, Size: uint32(len(buf))}, entries); got != fuse.OK {
		t.Fatalf("readdirplus = %s", got)
	}
	// AddDirLookupEntry serializes EntryOut at the beginning of the supplied
	// buffer. This directory has exactly one child, whose full attrs take the
	// replyEntry path used by ReadDirPlus.
	plus := (*fuse.EntryOut)(unsafe.Pointer(&buf[0]))
	assertVisibleOwner(t, "readdirplus", plus.Attr)

	// The metadata owner remains root despite the FUSE-client projection.
	var stored meta.Attr
	if st := m.GetAttr(root, entry.Inode, &stored); st != 0 {
		t.Fatalf("get stored attr = %s", st)
	}
	if stored.Uid != 0 || stored.Gid != 0 {
		t.Fatalf("stored owner = %d:%d, want 0:0", stored.Uid, stored.Gid)
	}
	if st := m.GetAttr(root, Ino(create.EntryOut.NodeId), &stored); st != 0 {
		t.Fatalf("get stored created attr = %s", st)
	}
	if stored.Uid != 0 || stored.Gid != 0 {
		t.Fatalf("stored created owner = %d:%d, want 0:0", stored.Uid, stored.Gid)
	}
}

func assertVisibleOwner(t *testing.T, path string, attr fuse.Attr) {
	t.Helper()
	if attr.Uid != 65532 || attr.Gid != 65532 {
		t.Errorf("%s FUSE owner = %d:%d, want 65532:65532", path, attr.Uid, attr.Gid)
	}
}
