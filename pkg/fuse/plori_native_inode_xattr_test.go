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
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/juicedata/juicefs/pkg/vfs"
)

func TestPloriNativeInodeXattrIsReadOnlyFUSEIdentity(t *testing.T) {
	fs := &fileSystem{conf: &vfs.Config{EnablePloriNativeInodeXattr: true}}
	header := &fuse.InHeader{NodeId: ^uint64(0)}
	want := "18446744073709551615"

	if got, code := fs.GetXAttr(nil, header, ploriNativeInodeXattr, nil); code != 0 || got != uint32(len(want)) {
		t.Fatalf("size query = (%d, %v), want (%d, 0)", got, code, len(want))
	}
	if got, code := fs.GetXAttr(nil, header, ploriNativeInodeXattr, make([]byte, len(want)-1)); got != 0 || code != fuse.Status(syscall.ERANGE) {
		t.Fatalf("short buffer = (%d, %v), want (0, ERANGE)", got, code)
	}
	buf := make([]byte, len(want))
	if got, code := fs.GetXAttr(nil, header, ploriNativeInodeXattr, buf); code != 0 || got != uint32(len(want)) || string(buf) != want {
		t.Fatalf("read = (%d, %v, %q), want (%d, 0, %q)", got, code, buf, len(want), want)
	}
	if _, code := fs.GetXAttr(nil, header, "user.other", nil); code != fuse.Status(syscall.EOPNOTSUPP) {
		t.Fatalf("other xattr read = %v, want EOPNOTSUPP", code)
	}
	if got, code := fs.ListXAttr(nil, header, make([]byte, 64)); got != 0 || code != 0 {
		t.Fatalf("list = (%d, %v), want (0, 0)", got, code)
	}
	if code := fs.SetXAttr(nil, &fuse.SetXAttrIn{InHeader: *header}, ploriNativeInodeXattr, []byte("forged")); code != fuse.Status(syscall.EOPNOTSUPP) {
		t.Fatalf("identity xattr set = %v, want EOPNOTSUPP", code)
	}
	if code := fs.RemoveXAttr(nil, header, ploriNativeInodeXattr); code != fuse.Status(syscall.EOPNOTSUPP) {
		t.Fatalf("identity xattr remove = %v, want EOPNOTSUPP", code)
	}
	if code := fs.SetXAttr(nil, &fuse.SetXAttrIn{InHeader: *header}, "user.other", []byte("value")); code != fuse.Status(syscall.EOPNOTSUPP) {
		t.Fatalf("other xattr set = %v, want EOPNOTSUPP", code)
	}
	if code := fs.RemoveXAttr(nil, header, "user.other"); code != fuse.Status(syscall.EOPNOTSUPP) {
		t.Fatalf("other xattr remove = %v, want EOPNOTSUPP", code)
	}
}

func TestFuseXattrsEnabledOnlyForExistingOptionOrPloriIdentity(t *testing.T) {
	if fuseXattrsEnabled(&vfs.Config{}, false) {
		t.Fatal("generic mount enables xattrs without its existing option")
	}
	if !fuseXattrsEnabled(&vfs.Config{}, true) {
		t.Fatal("generic mount disables requested xattrs")
	}
	if !fuseXattrsEnabled(&vfs.Config{EnablePloriNativeInodeXattr: true}, false) {
		t.Fatal("Plori identity does not enable FUSE xattr dispatch")
	}
}
