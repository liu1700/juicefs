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
package vfs

import (
	"bytes"
	"syscall"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
)

func TestDisableInternalCommandsRefusesRootAndUntrustedCallers(t *testing.T) {
	for _, tc := range []struct {
		name string
		uid  uint32
		cmd  uint32
	}{
		{name: "root known command", uid: 0, cmd: meta.RemoteDurability},
		{name: "root unknown command", uid: 0, cmd: 0xffffffff},
		{name: "untrusted known command", uid: 12345, cmd: meta.RemoteDurability},
		{name: "untrusted unknown command", uid: 12345, cmd: 0xffffffff},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := &VFS{Conf: &Config{DisableInternalCommands: true}}
			out := &bytes.Buffer{}
			v.handleInternalMsg(
				meta.NewContext(1, tc.uid, []uint32{tc.uid}), tc.cmd,
				utils.FromBuffer([]byte{1}), out,
			)
			want := []byte{byte(syscall.EACCES & 0xff)}
			if got := out.Bytes(); !bytes.Equal(got, want) {
				t.Fatalf("response = %v, want %v", got, want)
			}
		})
	}
}

func TestDefaultInternalCommandsPreserveUnknownCommandBehavior(t *testing.T) {
	v := &VFS{Conf: &Config{}}
	out := &bytes.Buffer{}
	v.handleInternalMsg(
		meta.NewContext(1, 0, []uint32{0}), 0xffffffff,
		utils.FromBuffer(nil), out,
	)
	want := []byte{byte(syscall.EINVAL & 0xff)}
	if got := out.Bytes(); !bytes.Equal(got, want) {
		t.Fatalf("default unknown command response = %v, want %v", got, want)
	}
}
