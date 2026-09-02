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

package vfs

import (
	"syscall"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
)

// The Plori distribution mounts one filesystem per Agent and the Agent's
// processes run inside that mount. `.control` is a virtual file in the
// filesystem root, so those processes can open it; upstream, only
// `meta.RemoteDurability` checks the caller's uid, and every other internal
// command — `rmr`, `info`, `fill`, the quota and the debug commands — is
// reachable by anyone who can write the file.
//
// threat-model.md F-7 is that gap. The mitigation is to move the check from
// one command to the dispatcher: an internal command is an operator action,
// and the only callers that may perform one are root and the uid the mount
// itself runs as.
func init() {
	internalMsgGate = func(ctx meta.Context, cmd uint32) syscall.Errno {
		if ctx.Uid() == 0 || ctx.Uid() == uint32(utils.GetCurrentUID()) {
			return 0
		}
		return syscall.EACCES
	}
}
