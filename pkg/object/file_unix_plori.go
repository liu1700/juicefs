//go:build !windows && plori
// +build !windows,plori

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

package object

import (
	"os"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/utils"
)

func getOwnerGroup(info os.FileInfo) (string, string) {
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		return utils.UserName(int(st.Uid)), utils.GroupName(int(st.Gid))
	}
	return "", ""
}

func (d *filestore) Chtimes(key string, mtime time.Time) error {
	r, name, err := d.openKeyRoot(key, false)
	if err != nil {
		return err
	}
	defer r.Close()
	return lchtimesRoot(r, name, time.Time{}, mtime)
}
