/*
 * JuiceFS, Copyright 2025 Juicedata, Inc.
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
	"path/filepath"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// nolint:unused
func getAtime(fi os.FileInfo) time.Time {
	if sst, ok := fi.Sys().(*syscall.Stat_t); ok {
		return time.Unix(sst.Atimespec.Unix())
	} else {
		return fi.ModTime()
	}
}

func lchtimesRoot(root *os.Root, name string, atime time.Time, mtime time.Time) error {
	dir, err := root.Open(filepath.Dir(name))
	if err != nil {
		return err
	}
	defer dir.Close()
	var ts = []unix.Timespec{
		{Sec: -2, Nsec: -2},
		unix.NsecToTimespec(mtime.UnixNano()),
	}
	if e := unix.UtimesNanoAt(int(dir.Fd()), filepath.Base(name), ts, unix.AT_SYMLINK_NOFOLLOW); e != nil {
		return &os.PathError{Op: "lchtimes", Path: name, Err: e}
	}
	return nil
}

func chmodInRoot(root *os.Root, name string, mode os.FileMode) error {
	resolved, err := resolveInRoot(root, name)
	if err != nil {
		return err
	}
	return root.Chmod(resolved, mode)
}
