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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// nolint:unused
func getAtime(fi os.FileInfo) time.Time {
	if sst, ok := fi.Sys().(*syscall.Stat_t); ok {
		return time.Unix(sst.Atim.Unix())
	}
	return fi.ModTime()
}

func lchtimes(name string, atime time.Time, mtime time.Time) error {
	var ts = make([]unix.Timespec, 2)
	// only change mtime
	ts[0] = unix.Timespec{Sec: unix.UTIME_OMIT, Nsec: unix.UTIME_OMIT}
	ts[1] = unix.NsecToTimespec(mtime.UnixNano())

	if e := unix.UtimesNanoAt(unix.AT_FDCWD, name, ts, unix.AT_SYMLINK_NOFOLLOW); e != nil {
		return &os.PathError{Op: "lchtimes", Path: name, Err: e}
	}
	return nil
}

func lchtimesRoot(root *os.Root, name string, atime time.Time, mtime time.Time) error {
	dir, err := root.Open(filepath.Dir(name))
	if err != nil {
		return err
	}
	defer dir.Close()
	var ts = []unix.Timespec{
		{Sec: unix.UTIME_OMIT, Nsec: unix.UTIME_OMIT},
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
	base, err := root.Open(".")
	if err != nil {
		return err
	}
	defer base.Close()
	fd, err := unix.Openat2(int(base.Fd()), resolved, &unix.OpenHow{
		Flags:   unix.O_PATH | unix.O_CLOEXEC,
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_MAGICLINKS,
	})
	if err != nil {
		// openat2 was added in Linux 5.6. Root.Chmod still confines the
		// operation on older kernels, although it cannot pin the target.
		if errors.Is(err, unix.ENOSYS) {
			return root.Chmod(resolved, mode)
		}
		return &os.PathError{Op: "chmod", Path: name, Err: err}
	}
	defer unix.Close(fd)
	if err = unix.Fchmodat(fd, "", uint32(mode.Perm()), unix.AT_EMPTY_PATH); err == nil {
		return nil
	} else if err != unix.EOPNOTSUPP {
		return &os.PathError{Op: "chmod", Path: name, Err: err}
	}
	if err = os.Chmod(fmt.Sprintf("/proc/self/fd/%d", fd), mode.Perm()); err != nil {
		return fmt.Errorf("chmod %q through pinned descriptor: %w", name, err)
	}
	return nil
}
