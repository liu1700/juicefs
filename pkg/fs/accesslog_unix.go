//go:build !windows
// +build !windows

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

package fs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

const maxAccessLogSymlinks = 40

// AccessLog keeps both the log and its parent directory open so rotation does
// not need to resolve an attacker-replaceable pathname again.
type AccessLog struct {
	path string
	name string
	dir  *os.File
	file *os.File
}

// OpenAccessLog securely opens path for append. New and overly permissive logs
// are owner-only; an existing owner-only mode is preserved.
func OpenAccessLog(path string) (*AccessLog, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	name := filepath.Base(abs)
	if name == "." || name == string(filepath.Separator) {
		return nil, fmt.Errorf("access log path %q does not name a file", path)
	}
	dir, err := openAccessLogDir(filepath.Dir(abs))
	if err != nil {
		return nil, fmt.Errorf("open access log parent for %q: %w", path, err)
	}
	l := &AccessLog{path: abs, name: name, dir: dir}
	file, err := l.openFile(true)
	if err != nil {
		_ = dir.Close()
		return nil, err
	}
	l.file = file
	return l, nil
}

func openAccessLogDir(path string) (*os.File, error) {
	path = filepath.Clean(path)
	if !filepath.IsAbs(path) {
		return nil, fmt.Errorf("access log parent must be absolute: %q", path)
	}
	components := splitAccessLogPath(path)
	dir, err := os.Open(string(filepath.Separator))
	if err != nil {
		return nil, err
	}
	current := string(filepath.Separator)
	symlinks := 0
	for len(components) > 0 {
		component := components[0]
		components = components[1:]
		fd, openErr := unix.Openat(int(dir.Fd()), component,
			unix.O_RDONLY|unix.O_CLOEXEC|unix.O_DIRECTORY|unix.O_NOFOLLOW, 0)
		if openErr == nil {
			next := os.NewFile(uintptr(fd), filepath.Join(current, component))
			if next == nil {
				_ = unix.Close(fd)
				_ = dir.Close()
				return nil, errors.New("convert access log directory descriptor")
			}
			_ = dir.Close()
			dir = next
			current = filepath.Join(current, component)
			continue
		}

		var lst unix.Stat_t
		if statErr := unix.Fstatat(int(dir.Fd()), component, &lst, unix.AT_SYMLINK_NOFOLLOW); statErr != nil || lst.Mode&unix.S_IFMT != unix.S_IFLNK {
			_ = dir.Close()
			return nil, &os.PathError{Op: "openat", Path: filepath.Join(current, component), Err: openErr}
		}
		var parent unix.Stat_t
		if err = unix.Fstat(int(dir.Fd()), &parent); err != nil {
			_ = dir.Close()
			return nil, err
		}
		if (parent.Uid != 0 && parent.Uid != uint32(os.Geteuid())) || parent.Mode&0o022 != 0 {
			_ = dir.Close()
			return nil, fmt.Errorf("refuse access log parent symlink %q in an untrusted directory", filepath.Join(current, component))
		}
		symlinks++
		if symlinks > maxAccessLogSymlinks {
			_ = dir.Close()
			return nil, fmt.Errorf("too many symlinks in access log parent %q", path)
		}
		target, err := readlinkAt(int(dir.Fd()), component)
		if err != nil {
			_ = dir.Close()
			return nil, err
		}
		remaining := filepath.Join(components...)
		var resolved string
		if filepath.IsAbs(target) {
			resolved = filepath.Join(target, remaining)
		} else {
			resolved = filepath.Join(current, target, remaining)
		}
		components = splitAccessLogPath(filepath.Clean(resolved))
		_ = dir.Close()
		dir, err = os.Open(string(filepath.Separator))
		if err != nil {
			return nil, err
		}
		current = string(filepath.Separator)
	}
	return dir, nil
}

func splitAccessLogPath(path string) []string {
	return strings.FieldsFunc(filepath.ToSlash(path), func(r rune) bool { return r == '/' })
}

func readlinkAt(dirfd int, name string) (string, error) {
	for size := 128; size <= 64<<10; size *= 2 {
		buf := make([]byte, size)
		n, err := unix.Readlinkat(dirfd, name, buf)
		if err != nil {
			return "", err
		}
		if n < len(buf) {
			return string(buf[:n]), nil
		}
	}
	return "", fmt.Errorf("access log parent symlink %q is too long", name)
}

func (l *AccessLog) openFile(allowExisting bool) (*os.File, error) {
	flags := unix.O_WRONLY | unix.O_APPEND | unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_CREAT | unix.O_EXCL
	fd, err := unix.Openat(int(l.dir.Fd()), l.name, flags, 0o600)
	created := err == nil
	if errors.Is(err, unix.EEXIST) && allowExisting {
		fd, err = unix.Openat(int(l.dir.Fd()), l.name,
			unix.O_WRONLY|unix.O_APPEND|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	}
	if err != nil {
		return nil, &os.PathError{Op: "open access log", Path: l.path, Err: err}
	}
	f := os.NewFile(uintptr(fd), l.path)
	if f == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("convert access log descriptor for %q", l.path)
	}
	if err = validateAccessLogFile(f); err != nil {
		_ = f.Close()
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	perm := info.Mode().Perm()
	if created || perm&0o077 != 0 || perm&0o100 != 0 {
		if err = f.Chmod(0o600); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("secure access log mode for %q: %w", l.path, err)
		}
	}
	return f, nil
}

func validateAccessLogFile(f *os.File) error {
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("access log %q is not a regular file", f.Name())
	}
	var stat unix.Stat_t
	if err = unix.Fstat(int(f.Fd()), &stat); err != nil {
		return err
	}
	if stat.Nlink != 1 {
		return fmt.Errorf("access log %q has %d hard links; expected one", f.Name(), stat.Nlink)
	}
	if stat.Uid != uint32(os.Geteuid()) {
		return fmt.Errorf("access log %q is owned by uid %d, process uid is %d", f.Name(), stat.Uid, os.Geteuid())
	}
	return nil
}

// Write appends to the currently open access log.
func (l *AccessLog) Write(p []byte) (int, error) {
	return l.file.Write(p)
}

// Close closes the log and its pinned parent directory.
func (l *AccessLog) Close() error {
	fileErr := l.file.Close()
	dirErr := l.dir.Close()
	return errors.Join(fileErr, dirErr)
}

// Rotate validates the visible path on every call and rotates it once maxSize
// is exceeded. Rotation never truncates a file after a rename failure.
func (l *AccessLog) Rotate(maxSize int64, count int) (bool, error) {
	current, err := l.file.Stat()
	if err != nil {
		return false, err
	}
	if err = l.validatePath(); err != nil {
		return false, err
	}
	if current.Size() <= maxSize {
		return false, nil
	}
	if count < 1 {
		count = 1
	}
	for i := count - 1; i > 0; i-- {
		oldName := fmt.Sprintf("%s.%d", l.name, i)
		newName := fmt.Sprintf("%s.%d", l.name, i+1)
		if err = unix.Renameat(int(l.dir.Fd()), oldName, int(l.dir.Fd()), newName); err != nil && !errors.Is(err, unix.ENOENT) {
			return false, fmt.Errorf("rotate access log %q to %q: %w", oldName, newName, err)
		}
	}
	first := l.name + ".1"
	if err = unix.Renameat(int(l.dir.Fd()), l.name, int(l.dir.Fd()), first); err != nil {
		return false, fmt.Errorf("rotate access log %q: %w", l.path, err)
	}
	newFile, err := l.openFile(false)
	if err != nil {
		return false, fmt.Errorf("open new access log after rotating %q: %w", l.path, err)
	}
	oldFile := l.file
	l.file = newFile
	if err = oldFile.Close(); err != nil {
		return true, fmt.Errorf("close rotated access log %q: %w", l.path, err)
	}
	return true, nil
}

func (l *AccessLog) validatePath() error {
	parent, err := os.Stat(filepath.Dir(l.path))
	if err != nil {
		return fmt.Errorf("stat access log parent %q: %w", filepath.Dir(l.path), err)
	}
	pinned, err := l.dir.Stat()
	if err != nil {
		return err
	}
	if !os.SameFile(parent, pinned) {
		return fmt.Errorf("access log parent %q was replaced", filepath.Dir(l.path))
	}
	var stat unix.Stat_t
	if err = unix.Fstatat(int(l.dir.Fd()), l.name, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return fmt.Errorf("stat visible access log %q: %w", l.path, err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG {
		return fmt.Errorf("visible access log %q is not a regular file", l.path)
	}
	if stat.Nlink != 1 || stat.Uid != uint32(os.Geteuid()) {
		return fmt.Errorf("visible access log %q has unsafe ownership or link count", l.path)
	}
	var opened unix.Stat_t
	if err = unix.Fstat(int(l.file.Fd()), &opened); err != nil {
		return err
	}
	if stat.Dev != opened.Dev || stat.Ino != opened.Ino {
		return fmt.Errorf("visible access log %q no longer refers to the open file", l.path)
	}
	return nil
}
