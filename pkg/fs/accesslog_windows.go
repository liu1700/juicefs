//go:build windows
// +build windows

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

	"golang.org/x/sys/windows"
)

// AccessLog is an append-only Windows access log that rejects reparse points.
type AccessLog struct {
	path string
	file *os.File
}

// OpenAccessLog securely opens path for append without following a final
// reparse point.
func OpenAccessLog(path string) (*AccessLog, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	l := &AccessLog{path: abs}
	l.file, err = l.openFile(windows.OPEN_ALWAYS)
	if err != nil {
		return nil, err
	}
	return l, nil
}

func (l *AccessLog) openFile(disposition uint32) (*os.File, error) {
	p, err := windows.UTF16PtrFromString(l.path)
	if err != nil {
		return nil, err
	}
	h, err := windows.CreateFile(p,
		windows.FILE_APPEND_DATA|windows.FILE_READ_ATTRIBUTES,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_DELETE,
		nil, disposition,
		windows.FILE_ATTRIBUTE_NORMAL|windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open access log", Path: l.path, Err: err}
	}
	var info windows.ByHandleFileInformation
	if err = windows.GetFileInformationByHandle(h, &info); err != nil {
		_ = windows.CloseHandle(h)
		return nil, err
	}
	if info.FileAttributes&(windows.FILE_ATTRIBUTE_DIRECTORY|windows.FILE_ATTRIBUTE_REPARSE_POINT) != 0 {
		_ = windows.CloseHandle(h)
		return nil, fmt.Errorf("access log %q is not a regular non-reparse file", l.path)
	}
	if info.NumberOfLinks != 1 {
		_ = windows.CloseHandle(h)
		return nil, fmt.Errorf("access log %q has %d hard links; expected one", l.path, info.NumberOfLinks)
	}
	f := os.NewFile(uintptr(h), l.path)
	if f == nil {
		_ = windows.CloseHandle(h)
		return nil, fmt.Errorf("convert access log handle for %q", l.path)
	}
	// Windows has no POSIX 0600 mode. CreateFile applies the caller's default
	// ACL, and existing ACLs are preserved.
	return f, nil
}

func (l *AccessLog) Write(p []byte) (int, error) {
	return l.file.Write(p)
}

func (l *AccessLog) Close() error {
	return l.file.Close()
}

// Rotate rejects a replaced path and never truncates after rename failure.
func (l *AccessLog) Rotate(maxSize int64, count int) (bool, error) {
	current, err := l.file.Stat()
	if err != nil {
		return false, err
	}
	visible, err := os.Lstat(l.path)
	if err != nil {
		return false, err
	}
	if visible.Mode()&os.ModeSymlink != 0 || !visible.Mode().IsRegular() || !os.SameFile(current, visible) {
		return false, fmt.Errorf("visible access log %q is unsafe or was replaced", l.path)
	}
	if current.Size() <= maxSize {
		return false, nil
	}
	if count < 1 {
		count = 1
	}
	for i := count - 1; i > 0; i-- {
		oldName := fmt.Sprintf("%s.%d", l.path, i)
		newName := fmt.Sprintf("%s.%d", l.path, i+1)
		if err = os.Rename(oldName, newName); err != nil && !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
	}
	if err = os.Rename(l.path, l.path+".1"); err != nil {
		return false, err
	}
	newFile, err := l.openFile(windows.CREATE_NEW)
	if err != nil {
		return false, err
	}
	oldFile := l.file
	l.file = newFile
	return true, oldFile.Close()
}
