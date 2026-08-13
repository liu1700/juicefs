/*
 * JuiceFS, Copyright 2018 Juicedata, Inc.
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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/juicedata/juicefs/pkg/utils"
)

const (
	dirSuffix = "/"
)

var TryCFR bool // try copy_file_range
var PutInplace bool

type filestore struct {
	DefaultObjectStorage
	root string
}

func (d *filestore) Symlink(oldName, newName string) error {
	r, name, err := d.openKeyRoot(newName, true)
	if err != nil {
		return err
	}
	defer r.Close()
	if err = mkdirAllInRoot(r, filepath.Dir(name), os.FileMode(0777)); err != nil {
		return err
	}
	return r.Symlink(oldName, name)
}

func (d *filestore) Readlink(name string) (string, error) {
	r, name, err := d.openKeyRoot(name, false)
	if err != nil {
		return "", err
	}
	defer r.Close()
	return r.Readlink(name)
}

func (d *filestore) String() string {
	if runtime.GOOS == "windows" {
		return "file:///" + d.root
	}
	return "file://" + d.root
}

func (d *filestore) rootPath() string {
	if d.root == "" {
		return "."
	}
	return filepath.Clean(d.root)
}

func (d *filestore) openKeyRoot(key string, create bool) (*os.Root, string, error) {
	if strings.IndexByte(key, 0) >= 0 {
		return nil, "", fmt.Errorf("invalid file object key %q: NUL bytes are not allowed", key)
	}
	name := filepath.FromSlash(key)
	if filepath.VolumeName(name) != "" {
		return nil, "", fmt.Errorf("invalid file object key %q: volume paths are not allowed", key)
	}
	for _, component := range strings.Split(filepath.ToSlash(name), dirSuffix) {
		if component == ".." {
			return nil, "", fmt.Errorf("invalid file object key %q: parent traversal is not allowed", key)
		}
	}

	root := d.rootPath()
	if strings.HasSuffix(d.root, dirSuffix) {
		if filepath.IsAbs(name) {
			return nil, "", fmt.Errorf("invalid file object key %q: absolute paths are not allowed", key)
		}
	} else if key == "" {
		name = filepath.Base(root)
		root = filepath.Dir(root)
	} else {
		name = strings.TrimLeft(name, `/\`)
	}
	if name == "" {
		name = "."
	}
	name = filepath.Clean(name)
	if !filepath.IsLocal(name) {
		return nil, "", fmt.Errorf("invalid file object key %q: path escapes storage root", key)
	}
	if create {
		if err := os.MkdirAll(root, os.FileMode(0777)); err != nil {
			return nil, "", err
		}
	}
	r, err := os.OpenRoot(root)
	return r, name, err
}

func resolveInRoot(root *os.Root, name string) (string, error) {
	rootPath, err := filepath.Abs(root.Name())
	if err != nil {
		return "", err
	}
	rootPath, err = filepath.EvalSymlinks(rootPath)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(filepath.Join(rootPath, name))
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(rootPath, resolved)
	if err != nil || !filepath.IsLocal(rel) {
		return "", fmt.Errorf("path %q resolves to %q (relative %q), outside file storage root %q", name, resolved, rel, rootPath)
	}
	return rel, nil
}

func openInRoot(root *os.Root, name string) (*os.File, error) {
	f, err := root.Open(name)
	if err == nil {
		return f, nil
	}
	resolved, resolveErr := resolveInRoot(root, name)
	if resolveErr != nil {
		if os.IsNotExist(resolveErr) {
			return nil, resolveErr
		}
		return nil, fmt.Errorf("open %q in file storage root: %w (original error: %v)", name, resolveErr, err)
	}
	return root.Open(resolved)
}

func statInRoot(root *os.Root, name string) (os.FileInfo, error) {
	fi, err := root.Stat(name)
	if err == nil {
		return fi, nil
	}
	resolved, resolveErr := resolveInRoot(root, name)
	if resolveErr != nil {
		if os.IsNotExist(resolveErr) {
			return nil, resolveErr
		}
		return nil, fmt.Errorf("stat %q in file storage root: %w (original error: %v)", name, resolveErr, err)
	}
	return root.Stat(resolved)
}

func mkdirAllInRoot(root *os.Root, name string, mode os.FileMode) error {
	for i := 0; i < 3; i++ {
		err := root.MkdirAll(name, mode)
		if err == nil {
			return nil
		}
		if !errors.Is(err, fs.ErrExist) {
			return err
		}
	}
	return root.MkdirAll(name, mode)
}

func (d *filestore) objectKey(name string) string {
	name = filepath.ToSlash(name)
	if name == "." {
		return ""
	}
	if !strings.HasSuffix(d.root, dirSuffix) {
		return dirSuffix + name
	}
	return name
}

func (d *filestore) Head(ctx context.Context, key string) (Object, error) {
	r, name, err := d.openKeyRoot(key, false)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	fi, err := r.Lstat(name)
	if err != nil {
		return nil, err
	}
	isSymlink := fi.Mode()&os.ModeSymlink != 0
	if isSymlink {
		if key == "" && !strings.HasSuffix(d.root, dirSuffix) {
			fi, err = os.Stat(d.rootPath())
		} else {
			fi, err = statInRoot(r, name)
		}
		if err != nil {
			return nil, err
		}
	}
	return toFile(key, fi, isSymlink, getOwnerGroup), nil
}

func toFile(key string, fi fs.FileInfo, isSymlink bool, ownerGetter func(fs.FileInfo) (string, string)) *file {
	size := fi.Size()
	if fi.IsDir() {
		size = 0
	}
	owner, group := ownerGetter(fi)
	return &file{
		obj{
			key,
			size,
			fi.ModTime(),
			fi.IsDir(),
			"",
			"",
		},
		owner,
		group,
		fi.Mode(),
		isSymlink,
	}
}

type SectionReaderCloser struct {
	*io.SectionReader
	io.Closer
}

func (d *filestore) Get(ctx context.Context, key string, off, limit int64, getters ...AttrGetter) (io.ReadCloser, error) {
	r, name, err := d.openKeyRoot(key, false)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var f *os.File
	if key == "" && !strings.HasSuffix(d.root, dirSuffix) {
		f, err = os.Open(d.rootPath())
	} else {
		f, err = openInRoot(r, name)
	}
	if err != nil {
		return nil, err
	}

	finfo, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if finfo.IsDir() || off >= finfo.Size() {
		_ = f.Close()
		return io.NopCloser(bytes.NewBuffer([]byte{})), nil
	}

	if limit > 0 {
		return &SectionReaderCloser{
			SectionReader: io.NewSectionReader(f, off, limit),
			Closer:        f,
		}, nil
	}
	return f, nil
}

func (d *filestore) Put(ctx context.Context, key string, in io.Reader, getters ...AttrGetter) (err error) {
	r, name, err := d.openKeyRoot(key, true)
	if err != nil {
		return err
	}
	defer r.Close()

	if strings.HasSuffix(key, dirSuffix) || key == "" && strings.HasSuffix(d.root, dirSuffix) {
		return mkdirAllInRoot(r, name, os.FileMode(0777))
	}

	var tmp string
	var tmpCreated bool
	if PutInplace {
		tmp = name
	} else {
		base := filepath.Base(name)
		if len(base) > 200 {
			base = base[:200]
		}
		tmp = TmpFilePath(name, base)
		defer func() {
			if err != nil && tmpCreated {
				if e := r.Remove(tmp); e != nil && !os.IsNotExist(e) {
					logger.Warnf("delete %s: %s", tmp, e)
				}
			}
		}()
	}
	f, err := r.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil && os.IsNotExist(err) {
		if err := mkdirAllInRoot(r, filepath.Dir(name), os.FileMode(0777)); err != nil {
			return err
		}
		f, err = r.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	}
	if err != nil {
		return err
	}
	tmpCreated = true

	if TryCFR {
		_, err = io.Copy(f, in)
	} else {
		buf := bufPool.Get().(*[]byte)
		defer bufPool.Put(buf)
		_, err = io.CopyBuffer(onlyWriter{f}, in, *buf)
	}
	if err != nil {
		_ = f.Close()
		return err
	}
	err = f.Close()
	if err != nil {
		return err
	}
	if !PutInplace {
		err = r.Rename(tmp, name)
	}
	return err
}

func (d *filestore) Copy(ctx context.Context, dst, src string) error {
	r, err := d.Get(ctx, src, 0, -1)
	if err != nil {
		return err
	}
	defer r.Close()
	return d.Put(ctx, dst, r)
}

func (d *filestore) Delete(ctx context.Context, key string, getters ...AttrGetter) error {
	r, name, err := d.openKeyRoot(key, false)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer r.Close()
	err = r.Remove(name)
	if err != nil && os.IsNotExist(err) {
		err = nil
	}
	return err
}

type mEntry struct {
	os.FileInfo
	name      string
	fi        os.FileInfo
	isSymlink bool
}

func (m *mEntry) Name() string {
	return m.name
}

func (m *mEntry) Info() os.FileInfo {
	if m.fi != nil {
		return m.fi
	}
	return m.FileInfo
}

func (m *mEntry) IsDir() bool {
	if m.fi != nil {
		return m.fi.IsDir()
	}
	return m.FileInfo.IsDir()
}

// readDirSorted reads the directory named by dir and returns
// a sorted list of directory entries.
func readDirSorted(root *os.Root, dir string, followLink bool) ([]*mEntry, error) {
	f, err := root.Open(dir)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	entries, err := f.Readdir(-1)
	if err != nil {
		return nil, err
	}

	mEntries := make([]*mEntry, 0, len(entries))
	for _, e := range entries {
		isSymlink := e.Mode()&os.ModeSymlink != 0
		if e.IsDir() {
			mEntries = append(mEntries, &mEntry{e, e.Name() + dirSuffix, nil, false})
		} else if isSymlink && followLink {
			fi, err := statInRoot(root, filepath.Join(dir, e.Name()))
			if err != nil {
				mEntries = append(mEntries, &mEntry{e, e.Name(), nil, true})
				continue
			}
			name := e.Name()
			if fi.IsDir() {
				name = e.Name() + dirSuffix
			} else if !fi.Mode().IsRegular() {
				logger.Warnf("%s is not a regular file, ignore it", name)
				continue
			}
			mEntries = append(mEntries, &mEntry{e, name, fi, false})
		} else {
			if !isSymlink && !e.Mode().IsRegular() {
				logger.Warnf("%s is not a regular file, ignore it", e.Name())
				continue
			}
			mEntries = append(mEntries, &mEntry{e, e.Name(), nil, isSymlink})
		}
	}
	sort.Slice(mEntries, func(i, j int) bool { return mEntries[i].Name() < mEntries[j].Name() })
	return mEntries, err
}

func (d *filestore) List(ctx context.Context, prefix, marker, token, delimiter string, limit int64, followLink bool) ([]Object, bool, string, error) {
	if delimiter != "/" {
		return nil, false, "", notSupported
	}
	dir := prefix
	var objs []Object
	if prefix == "" && !strings.HasSuffix(d.root, dirSuffix) {
		r, name, err := d.openKeyRoot("", false)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, false, "", nil
			}
			return nil, false, "", err
		}
		defer r.Close()
		fi, err := r.Lstat(name)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, false, "", nil
			}
			return nil, false, "", err
		}
		isSymlink := fi.Mode()&os.ModeSymlink != 0
		if isSymlink && followLink {
			if target, statErr := os.Stat(d.rootPath()); statErr == nil {
				fi = target
				isSymlink = false
			}
		}
		key := ""
		if fi.IsDir() {
			key = dirSuffix
		}
		return generateListResult([]Object{toFile(key, fi, isSymlink, getOwnerGroup)}, limit)
	}
	includeDir := strings.HasSuffix(prefix, dirSuffix) || prefix == "" && strings.HasSuffix(d.root, dirSuffix)
	if !strings.HasSuffix(prefix, dirSuffix) {
		dir = path.Dir(prefix)
	}
	r, name, err := d.openKeyRoot(dir, false)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, "", nil
		}
		return nil, false, "", err
	}
	defer r.Close()
	if includeDir && marker == "" {
		obj, err := d.Head(ctx, prefix)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, false, "", nil
			}
			return nil, false, "", err
		}
		objs = append(objs, obj)
	}
	entries, err := readDirSorted(r, name, followLink)
	if err != nil {
		if os.IsPermission(err) {
			logger.Warnf("skip %s: %s", dir, err)
			return nil, false, "", nil
		}
		if os.IsNotExist(err) {
			logger.Debugf("skip %s: %s", dir, err)
			return nil, false, "", nil
		}
		return nil, false, "", err
	}
	for _, e := range entries {
		p := path.Join(filepath.ToSlash(name), e.Name())
		if p == "." {
			p = ""
		}
		if e.IsDir() {
			p = p + "/"
		}
		key := d.objectKey(strings.TrimSuffix(p, "/"))
		if e.IsDir() {
			key += "/"
		}
		if !strings.HasPrefix(key, prefix) || (marker != "" && key <= marker) {
			continue
		}
		info := e.Info()
		f := toFile(key, info, e.isSymlink, getOwnerGroup)
		objs = append(objs, f)
		if len(objs) == int(limit) {
			break
		}
	}
	return generateListResult(objs, limit)
}

func (d *filestore) Chmod(key string, mode os.FileMode) error {
	r, name, err := d.openKeyRoot(key, false)
	if err != nil {
		return err
	}
	defer r.Close()
	return chmodInRoot(r, name, mode)
}

func (d *filestore) Chown(key string, owner, group string) error {
	uid := utils.LookupUser(owner)
	gid := utils.LookupGroup(group)
	if uid == -1 || gid == -1 {
		return fmt.Errorf("user(%s):group(%s) not found", owner, group)
	}
	r, name, err := d.openKeyRoot(key, false)
	if err != nil {
		return err
	}
	defer r.Close()
	return r.Lchown(name, uid, gid)
}

func newDisk(root, accesskey, secretkey, token string) (ObjectStorage, error) {
	// For Windows, the path looks like /C:/a/b/c/
	if runtime.GOOS == "windows" {
		root = strings.TrimPrefix(root, "/")
	}
	return &filestore{root: root}, nil
}
