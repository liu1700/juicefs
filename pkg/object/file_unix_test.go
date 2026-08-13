//go:build !windows
// +build !windows

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
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/utils"
)

func TestLChtimesRoot(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "LChtimesTestAfile1")
	linkPath := filepath.Join(tmpDir, "LChtimesTestLink1")
	_, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("create file failed: %s", err)
	}
	err = os.Symlink(filePath, linkPath)
	if err != nil {
		t.Fatalf("symlink file failed: %s", err)
	}
	oldStat, err := os.Lstat(linkPath)
	if err != nil {
		t.Fatalf("lstat file failed: %s", err)
	}

	oldAtime := getAtime(oldStat)
	newMtime := oldStat.ModTime().Add(-time.Hour)
	root, err := os.OpenRoot(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	err = lchtimesRoot(root, filepath.Base(linkPath), time.Time{}, newMtime)
	if err != nil {
		t.Fatalf("lchtimes file failed: %s", err)
	}
	newStat, err := os.Lstat(linkPath)
	if err != nil {
		t.Fatalf("lstat file failed: %s", err)
	}
	if newStat.ModTime() != newMtime {
		t.Fatalf("mtime change failed")
	}
	newAtime := getAtime(newStat)
	if newAtime != oldAtime {
		t.Fatalf("atime change failed")
	}
}

func TestFileStoreRejectsEscapingSymlink(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "root")
	outside := filepath.Join(parent, "outside")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatal(err)
	}
	victim := filepath.Join(outside, "victim")
	if err := os.WriteFile(victim, []byte("safe"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "out")); err != nil {
		t.Fatal(err)
	}
	s, err := newDisk(root+string(filepath.Separator), "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	store := s.(*filestore)

	if _, err = store.Head(context.Background(), "out/victim"); err == nil {
		t.Error("Head followed a symlink outside the storage root")
	}
	if err = store.Put(context.Background(), "out/escaped", bytes.NewBufferString("escaped")); err == nil {
		t.Error("Put followed a symlink outside the storage root")
	}
	if err = store.Delete(context.Background(), "out/victim"); err == nil {
		t.Error("Delete followed a symlink outside the storage root")
	}
	if err = store.Chmod("out/victim", 0o666); err == nil {
		t.Error("Chmod followed a symlink outside the storage root")
	}
	if err = store.Chtimes("out/victim", time.Now().Add(-time.Hour)); err == nil {
		t.Error("Chtimes followed a symlink outside the storage root")
	}
	if err = store.Chown("out/victim", utils.UserName(os.Getuid()), utils.GroupName(os.Getgid())); err == nil {
		t.Error("Chown followed a symlink outside the storage root")
	}
	if err = store.Symlink("target", "../link"); err == nil {
		t.Error("Symlink accepted a destination outside the storage root")
	}
	if target, err := store.Readlink("out"); err != nil || target != outside {
		t.Fatalf("Readlink should preserve the symlink itself: target=%q err=%v", target, err)
	}
	if got, err := os.ReadFile(victim); err != nil || string(got) != "safe" {
		t.Fatalf("outside victim was changed: data=%q err=%v", got, err)
	}
	if _, err = os.Stat(filepath.Join(outside, "escaped")); !os.IsNotExist(err) {
		t.Fatalf("symlink allowed a write outside the root: %v", err)
	}
}

func TestFileStoreRejectsConcurrentSymlinkEscape(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "root")
	inside := filepath.Join(root, "inside")
	outside := filepath.Join(parent, "outside")
	if err := os.MkdirAll(inside, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inside, "victim"), []byte("inside"), 0o600); err != nil {
		t.Fatal(err)
	}
	outsideVictim := filepath.Join(outside, "victim")
	if err := os.WriteFile(outsideVictim, []byte("safe"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "flip")
	if err := os.Symlink(inside, link); err != nil {
		t.Fatal(err)
	}
	s, err := newDisk(root+string(filepath.Separator), "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	store := s.(*filestore)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = os.Remove(link)
			_ = os.Symlink(outside, link)
			_ = os.Remove(link)
			_ = os.Symlink(inside, link)
		}
	}()
	for i := 0; i < 200; i++ {
		_ = store.Put(context.Background(), "flip/new", bytes.NewBufferString("data"))
		_ = store.Chmod("flip/victim", 0o666)
	}
	close(stop)
	wg.Wait()

	if got, err := os.ReadFile(outsideVictim); err != nil || string(got) != "safe" {
		t.Fatalf("concurrent symlink replacement changed outside data: data=%q err=%v", got, err)
	}
	info, err := os.Stat(outsideVictim)
	if err != nil {
		t.Fatalf("stat outside victim: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("concurrent symlink replacement changed outside mode: %v", info.Mode())
	}
	if _, err := os.Stat(filepath.Join(outside, "new")); !os.IsNotExist(err) {
		t.Fatalf("concurrent symlink replacement wrote outside the root: %v", err)
	}
}

func TestFileStoreAllowsAbsoluteSymlinkWithinRoot(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "target"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "target"), filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	s, err := newDisk(root+string(filepath.Separator), "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.Get(context.Background(), "link", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil || string(got) != "data" {
		t.Fatalf("read absolute in-root symlink: data=%q err=%v", got, err)
	}
}
