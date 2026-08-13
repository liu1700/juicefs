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
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestAccessLogSecureOpen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.log")
	log, err := OpenAccessLog(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = log.Write([]byte("created\n")); err != nil {
		t.Fatal(err)
	}
	if err = log.Close(); err != nil {
		t.Fatal(err)
	}
	assertAccessLogMode(t, path, 0o600)

	if err = os.Chmod(path, 0o200); err != nil {
		t.Fatal(err)
	}
	log, err = OpenAccessLog(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = log.Close(); err != nil {
		t.Fatal(err)
	}
	assertAccessLogMode(t, path, 0o200)

	if err = os.Chmod(path, 0o666); err != nil {
		t.Fatal(err)
	}
	log, err = OpenAccessLog(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = log.Close(); err != nil {
		t.Fatal(err)
	}
	assertAccessLogMode(t, path, 0o600)
}

func TestAccessLogRejectsSymlinkAndHardlink(t *testing.T) {
	dir := t.TempDir()
	victim := filepath.Join(dir, "victim")
	if err := os.WriteFile(victim, []byte("safe"), 0o600); err != nil {
		t.Fatal(err)
	}
	symlink := filepath.Join(dir, "symlink.log")
	if err := os.Symlink(victim, symlink); err != nil {
		t.Fatal(err)
	}
	if log, err := OpenAccessLog(symlink); err == nil {
		_ = log.Close()
		t.Fatal("opened an access-log symlink")
	}
	hardlink := filepath.Join(dir, "hardlink.log")
	if err := os.Link(victim, hardlink); err != nil {
		t.Fatal(err)
	}
	if log, err := OpenAccessLog(hardlink); err == nil {
		_ = log.Close()
		t.Fatal("opened a multiply-linked access log")
	}
	assertAccessLogVictim(t, victim)
}

func TestAccessLogRejectsUntrustedParentSymlink(t *testing.T) {
	base := t.TempDir()
	target := t.TempDir()
	if err := os.Chmod(base, 0o777); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "logs")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if log, err := OpenAccessLog(filepath.Join(link, "access.log")); err == nil {
		_ = log.Close()
		t.Fatal("followed an access-log parent symlink in a writable directory")
	}
	if _, err := os.Stat(filepath.Join(target, "access.log")); !os.IsNotExist(err) {
		t.Fatalf("created a log through an untrusted parent symlink: %v", err)
	}
}

func TestAccessLogRotation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "access.log")
	log, err := OpenAccessLog(path)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	want := bytes.Repeat([]byte("a"), 128)
	if _, err = log.Write(want); err != nil {
		t.Fatal(err)
	}
	rotated, err := log.Rotate(32, 3)
	if err != nil {
		t.Fatal(err)
	}
	if !rotated {
		t.Fatal("oversized access log was not rotated")
	}
	got, err := os.ReadFile(path + ".1")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("rotated data = %q, want %q", got, want)
	}
	assertAccessLogMode(t, path, 0o600)
	assertAccessLogMode(t, path+".1", 0o600)
	if _, err = log.Write([]byte("new\n")); err != nil {
		t.Fatal(err)
	}
}

func TestAccessLogRejectsPathSwapDuringRotation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.log")
	displaced := filepath.Join(dir, "open.log")
	victim := filepath.Join(dir, "victim")
	if err := os.WriteFile(victim, []byte("safe"), 0o600); err != nil {
		t.Fatal(err)
	}
	log, err := OpenAccessLog(path)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	if _, err = log.Write([]byte("before\n")); err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(path, displaced); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(victim, path); err != nil {
		t.Fatal(err)
	}
	if _, err = log.Rotate(0, 7); err == nil {
		t.Fatal("rotation accepted a swapped symlink")
	}
	if _, err = log.Write([]byte("after\n")); err != nil {
		t.Fatal(err)
	}
	assertAccessLogVictim(t, victim)
	got, err := os.ReadFile(displaced)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "before\nafter\n" {
		t.Fatalf("open log lost data after rejected rotation: %q", got)
	}
}

func TestAccessLogConcurrentSymlinkSwap(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "access.log")
	displaced := filepath.Join(dir, "displaced.log")
	victim := filepath.Join(dir, "victim")
	if err := os.WriteFile(victim, []byte("safe"), 0o600); err != nil {
		t.Fatal(err)
	}
	log, err := OpenAccessLog(path)
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	if _, err = log.Write(bytes.Repeat([]byte("x"), 128)); err != nil {
		t.Fatal(err)
	}

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
			if os.Rename(path, displaced) == nil {
				_ = os.Symlink(victim, path)
				_ = os.Remove(path)
				_ = os.Rename(displaced, path)
			}
		}
	}()
	for i := 0; i < 200; i++ {
		_, _ = log.Rotate(0, 3)
		_, _ = log.Write([]byte("x"))
	}
	close(stop)
	wg.Wait()
	assertAccessLogVictim(t, victim)
}

func assertAccessLogMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("%s mode = %o, want %o", path, got, want)
	}
}

func assertAccessLogVictim(t *testing.T, path string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "safe" {
		t.Fatalf("victim data changed: %q", got)
	}
	assertAccessLogMode(t, path, 0o600)
}
