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

package restore

import (
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
)

// rewriteFormat replaces the persisted Format so a test can model a database
// written by an older or misconfigured writer.
func rewriteFormat(t *testing.T, dbPath string, mutate func(*meta.Format)) {
	t.Helper()

	db, err := sql.Open("sqlite3", dbPath+"?_busy_timeout=5000")
	if err != nil {
		t.Fatalf("open %s: %v", dbPath, err)
	}
	defer func() { _ = db.Close() }()

	var raw string
	if err := db.QueryRow("SELECT value FROM jfs_setting WHERE name = 'format'").Scan(&raw); err != nil {
		t.Fatalf("read format: %v", err)
	}
	var f meta.Format
	if err := json.Unmarshal([]byte(raw), &f); err != nil {
		t.Fatalf("decode format: %v", err)
	}
	mutate(&f)
	out, err := json.Marshal(&f)
	if err != nil {
		t.Fatalf("encode format: %v", err)
	}
	if _, err := db.Exec("UPDATE jfs_setting SET value = ? WHERE name = 'format'", string(out)); err != nil {
		t.Fatalf("write format: %v", err)
	}
}

// TestVerifyRejectsFormatWithCredentials is threat-model F-9. A Format that
// still holds the object-store key must never reach a replica prefix, and the
// refusal must not quote the value it found.
func TestVerifyRejectsFormatWithCredentials(t *testing.T) {
	secrets := map[string]string{
		"AccessKey":    "AKIAEXAMPLE",
		"SecretKey":    "s3cr3tvalue",
		"SessionToken": "tokenvalue",
	}
	for field, secret := range secrets {
		t.Run(field, func(t *testing.T) {
			v := newVolume(t, volumeOptions{trashDays: 1, files: map[string]int{"/f": 16}})
			rewriteFormat(t, v.metaPath, func(f *meta.Format) {
				switch field {
				case "AccessKey":
					f.AccessKey = secret
				case "SecretKey":
					f.SecretKey = secret
				case "SessionToken":
					f.SessionToken = secret
				}
			})
			_, err := VerifyRestored(t.Context(), v.metaPath, false)
			if !errors.Is(err, ErrFormatCarriesCredentials) {
				t.Fatalf("got %v, want %s", err, CodeFormatCarriesCredentials)
			}
			if strings.Contains(err.Error(), secret) {
				t.Fatalf("the refusal leaked the credential: %v", err)
			}
		})
	}
}

// TestVerifyRejectsTrashDisabled is the deletion-direction requirement: the
// Rank 1 protocol only works while deleted slices survive a restore window.
func TestVerifyRejectsTrashDisabled(t *testing.T) {
	v := newVolume(t, volumeOptions{trashDays: 0, files: map[string]int{"/f": 16}})
	_, err := VerifyRestored(t.Context(), v.metaPath, false)
	if !errors.Is(err, ErrTrashDisabled) {
		t.Fatalf("got %v, want %s", err, CodeTrashDisabled)
	}
}

func TestVerifyAcceptsAHealthyVolume(t *testing.T) {
	v := newVolume(t, volumeOptions{trashDays: 1, files: map[string]int{"/a/f": 4096}})
	format, err := VerifyRestored(t.Context(), v.metaPath, false)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if format.UUID != v.format.UUID || format.Name != v.format.Name {
		t.Fatalf("format = %+v, want name %q uuid %q", format, v.format.Name, v.format.UUID)
	}
	if format.AccessKey != "" || format.SecretKey != "" ||
		format.SessionToken != "" || format.EncryptKey != "" {
		t.Fatalf("VerifyRestored returned a Format with credential fields set: %+v", format)
	}
}

// TestIntegrityCheckReportsEveryLine is the difference from a single-row
// QueryRow: a corrupt database usually reports several problems and the first
// one alone is rarely the actionable one.
func TestIntegrityCheckRejectsCorruptDatabase(t *testing.T) {
	v := newVolume(t, volumeOptions{trashDays: 1, files: map[string]int{"/a/f": 4096, "/a/g": 4096}})

	f, err := os.OpenFile(v.metaPath, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	// Scribble over the middle of the file. Page 1 is the header SQLite checks
	// on open; damaging a b-tree page instead is what integrity_check is for.
	junk := make([]byte, 2048)
	for i := range junk {
		junk[i] = 0xA5
	}
	if _, err := f.WriteAt(junk, info.Size()/2); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if err := IntegrityCheck(t.Context(), v.metaPath, false); !errors.Is(err, ErrIntegrityCheckFailed) {
		t.Fatalf("got %v, want %s", err, CodeIntegrityCheckFailed)
	}
	if _, err := VerifyRestored(t.Context(), v.metaPath, false); !errors.Is(err, ErrIntegrityCheckFailed) {
		t.Fatalf("VerifyRestored got %v, want %s", err, CodeIntegrityCheckFailed)
	}
}

func TestIntegrityCheckAcceptsAHealthyDatabase(t *testing.T) {
	v := newVolume(t, volumeOptions{trashDays: 1, files: map[string]int{"/f": 4096}})
	if err := IntegrityCheck(t.Context(), v.metaPath, false); err != nil {
		t.Fatalf("integrity_check: %v", err)
	}
	if err := IntegrityCheck(t.Context(), v.metaPath, true); err != nil {
		t.Fatalf("quick_check: %v", err)
	}
}

func TestLoadFormatRejectsDatabaseWithoutFormat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "blank.db")
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE jfs_setting (name TEXT PRIMARY KEY, value TEXT)"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := LoadFormat(t.Context(), path); !errors.Is(err, ErrFormatMissing) {
		t.Fatalf("got %v, want %s", err, CodeFormatMissing)
	}
}

func TestScrubIsANoopOnNil(t *testing.T) {
	if Scrub(nil) != nil {
		t.Fatal("Scrub(nil) should be nil")
	}
}
