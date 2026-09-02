//go:build !nosqlite
// +build !nosqlite

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

package meta

import (
	"path"
	"strings"
	"testing"
)

// readPragma returns the single value SQLite reports for a PRAGMA.
func readPragma(t *testing.T, m Meta, name string) string {
	t.Helper()
	db, ok := m.(*dbMeta)
	if !ok {
		t.Fatalf("meta is %T, want *dbMeta", m)
	}
	rows, err := db.db.QueryString("PRAGMA " + name)
	if err != nil {
		t.Fatalf("read pragma %s: %s", name, err)
	}
	if len(rows) != 1 {
		t.Fatalf("read pragma %s: got %d rows, want 1", name, len(rows))
	}
	for _, value := range rows[0] {
		return value
	}
	t.Fatalf("read pragma %s: no value", name)
	return ""
}

// TestSQLitePragmaContract pins the durability and contention settings a
// SQLite-backed volume is opened with. Every one of them is a default the DSN
// builder fills in, so an explicit value in the metadata URL still wins.
func TestSQLitePragmaContract(t *testing.T) {
	m, err := newSQLMeta("sqlite3", path.Join(t.TempDir(), "pragma-contract.db"), testConfig())
	if err != nil {
		t.Fatalf("create meta: %s", err)
	}
	defer func() { _ = m.Shutdown() }()

	for _, c := range []struct{ pragma, want string }{
		// WAL: readers do not block the writer, and the write-ahead log is what
		// a replicator ships.
		{"journal_mode", "wal"},
		// 1 == NORMAL. Durable across a process crash; a host power failure can
		// lose the tail of the WAL. Pinned on the DSN by newSQLMeta rather than
		// inherited from the driver's SQLITE_DEFAULT_WAL_SYNCHRONOUS build flag.
		{"synchronous", "1"},
		// 5 s, so a contended metadata transaction retries instead of failing
		// immediately with SQLITE_BUSY.
		{"busy_timeout", "5000"},
	} {
		if got := strings.ToLower(readPragma(t, m, c.pragma)); got != c.want {
			t.Errorf("PRAGMA %s = %q, want %q", c.pragma, got, c.want)
		}
	}
}

// TestSQLitePragmaOverride proves the pinned values are defaults, not a
// hard-coded policy: a metadata URL that asks for something else gets it.
func TestSQLitePragmaOverride(t *testing.T) {
	for _, c := range []struct{ query, pragma, want string }{
		{"_synchronous=FULL", "synchronous", "2"},
		{"_sync=FULL", "synchronous", "2"},
		{"_journal=TRUNCATE", "journal_mode", "truncate"},
		{"_timeout=1234", "busy_timeout", "1234"},
	} {
		t.Run(c.query, func(t *testing.T) {
			addr := path.Join(t.TempDir(), "pragma-override.db") + "?" + c.query
			m, err := newSQLMeta("sqlite3", addr, testConfig())
			if err != nil {
				t.Fatalf("create meta: %s", err)
			}
			defer func() { _ = m.Shutdown() }()
			if got := strings.ToLower(readPragma(t, m, c.pragma)); got != c.want {
				t.Errorf("with %q: PRAGMA %s = %q, want %q", c.query, c.pragma, got, c.want)
			}
		})
	}
}
