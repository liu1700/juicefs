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

// Package restore holds the two things the per-Agent mount needs from a
// restored metadata replica that nothing else in the tree provides: proof that
// the restored database is sound and safe to replicate onward, and the
// restore-time repair for the data-plane damage an unclean generation leaves
// behind (crash-consistency.md §7 d3).
//
// It is a leaf. It never mounts, never talks to the control plane, never holds
// a lease and never runs Litestream. `juicefs plori-mount` (pkg/plori/mount)
// owns all of that and calls in here at two points; see
// docs/en/development/plori_restore.md.
//
// Litestream is deliberately absent from this package's imports. Restore runs
// through the pinned Litestream BINARY in pkg/plori/mount/litestream.go,
// because linking the library would put two independent SQLite
// implementations — modernc.org/sqlite for Litestream, mattn/go-sqlite3 for
// JuiceFS — on one database file inside one process, which POSIX advisory
// locking cannot support. Every check below therefore runs on the audited
// engine, the same one the mount uses.
package restore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	// The audited SQLite engine: mattn/go-sqlite3 built with
	// sqlite_omit_load_extension (PLORI_TAGS).
	_ "github.com/mattn/go-sqlite3"

	"github.com/juicedata/juicefs/pkg/meta"
)

// DefaultTablePrefix is the xorm table prefix a default JuiceFS SQL volume
// uses (pkg/meta/sql.go:494).
const DefaultTablePrefix = "jfs_"

// MinTrashDays is the floor the Rank 1 crash-consistency protocol needs.
const MinTrashDays = 1

// IntegrityCheck runs SQLite's own structural check over a restored database
// and reports every line the check produced, not just the first.
//
// It implements the mount.Volume.IntegrityCheck seam. Litestream's own
// restore-time check proves the LTX chain replays; this proves the page image
// it produced is a sound database, and it is the audited engine that says so.
//
// quick reduces it to PRAGMA quick_check, which skips the cross-index
// consistency pass. That is the right gate for a warm restart, never for a
// restore.
func IntegrityCheck(ctx context.Context, dbPath string, quick bool) error {
	db, err := open(dbPath)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	return integrityCheck(ctx, db, quick)
}

func integrityCheck(ctx context.Context, db *sql.DB, quick bool) error {
	pragma := "integrity_check"
	if quick {
		pragma = "quick_check"
	}
	rows, err := db.QueryContext(ctx, "PRAGMA "+pragma)
	if err != nil {
		return newError(CodeIntegrityCheckFailed, "run PRAGMA "+pragma, false, err)
	}
	defer func() { _ = rows.Close() }()

	var lines []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return newError(CodeIntegrityCheckFailed, "scan PRAGMA "+pragma, false, err)
		}
		lines = append(lines, s)
	}
	if err := rows.Err(); err != nil {
		return newError(CodeIntegrityCheckFailed, "read PRAGMA "+pragma, false, err)
	}
	if len(lines) == 1 && lines[0] == "ok" {
		return nil
	}
	// SQLite caps its own report at 100 rows. Keep the first few so the
	// kubelet event stays small and still says what is broken.
	if len(lines) > 5 {
		lines = append(lines[:5:5], fmt.Sprintf("(+%d more)", len(lines)-5))
	}
	return newError(CodeIntegrityCheckFailed,
		pragma+" reported: "+strings.Join(lines, "; "), false, nil)
}

// LoadFormat reads the JuiceFS Format straight out of the restored database.
//
// It exists so a caller can inspect a replica before anything opens it as a
// filesystem: meta.NewClient calls logger.Fatalf on a database it cannot use
// (pkg/meta/interface.go NewClient), which would take the supervisor's process
// with it, and the whole point of these checks is to refuse rather than die.
func LoadFormat(ctx context.Context, dbPath string, tablePrefix ...string) (*meta.Format, error) {
	db, err := open(dbPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	return loadFormat(ctx, db, prefixOf(tablePrefix))
}

func prefixOf(tablePrefix []string) string {
	if len(tablePrefix) > 0 && tablePrefix[0] != "" {
		return tablePrefix[0]
	}
	return DefaultTablePrefix
}

func open(dbPath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", dbPath+"?_busy_timeout=5000")
	if err != nil {
		return nil, newError(CodeIntegrityCheckFailed, "open restored database", false, err)
	}
	return db, nil
}

func loadFormat(ctx context.Context, db *sql.DB, prefix string) (*meta.Format, error) {
	var value string
	err := db.QueryRowContext(ctx,
		"SELECT value FROM "+prefix+"setting WHERE name = 'format'").Scan(&value)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, newError(CodeFormatMissing,
			"restored database has no format row in "+prefix+"setting", false, nil)
	case err != nil:
		return nil, newError(CodeFormatMissing,
			"read format from "+prefix+"setting", false, err)
	}

	format := &meta.Format{}
	if err := json.Unmarshal([]byte(value), format); err != nil {
		return nil, newError(CodeFormatMissing, "decode format", false, err)
	}
	if format.Name == "" || format.UUID == "" {
		return nil, newError(CodeFormatMissing,
			"restored format has no name or UUID", false, nil)
	}
	return format, nil
}

// CheckReplicable refuses a Format that must not be replicated onward.
//
// F-9 (threat-model.md): a Format that still carries the object-store key must
// never reach a replica prefix. The key is bucket-wide and the prefix is
// readable by anything that can read the bucket, so a credential inside it
// widens the blast radius from one mount to the whole store. JuiceFS persists
// AccessKey/SecretKey into the metadata by default (cmd/format.go), so this is
// a live regression risk on every format path, not a hypothetical one — which
// is why it is checked against the database rather than against the code that
// wrote it.
//
// TrashDays is the deletion-direction requirement: with the trash off, a
// restore that lands before a delete resurrects metadata whose blocks are
// already gone, and there is nothing left for the repair to work from.
func CheckReplicable(format *meta.Format) error {
	if format == nil {
		return newError(CodeFormatMissing, "no format to check", false, nil)
	}
	if field := credentialField(format); field != "" {
		return newError(CodeFormatCarriesCredentials, fmt.Sprintf(
			"Format field %s is set; the per-Agent profile requires a credential-free "+
				"Format with the key injected in-process from the node Secret", field), false, nil)
	}
	if format.TrashDays < MinTrashDays {
		return newError(CodeTrashDisabled, fmt.Sprintf(
			"Format has trash-days %d; the per-Agent runtime requires at least %d",
			format.TrashDays, MinTrashDays), false, nil)
	}
	return nil
}

// credentialField returns the name of the first credential field that is set,
// or "" when the Format is credential-free. The message never contains the
// value.
func credentialField(f *meta.Format) string {
	switch {
	case f.AccessKey != "":
		return "AccessKey"
	case f.SecretKey != "":
		return "SecretKey"
	case f.SessionToken != "":
		return "SessionToken"
	default:
		return ""
	}
}

// Scrub returns a copy of format safe to log or publish. The credential fields
// are already required to be empty by CheckReplicable, but a copy that cannot
// carry them is cheaper to reason about than a rule that says they will be.
func Scrub(f *meta.Format) *meta.Format {
	if f == nil {
		return nil
	}
	c := *f
	c.AccessKey = ""
	c.SecretKey = ""
	c.SessionToken = ""
	c.EncryptKey = ""
	return &c
}

// VerifyRestored is the whole pre-mount metadata gate in one call: the
// structural check, then the Format, then the policy checks that decide
// whether this database may be replicated onward.
//
// Identity is deliberately not here. The three-way match needs the MountSpec
// and a live object-store handle, and pkg/plori/mount owns both
// (Supervisor.identityMatches); a second implementation of it in this package
// would be a second thing to keep in step.
func VerifyRestored(ctx context.Context, dbPath string, quick bool, tablePrefix ...string) (*meta.Format, error) {
	db, err := open(dbPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()

	if err := integrityCheck(ctx, db, quick); err != nil {
		return nil, err
	}
	format, err := loadFormat(ctx, db, prefixOf(tablePrefix))
	if err != nil {
		return nil, err
	}
	if err := CheckReplicable(format); err != nil {
		return nil, err
	}
	return Scrub(format), nil
}
