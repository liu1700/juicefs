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

package mount

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A durable point names a transaction and an instant. These tests are about
// which of the two the worker actually restores to, and what it does when the
// transaction is out of reach (PLO-396).

// fakeLitestream writes a stand-in for the pinned binary. It records every
// argv it is called with, one line per call, and behaves as `body` says.
func fakeLitestream(t *testing.T, body string) (bin, argvLog string) {
	t.Helper()
	dir := t.TempDir()
	bin = filepath.Join(dir, "litestream")
	argvLog = filepath.Join(dir, "argv.log")
	script := "#!/bin/sh\n" +
		"printf '%s\\n' \"$*\" >> " + argvLog + "\n" +
		"out=\"\"; prev=\"\"\n" +
		"for a in \"$@\"; do if [ \"$prev\" = \"-o\" ]; then out=\"$a\"; fi; prev=\"$a\"; done\n" +
		body + "\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin, argvLog
}

const fakeRestoreOK = ": > \"$out\"\nexit 0"

func calls(t *testing.T, argvLog string) []string {
	t.Helper()
	data, err := os.ReadFile(argvLog)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSpace(string(data)), "\n")
}

// newTestLitestream returns a Litestream whose config has already been
// rendered, because Restore rewrites that config for the source prefix and
// cannot do it from nothing.
func newTestLitestream(t *testing.T, bin string) *Litestream {
	t.Helper()
	dir := t.TempDir()
	ls := &Litestream{
		Bin:        bin,
		ConfigPath: filepath.Join(dir, "litestream.yml"),
		SocketPath: filepath.Join(dir, "litestream.sock"),
		DBPath:     filepath.Join(dir, "meta.db"),
	}
	spec := &MountSpec{
		MetaPrefix: "agents-meta/v1/g3/",
		ObjectStore: ObjectStore{
			Endpoint: "https://plorifs.lax1.vultrobjects.com",
			Bucket:   "plorifs",
			Region:   "lax1",
		},
	}
	if err := ls.WriteConfig(spec, ParseMountOptions(nil)); err != nil {
		t.Fatalf("WriteConfig: %v", err)
	}
	return ls
}

func TestRestorePassesTheTXIDAndNotTheTimestamp(t *testing.T) {
	bin, argvLog := fakeLitestream(t, fakeRestoreOK)
	ls := newTestLitestream(t, bin)
	anchor := time.Date(2026, 9, 2, 17, 55, 30, 123456789, time.UTC)

	if err := ls.Restore(context.Background(), "agents-meta/v1/g2/", RestoreOptions{
		TXID: "000000000000007b", Timestamp: anchor,
	}); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	got := calls(t, argvLog)
	if len(got) != 1 {
		t.Fatalf("want one restore, got %d: %v", len(got), got)
	}
	if !strings.Contains(got[0], "-txid 000000000000007b") {
		t.Errorf("restore did not carry the durable point's txid: %s", got[0])
	}
	// v0.5.17 refuses both anchors in one invocation ("cannot specify index &
	// timestamp to restore", replica.go:612), so preferring the TXID means the
	// timestamp is not sent at all.
	if strings.Contains(got[0], "-timestamp") {
		t.Errorf("restore sent both anchors, which litestream rejects: %s", got[0])
	}
	if strings.Contains(got[0], "-if-replica-exists") {
		t.Errorf("an anchored restore must never be an empty-replica probe: %s", got[0])
	}
}

func TestRestoreFallsBackToTheTimestampAtFullPrecision(t *testing.T) {
	bin, argvLog := fakeLitestream(t, fakeRestoreOK)
	ls := newTestLitestream(t, bin)
	// A durable point recorded before the fork read a TXID at all.
	anchor := time.Date(2026, 9, 2, 17, 55, 30, 123456789, time.UTC)

	if err := ls.Restore(context.Background(), "agents-meta/v1/g2/", RestoreOptions{Timestamp: anchor}); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	got := calls(t, argvLog)
	if len(got) != 1 {
		t.Fatalf("want one restore, got %d: %v", len(got), got)
	}
	// The sub-second part is the point: Format(time.RFC3339) truncates it, and
	// a truncated T_before excludes every LTX file encoded earlier in the same
	// second — measured as one lost row in the fixture below.
	if !strings.Contains(got[0], "-timestamp 2026-09-02T17:55:30.123456789Z") {
		t.Errorf("timestamp anchor lost precision: %s", got[0])
	}
	if strings.Contains(got[0], "-txid") {
		t.Errorf("no txid was available, yet one was sent: %s", got[0])
	}
}

func TestRestoreWithNoAnchorProbesForAnEmptyReplica(t *testing.T) {
	bin, argvLog := fakeLitestream(t, fakeRestoreOK)
	ls := newTestLitestream(t, bin)

	if err := ls.Restore(context.Background(), "agents-meta/v1/g2/", RestoreOptions{}); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	got := calls(t, argvLog)
	if len(got) != 1 || !strings.Contains(got[0], "-if-replica-exists") {
		t.Fatalf("want a single empty-replica probe, got %v", got)
	}
	if strings.Contains(got[0], "-txid") || strings.Contains(got[0], "-timestamp") {
		t.Errorf("the probe must carry no anchor: %s", got[0])
	}
}

// A durable point is machine-written. A txid that is not one is a broken
// contract somewhere upstream, and quietly restoring something else is how
// this field spent its whole life being ignored.
func TestRestoreRefusesAnUnparseableTXID(t *testing.T) {
	bin, argvLog := fakeLitestream(t, fakeRestoreOK)
	ls := newTestLitestream(t, bin)

	err := ls.Restore(context.Background(), "agents-meta/v1/g2/", RestoreOptions{
		TXID: "123", Timestamp: time.Now(),
	})
	if err == nil {
		t.Fatal("want an error for a malformed txid")
	}
	if !strings.Contains(err.Error(), "16 hex digits") {
		t.Errorf("error does not say what is wrong: %v", err)
	}
	if got := calls(t, argvLog); len(got) != 0 {
		t.Errorf("litestream was run with a malformed anchor: %v", got)
	}
}

// Compaction merges the L0 files a recorded TXID was the boundary of into one
// file that straddles it; `l0-retention` then deletes the originals. From that
// moment `-txid` on the swallowed value fails permanently — reproduced against
// the real binary — while the data itself is still there.
func TestRestoreRetriesTheTimestampWhenTheTXIDIsUnreachable(t *testing.T) {
	bin, argvLog := fakeLitestream(t, `case "$*" in
  *-txid*) echo "Error: no matching backup files available" >&2; exit 1;;
esac
: > "$out"
exit 0`)
	ls := newTestLitestream(t, bin)
	var events []string
	ls.Log = func(event string, _ ...any) { events = append(events, event) }
	anchor := time.Date(2026, 9, 2, 17, 55, 30, 123456789, time.UTC)

	if err := ls.Restore(context.Background(), "agents-meta/v1/g2/", RestoreOptions{
		TXID: "000000000000007b", Timestamp: anchor,
	}); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	got := calls(t, argvLog)
	if len(got) != 2 {
		t.Fatalf("want the txid attempt and then the timestamp, got %v", got)
	}
	if !strings.Contains(got[0], "-txid") {
		t.Errorf("first attempt was not the txid: %s", got[0])
	}
	if !strings.Contains(got[1], "-timestamp 2026-09-02T17:55:30.123456789Z") || strings.Contains(got[1], "-txid") {
		t.Errorf("second attempt is not the timestamp of the same point: %s", got[1])
	}
	if len(events) != 1 || events[0] != "restore_txid_unreachable" {
		t.Errorf("a silent fallback is indistinguishable from a working anchor; events = %v", events)
	}
}

func TestRestoreKeepsTheFailureWhenThereIsNoTimestampToFallBackTo(t *testing.T) {
	bin, argvLog := fakeLitestream(t, `echo "Error: no matching backup files available" >&2; exit 1`)
	ls := newTestLitestream(t, bin)

	err := ls.Restore(context.Background(), "agents-meta/v1/g2/", RestoreOptions{TXID: "000000000000007b"})
	if err == nil {
		t.Fatal("want the restore failure to surface")
	}
	if !strings.Contains(err.Error(), errTxUnreachable) {
		t.Errorf("error lost the reason: %v", err)
	}
	if got := calls(t, argvLog); len(got) != 1 {
		t.Errorf("want exactly one attempt, got %v", got)
	}
}

// Any other failure is not a stale anchor and must not be retried at an older
// point: an object store that refused the credential would come back as a
// silently older filesystem.
func TestRestoreDoesNotRetryOtherFailures(t *testing.T) {
	bin, argvLog := fakeLitestream(t, `echo "Error: AccessDenied" >&2; exit 1`)
	ls := newTestLitestream(t, bin)

	err := ls.Restore(context.Background(), "agents-meta/v1/g2/", RestoreOptions{
		TXID: "000000000000007b", Timestamp: time.Now(),
	})
	if err == nil {
		t.Fatal("want the restore failure to surface")
	}
	if got := calls(t, argvLog); len(got) != 1 {
		t.Errorf("want exactly one attempt, got %v", got)
	}
}

// TxID decodes v0.5.17's `POST /sync` body, where both ids are JSON NUMBERS
// (server.go:566-571). Decoding `txid` into a string — what this file's
// syncResponse did until PLO-396 — is a decode error, and every durable point
// went to the control-plane with no transaction id at all.
func TestTxIDReadsTheReplicatedPositionAsHex(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{"replicated", `{"status":"synced","path":"/x","txid":123,"replicated_txid":123}`, "000000000000007b"},
		{"local ahead of the replica", `{"status":"synced","path":"/x","txid":200,"replicated_txid":123}`, "000000000000007b"},
		{"nothing replicated yet", `{"status":"no_change","path":"/x","txid":0,"replicated_txid":0}`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A short socket path on purpose: the sun_path limit is 104 bytes
			// on macOS, and t.TempDir() plus a test name of this length is
			// already over it.
			dir, err := os.MkdirTemp("", "plo396")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dir)
			sock := filepath.Join(dir, "ls.sock")
			ln, err := net.Listen("unix", sock)
			if err != nil {
				t.Fatal(err)
			}
			srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/sync" {
					t.Errorf("unexpected route %s", r.URL.Path)
				}
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, tc.body)
			})}
			go func() { _ = srv.Serve(ln) }()
			defer srv.Close()

			ls := &Litestream{SocketPath: sock, DBPath: filepath.Join(dir, "meta.db")}
			got, err := ls.TxID(context.Background())
			if err != nil {
				t.Fatalf("TxID: %v", err)
			}
			if got != tc.want {
				t.Errorf("TxID = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestRestoreByTXIDMatchesTheDurablePointWhereATimestampDoesNot is the
// byte-level invariant, run against the REAL pinned Litestream and a real
// SQLite database rather than a fake.
//
// The fixture is the crash shape: a transaction commits, `T_before` is
// captured (what runBarrier does), and the LTX file carrying that transaction
// is encoded afterwards — which is the normal case, because encoding follows
// the commit by up to one sync interval. Restoring to `T_before` then drops
// the transaction, because `-timestamp` takes a file iff `CreatedAt < T` and
// the file's stamp is its encode moment (litestream db.go:2141,
// replica.go:1673). Restoring to the recorded TXID does not.
//
// `PRAGMA data_version` is deliberately not the oracle: measured on both
// restores of this fixture it reads 2 either way, because it counts changes
// this connection did not make rather than identifying content. The row digest
// is the oracle.
func TestRestoreByTXIDMatchesTheDurablePointWhereATimestampDoesNot(t *testing.T) {
	bin := os.Getenv("LITESTREAM_BIN")
	if bin == "" {
		var err error
		if bin, err = exec.LookPath("litestream"); err != nil {
			t.Skip("no litestream binary: set LITESTREAM_BIN or put v0.5.17 on PATH")
		}
	}
	sqlite, err := exec.LookPath("sqlite3")
	if err != nil {
		t.Skip("no sqlite3 CLI to build the fixture with")
	}

	// No control socket in this fixture — restore is a one-shot command — so
	// the usual temp dir is fine here.
	root := t.TempDir()
	dbPath := filepath.Join(root, "meta.db")
	replica := filepath.Join(root, "replica")
	cfgPath := filepath.Join(root, "litestream.yml")
	cfg := fmt.Sprintf(`socket:
  enabled: false
levels:
  - interval: 10m
snapshot:
  interval: 24h
l0-retention: 30m
dbs:
  - path: %s
    replica:
      type: file
      path: %s
      sync-interval: 200ms
`, dbPath, replica)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}

	sql := func(stmt string) string {
		t.Helper()
		out, err := exec.Command(sqlite, dbPath, stmt).CombinedOutput()
		if err != nil {
			t.Fatalf("sqlite3 %q: %v: %s", stmt, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	replicateUntil := func(want string) {
		t.Helper()
		cmd := exec.Command(bin, "replicate", "-config", cfgPath)
		cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatalf("start litestream: %v", err)
		}
		deadline := time.Now().Add(30 * time.Second)
		for {
			matches, _ := filepath.Glob(filepath.Join(replica, "ltx", "0", want+".ltx"))
			if len(matches) > 0 {
				break
			}
			if time.Now().After(deadline) {
				_ = cmd.Process.Kill()
				t.Fatalf("litestream never replicated %s", want)
			}
			time.Sleep(50 * time.Millisecond)
		}
		// SIGTERM makes replicate run its shutdown sync, exactly as the stop
		// sequence relies on.
		_ = cmd.Process.Signal(os.Interrupt)
		_ = cmd.Wait()
	}

	sql("PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY); INSERT INTO t VALUES (0);")
	replicateUntil("0000000000000001-0000000000000001")

	// The last transaction before the barrier, and the instant the supervisor
	// records for it. Replication of it happens afterwards — which is the
	// whole point.
	sql("INSERT INTO t VALUES (1);")
	tBefore := time.Now().UTC()
	wantDigest := sql("SELECT group_concat(id) FROM t ORDER BY id;")
	time.Sleep(50 * time.Millisecond)
	replicateUntil("0000000000000002-0000000000000002")
	const durablePointTxID = "0000000000000002"

	restore := func(name, txid string, ts time.Time) string {
		t.Helper()
		out := filepath.Join(root, name+".db")
		args := restoreArgs(cfgPath, dbPath, out, txid, ts)
		if b, err := exec.Command(bin, args...).CombinedOutput(); err != nil {
			t.Fatalf("litestream %v: %v: %s", args, err, b)
		}
		b, err := exec.Command(sqlite, out, "SELECT group_concat(id) FROM t ORDER BY id;").CombinedOutput()
		if err != nil {
			t.Fatalf("read restored db: %v: %s", err, b)
		}
		return strings.TrimSpace(string(b))
	}

	byTXID := restore("by-txid", durablePointTxID, time.Time{})
	byTimestamp := restore("by-timestamp", "", tBefore)

	if byTXID != wantDigest {
		t.Errorf("restore by TXID = %q, want the durable point's %q", byTXID, wantDigest)
	}
	if byTimestamp == wantDigest {
		t.Skipf("timestamp and TXID agreed here (%q): the LTX file happened to be encoded before T_before, which is the race this issue exists to remove", byTimestamp)
	}
	t.Logf("divergence: by-txid=%q by-timestamp=%q (durable point = %q)", byTXID, byTimestamp, wantDigest)
}
