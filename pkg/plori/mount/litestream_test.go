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
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The rendered config is asserted verbatim. It is the file a pinned external
// binary parses, so a silent shape change here is a mount that replicates
// nowhere; CI additionally parses it with the real litestream.
func TestLitestreamConfigRendersTheWave2Defaults(t *testing.T) {
	dir := t.TempDir()
	ls := &Litestream{
		Bin:        "litestream",
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
	data, err := os.ReadFile(ls.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	got := string(data)

	// PLO-316 wave 2 measured these; raising sync-interval does not reduce
	// PUTs and multiplies replica lag, so the value is pinned here.
	for _, want := range []string{
		`sync-interval: "1s"`,
		`- interval: "10m0s"`,
		`- interval: "1h0m0s"`,
		`- interval: "6h0m0s"`,
		`interval: "24h0m0s"`,
		`l0-retention: "30m0s"`,
		`path: "agents-meta/v1/g3"`,
		`bucket: "plorifs"`,
		`force-path-style: true`,
		`enabled: true`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("rendered config is missing %s\n%s", want, got)
		}
	}
	// The one bucket-wide credential is inherited from the environment and
	// must never reach a file (mountspec.md §5, threat-model F-11).
	for _, forbidden := range []string{"access-key-id", "secret-access-key", "AKIA"} {
		if strings.Contains(got, forbidden) {
			t.Errorf("rendered config leaks %q:\n%s", forbidden, got)
		}
	}
	info, err := os.Stat(ls.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("config mode = %o, want 600", info.Mode().Perm())
	}
}

func TestYamlQuoteEscapesStructuralCharacters(t *testing.T) {
	cases := map[string]string{
		"plain":       `"plain"`,
		`with"quote`:  `"with\"quote"`,
		"a: b":        `"a: b"`,
		"*anchor":     `"*anchor"`,
		"line\nbreak": `"line\nbreak"`,
		"tab\there":   `"tab\there"`,
		"back\\slash": `"back\\slash"`,
		"\x01control": `"\x01control"`,
	}
	for in, want := range cases {
		if got := yamlQuote(in); got != want {
			t.Errorf("yamlQuote(%q) = %s, want %s", in, got, want)
		}
	}
}

// PLO-421. The plugin stops a worker with kill(-pid, SIGTERM) — the whole
// process group — so a replicator inside that group dies before the worker's
// ordered stop can use it. Measured on staging: the child died 1.3 ms in, the
// final `sync -wait` 26 ms later found no socket, and EVERY ordered stop in a
// cluster came out exit 69 with no `clean` marker and the lease left open.
//
// The child therefore gets a process group of its own, and its lifetime is
// owned by the worker's shutdown instead: final sync, one SIGTERM, wait.
func TestTheLitestreamChildIsOutsideTheWorkersSignalledGroup(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "fake-litestream")
	sock := filepath.Join(dir, "l.sock")
	script := "#!/bin/sh\n: > " + sock + "\nwhile true; do sleep 0.05; done\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatalf("write the fake litestream: %v", err)
	}
	ls := &Litestream{Bin: bin, ConfigPath: filepath.Join(dir, "l.yml"), SocketPath: sock, DBPath: filepath.Join(dir, "meta.db")}
	if err := ls.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = ls.Abort(context.Background()) })

	child := ls.cmd.Process.Pid
	childGroup, err := syscall.Getpgid(child)
	if err != nil {
		t.Fatalf("getpgid(child): %v", err)
	}
	ourGroup, err := syscall.Getpgid(os.Getpid())
	if err != nil {
		t.Fatalf("getpgid(self): %v", err)
	}
	if childGroup == ourGroup {
		t.Fatalf("the litestream child is in this process's group (%d); a group-wide SIGTERM would kill it before the final sync", ourGroup)
	}
	if childGroup != child {
		t.Errorf("child pgid = %d, want %d — it should lead its own group", childGroup, child)
	}
}

// Its lifetime being the worker's SHUTDOWN, not the worker's process, is the
// other half: Stop still ends it, so nothing is leaked by moving it out of the
// group.
func TestStopStillEndsTheChildOutsideTheGroup(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "fake-litestream")
	sock := filepath.Join(dir, "l.sock")
	script := "#!/bin/sh\ntrap 'exit 0' TERM\n: > " + sock + "\nwhile true; do sleep 0.05; done\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatalf("write the fake litestream: %v", err)
	}
	ls := &Litestream{Bin: bin, ConfigPath: filepath.Join(dir, "l.yml"), SocketPath: sock, DBPath: filepath.Join(dir, "meta.db")}
	if err := ls.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	child := ls.cmd.Process.Pid
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := ls.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := syscall.Kill(child, 0); err == nil {
		t.Fatalf("the child pid %d is still alive after Stop", child)
	}
}

// rawControlSocket is a Litestream control socket that answers with whatever
// bytes the test writes, so an answer can be cut off, left unfinished or never
// started: the shapes a dying or stalled replicator produces, which net/http's
// own server will not emit on purpose.
type rawControlSocket struct {
	path string
}

func newRawControlSocket(t *testing.T, answer func(conn net.Conn)) *rawControlSocket {
	t.Helper()
	// A short path: sun_path is capped near 100 bytes on macOS.
	dir, err := os.MkdirTemp("", "ctl")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	s := &rawControlSocket{path: filepath.Join(dir, "c.sock")}
	ln, err := net.Listen("unix", s.path)
	if err != nil {
		t.Fatalf("listen on %s: %v", s.path, err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				if _, err := http.ReadRequest(bufio.NewReader(conn)); err != nil {
					return
				}
				answer(conn)
			}()
		}
	}()
	return s
}

// A 200 is only an acknowledgement once its body is whole. Each row is a way a
// replicator can die mid-answer, and each used to be a completed sync: the read
// loop stopped at any error and returned the bytes it had. The transport can
// see the first two; the third ends like a whole body and only the decode of a
// waiting sync can refuse it.
func TestAControlAnswerCutShortIsNeverASuccessfulSync(t *testing.T) {
	for _, tc := range []struct {
		name             string
		wire             string
		transportRefuses bool
	}{
		{
			name:             "shorter than its content-length",
			wire:             "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 96\r\n\r\n{\"status\":\"synced\",\"txid\":9,",
			transportRefuses: true,
		},
		{
			name:             "a chunked body that never ends",
			wire:             "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n40\r\n{\"status\":\"synced\",\"txid\":9,",
			transportRefuses: true,
		},
		{
			name: "a close-delimited body cut mid-object",
			wire: "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"status\":\"synced\",\"txid\":9,\"replicated_txid\":",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sock := newRawControlSocket(t, func(conn net.Conn) { _, _ = io.WriteString(conn, tc.wire) })
			ctx := context.Background()
			db := filepath.Join(t.TempDir(), "meta.db")
			if _, err := litestreamControl(ctx, sock.path, "/sync", map[string]any{"path": db}); (err != nil) != tc.transportRefuses {
				t.Errorf("control call error = %v, want refused by the transport: %t", err, tc.transportRefuses)
			}
			ls := &Litestream{SocketPath: sock.path, DBPath: db}
			node := &NodeReplicator{SocketPath: sock.path, DBPath: db}
			if err := ls.SyncAndWait(ctx); err == nil {
				t.Error("per-mount SyncAndWait read a cut-off 200 as a completed sync")
			}
			if txid, err := ls.TxID(ctx); err == nil {
				t.Errorf("per-mount TxID = %q from a cut-off 200, want an error", txid)
			}
			if err := node.SyncAndWait(ctx); err == nil {
				t.Error("node SyncAndWait read a cut-off 200 as a completed sync")
			}
			if txid, err := node.TxID(ctx); err == nil {
				t.Errorf("node TxID = %q from a cut-off 200, want an error", txid)
			}
		})
	}
}

// The status line is whole even when the body is not, and a 404 is the answer
// the node replicator's repair and detach branch on (isNotRegistered). A
// cut-off body must not turn it into an untyped transport error.
func TestAControlRefusalWithACutOffBodyKeepsItsStatus(t *testing.T) {
	sock := newRawControlSocket(t, func(conn net.Conn) {
		_, _ = io.WriteString(conn, "HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nContent-Length: 64\r\n\r\n{\"error\":\"database not")
	})
	node := &NodeReplicator{SocketPath: sock.path, DBPath: filepath.Join(t.TempDir(), "meta.db")}
	if err := node.Probe(context.Background()); !isNotRegistered(err) {
		t.Fatalf("probe error = %v, want a 404 the caller can still tell apart", err)
	}
	if err := node.DetachBeforeRestore(context.Background()); err != nil {
		t.Fatalf("detach read a cut-off 404 as a failure: %v", err)
	}
}

// A replicator that stops answering must not hold a control call past its
// bound, whether it never sends a status line or sends one and stalls in the
// body, and a caller with no deadline of its own still gets one. The
// abandoned connection is closed, so nothing is left waiting on the socket.
func TestAStalledControlAnswerIsBoundedEvenWithoutACallerDeadline(t *testing.T) {
	for _, tc := range []struct {
		name     string
		answer   func(net.Conn)
		noStatus bool
	}{
		{
			name:     "no status line",
			answer:   func(conn net.Conn) { _, _ = io.Copy(io.Discard, conn) },
			noStatus: true,
		},
		{
			name: "a body that stops arriving",
			answer: func(conn net.Conn) {
				_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 96\r\n\r\n{\"status\":")
				_, _ = io.Copy(io.Discard, conn)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			released := make(chan struct{}, 1)
			sock := newRawControlSocket(t, func(conn net.Conn) {
				tc.answer(conn)
				released <- struct{}{}
			})
			const bound = 150 * time.Millisecond
			start := time.Now()
			_, err := litestreamControlWithin(context.Background(), bound, sock.path, "/sync", map[string]any{"wait": true})
			elapsed := time.Since(start)
			if err == nil {
				t.Fatal("a stalled control call returned success")
			}
			if tc.noStatus && !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("error = %v, want the call's own deadline", err)
			}
			if elapsed > bound+time.Second {
				t.Errorf("the call returned after %s, want about %s", elapsed, bound)
			}
			select {
			case <-released:
			case <-time.After(2 * time.Second):
				t.Error("the abandoned connection was left open on the replicator")
			}
		})
	}
}
