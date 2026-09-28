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
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
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

// fakeLitestreamEnv makes the test binary act as `litestream replicate`; see
// TestHelperLitestreamChild.
const fakeLitestreamEnv = "PLORI_FAKE_LITESTREAM"

// TestHelperLitestreamChild is not a test on its own. With fakeLitestreamEnv
// set, the test binary is a stand-in for `litestream replicate`: after
// FAKE_LS_DELAY it listens on FAKE_LS_SOCKET and answers `POST /sync`, or
// never answers when FAKE_LS_SYNC is "hang" (a child that is alive but not
// serving, as under memory pressure). FAKE_LS_TERM "exit" makes SIGTERM end it
// with status 0, as litestream's shutdown does; "ignore" makes it ignore
// SIGTERM, as a child that is not scheduling its signal handler would. A shell
// script cannot serve HTTP on a unix socket, and the probe needs a real answer
// to succeed.
func TestHelperLitestreamChild(t *testing.T) {
	if os.Getenv(fakeLitestreamEnv) != "1" {
		return
	}
	switch os.Getenv("FAKE_LS_TERM") {
	case "exit":
		term := make(chan os.Signal, 1)
		signal.Notify(term, syscall.SIGTERM)
		go func() {
			<-term
			// syscall.Exit, not os.Exit: the testing package turns os.Exit(0)
			// inside a test into a panic, which would exit with status 2.
			syscall.Exit(0)
		}()
	case "ignore":
		signal.Ignore(syscall.SIGTERM)
	}
	delay, _ := time.ParseDuration(os.Getenv("FAKE_LS_DELAY"))
	time.Sleep(delay)
	ln, err := net.Listen("unix", os.Getenv("FAKE_LS_SOCKET"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "fake litestream:", err)
		os.Exit(3)
	}
	hang := os.Getenv("FAKE_LS_SYNC") == "hang"
	_ = http.Serve(ln, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hang {
			<-r.Context().Done()
			return
		}
		_, _ = w.Write([]byte(`{"txid":1,"replicated_txid":1}`))
	}))
	os.Exit(0)
}

// fakeChild is what the NEXT fake litestream child will do. It is read when a
// child is spawned, so a test can make a replacement behave differently from
// the child it replaces.
type fakeChild struct {
	mu    sync.Mutex
	delay time.Duration
	sync  string
	term  string
}

func (c *fakeChild) set(delay time.Duration, sync string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.delay, c.sync = delay, sync
}

// newFakeLitestream returns a Litestream whose children are the test binary
// running TestHelperLitestreamChild.
func newFakeLitestream(t *testing.T, child *fakeChild) *Litestream {
	t.Helper()
	testBin, err := os.Executable()
	if err != nil {
		t.Fatalf("test binary: %v", err)
	}
	// A unix socket path is capped near 100 bytes, so it goes in the shortest
	// temp dir available rather than under the test's own name.
	dir, err := os.MkdirTemp("", "ls")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	bin := filepath.Join(dir, "litestream")
	script := "#!/bin/sh\nexec " + testBin + " -test.run='^TestHelperLitestreamChild$' -test.count=1\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatalf("write the fake litestream: %v", err)
	}
	sock := filepath.Join(dir, "l.sock")
	return &Litestream{
		Bin: bin, ConfigPath: filepath.Join(dir, "l.yml"), SocketPath: sock, DBPath: filepath.Join(dir, "meta.db"),
		Env: func() []string {
			child.mu.Lock()
			defer child.mu.Unlock()
			return append(os.Environ(), fakeLitestreamEnv+"=1", "FAKE_LS_SOCKET="+sock,
				"FAKE_LS_DELAY="+child.delay.String(), "FAKE_LS_SYNC="+child.sync, "FAKE_LS_TERM="+child.term)
		},
	}
}

func processAlive(pid int) bool { return syscall.Kill(pid, 0) == nil }

// PLO-1172 item 1. A `/sync` that times out against a running child is not an
// exit: the error must not carry ErrReplicatorGone (which the supervisor
// restarts on at once), and the child must still be running afterwards.
func TestAProbeTimeoutOnARunningChildIsNotReportedAsGone(t *testing.T) {
	child := &fakeChild{sync: "hang"}
	ls := newFakeLitestream(t, child)
	if err := ls.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = ls.Abort(context.Background()) })

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := ls.Probe(ctx)
	if err == nil {
		t.Fatal("a probe against a child that never answers passed")
	}
	if errors.Is(err, ErrReplicatorGone) || errors.Is(err, ErrReplicatorStarting) {
		t.Fatalf("probe error = %v, want a plain failure of a running child", err)
	}
	if !processAlive(ls.cmd.Process.Pid) {
		t.Fatal("the child is not running after a timed-out probe")
	}
}

// A child that died on its own is the one failure a restart repairs at once.
func TestAChildThatExitedIsReportedGone(t *testing.T) {
	child := &fakeChild{sync: "ok"}
	ls := newFakeLitestream(t, child)
	if err := ls.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := ls.Probe(context.Background()); err != nil {
		t.Fatalf("probe of a serving child: %v", err)
	}
	if err := ls.cmd.Process.Kill(); err != nil {
		t.Fatalf("kill: %v", err)
	}
	// The exit reaches Probe through the reaper goroutine; until it does, the
	// probe dials a socket nobody listens on, which is a plain failure.
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := ls.Probe(context.Background())
		if errors.Is(err, ErrReplicatorGone) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("probe error = %v five seconds after SIGKILL, want ErrReplicatorGone", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	// A child already seen to exit is not signalled again by the restart.
	log := &capturedLog{}
	ls.Log = log.fn
	if err := ls.Restart(context.Background()); err != nil {
		t.Fatalf("Restart: %v", err)
	}
	t.Cleanup(func() { _ = ls.Abort(context.Background()) })
	if strings.Contains(log.all(), "litestream_restart_signal") {
		t.Fatalf("an exited child was signalled:\n%s", log.all())
	}
}

// PLO-1172 items 2 and 3. Restart returns once the replacement is spawned, not
// once its socket is open, and the replacement is reported as starting (not
// failed) until it opens it. Probing it in that period must not replace it.
func TestRestartReturnsBeforeTheReplacementOpensItsSocket(t *testing.T) {
	child := &fakeChild{sync: "ok"}
	ls := newFakeLitestream(t, child)
	if err := ls.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = ls.Abort(context.Background()) })

	child.set(2*time.Second, "ok")
	began := time.Now()
	if err := ls.Restart(context.Background()); err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if took := time.Since(began); took > time.Second {
		t.Fatalf("Restart took %s: it waited for the replacement's socket on the caller's goroutine", took)
	}
	if got := ls.RestartCount(); got != 1 {
		t.Fatalf("RestartCount() = %d, want 1", got)
	}
	replacement := ls.cmd.Process.Pid

	deadline := time.Now().Add(10 * time.Second)
	sawStarting := false
	for {
		err := ls.Probe(context.Background())
		if err == nil {
			break
		}
		if !errors.Is(err, ErrReplicatorStarting) {
			t.Fatalf("probe of a replacement inside its start deadline = %v, want ErrReplicatorStarting", err)
		}
		sawStarting = true
		if ls.cmd.Process.Pid != replacement || !processAlive(replacement) {
			t.Fatal("the replacement was replaced or died while it was starting")
		}
		if time.Now().After(deadline) {
			t.Fatal("the replacement never answered a probe")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !sawStarting {
		t.Fatal("the replacement answered at once; the test did not exercise the starting period")
	}
	if ls.cmd.Process.Pid != replacement {
		t.Fatal("the replacement that answered is not the one Restart started")
	}
}

// PLO-1172 item 2. A Start whose caller stopped waiting leaves a child that
// is still inside its own 30 s start deadline. It is reported as starting and
// left running; only past that deadline is it reported gone.
func TestAStartWhoseSocketWaitWasCutShortLeavesTheChildStarting(t *testing.T) {
	child := &fakeChild{delay: time.Hour, sync: "ok"}
	ls := newFakeLitestream(t, child)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := ls.Start(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Start = %v, want the caller's deadline", err)
	}
	t.Cleanup(func() { _ = ls.Abort(context.Background()) })
	pid := ls.cmd.Process.Pid

	for i := 0; i < 3; i++ {
		if err := ls.Probe(context.Background()); !errors.Is(err, ErrReplicatorStarting) {
			t.Fatalf("probe %d of a child inside its start deadline = %v, want ErrReplicatorStarting", i, err)
		}
	}
	if !processAlive(pid) {
		t.Fatal("the half-started child was killed by a probe")
	}

	ls.startBy = time.Now().Add(-time.Second)
	if err := ls.Probe(context.Background()); !errors.Is(err, ErrReplicatorGone) {
		t.Fatalf("probe past the start deadline = %v, want ErrReplicatorGone", err)
	}
}

// PLO-1172 follow-up. Replacing a child that is still running starts with one
// SIGTERM, because that is what makes `litestream replicate` run its shutdown
// sync; a child that exits on it promptly is reaped without SIGKILL.
func TestRestartOfARunningChildSendsSIGTERMFirst(t *testing.T) {
	child := &fakeChild{sync: "hang", term: "exit"}
	ls := newFakeLitestream(t, child)
	log := &capturedLog{}
	ls.Log = log.fn
	if err := ls.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = ls.Abort(context.Background()) })
	old := ls.cmd.Process.Pid

	child.set(0, "ok")
	began := time.Now()
	if err := ls.Restart(context.Background()); err != nil {
		t.Fatalf("Restart: %v", err)
	}
	if took := time.Since(began); took > time.Second {
		t.Fatalf("Restart took %s for a child that exits on SIGTERM", took)
	}
	got := log.all()
	for _, want := range []string{
		fmt.Sprintf("litestream_restart_signal pid %d signal SIGTERM", old),
		fmt.Sprintf("litestream_restart_reaped pid %d status exit status 0", old),
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("log is missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "SIGKILL") {
		t.Fatalf("a child that exited on SIGTERM was also sent SIGKILL:\n%s", got)
	}
	if ls.cmd.Process.Pid == old || ls.RestartCount() != 1 {
		t.Fatalf("no replacement was started (pid %d, restarts %d)", ls.cmd.Process.Pid, ls.RestartCount())
	}
}

// A running child that does not exit on SIGTERM is killed, and the whole stop
// stays inside ProbeTimeout so the supervisor goroutine is not held longer.
func TestRestartKillsAChildThatIgnoresSIGTERMWithinTheCap(t *testing.T) {
	child := &fakeChild{sync: "hang", term: "ignore"}
	ls := newFakeLitestream(t, child)
	log := &capturedLog{}
	ls.Log = log.fn
	if err := ls.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = ls.Abort(context.Background()) })
	old := ls.cmd.Process.Pid

	child.set(0, "ok")
	began := time.Now()
	if err := ls.Restart(context.Background()); err != nil {
		t.Fatalf("Restart: %v", err)
	}
	took := time.Since(began)
	if took > ProbeTimeout {
		t.Fatalf("Restart took %s, more than the %s cap", took, ProbeTimeout)
	}
	if took < ProbeTimeout-restartKillReserve-500*time.Millisecond {
		t.Fatalf("Restart took %s: SIGKILL came before the SIGTERM grace ended", took)
	}
	got := log.all()
	term := strings.Index(got, fmt.Sprintf("litestream_restart_signal pid %d signal SIGTERM", old))
	kill := strings.Index(got, fmt.Sprintf("litestream_restart_signal pid %d signal SIGKILL", old))
	if term < 0 || kill < 0 || kill < term {
		t.Fatalf("want SIGTERM then SIGKILL for pid %d:\n%s", old, got)
	}
	if !strings.Contains(got, fmt.Sprintf("litestream_restart_reaped pid %d status signal: killed", old)) {
		t.Fatalf("the old child was not reaped as killed:\n%s", got)
	}
	if processAlive(old) {
		t.Fatalf("the old child %d is still alive after Restart", old)
	}
	if ls.cmd.Process.Pid == old || ls.RestartCount() != 1 {
		t.Fatalf("no replacement was started (pid %d, restarts %d)", ls.cmd.Process.Pid, ls.RestartCount())
	}
}
