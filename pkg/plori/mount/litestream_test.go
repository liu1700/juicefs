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
