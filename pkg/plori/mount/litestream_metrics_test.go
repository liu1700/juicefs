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
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// procFixture is a /proc tree shaped like the gateway's: the writer is PID
// 100 in the /proc mount's namespace and PID 1 of its own, so its Litestream
// child is 205 outside and 2 inside.
type procFixture struct {
	t    *testing.T
	root string
}

const (
	fixtureWriter = 100
	fixtureChild  = 205
	fixtureInner  = 2
	fixtureTicks  = 777
	fixtureInode  = 4242
)

func newProcFixture(t *testing.T) *procFixture {
	t.Helper()
	p := &procFixture{t: t, root: t.TempDir()}
	if err := os.Symlink(strconv.Itoa(fixtureWriter), filepath.Join(p.root, "self")); err != nil {
		t.Fatal(err)
	}
	p.write("net/tcp", "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n")
	p.write("net/tcp6", "  sl  local_address                         remote_address                        st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n")
	p.process(fixtureWriter, 1, "100\t1", 500)
	p.process(fixtureChild, fixtureWriter, "205\t2", fixtureTicks)
	p.process(300, fixtureWriter, "300\t9", 900)      // another child of the writer
	p.process(301, 50, "301\t2", 901)                 // the same inner PID elsewhere
	p.fd(fixtureChild, 3, "socket:["+strconv.Itoa(fixtureInode)+"]")
	p.fd(fixtureChild, 4, "/state/litestream.yml")
	p.listen("tcp", loopbackHex(), 9909, fixtureInode)
	return p
}

func (p *procFixture) write(rel, content string) {
	p.t.Helper()
	path := filepath.Join(p.root, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		p.t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		p.t.Fatal(err)
	}
}

func (p *procFixture) append(rel, content string) {
	p.t.Helper()
	f, err := os.OpenFile(filepath.Join(p.root, rel), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		p.t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString(content); err != nil {
		p.t.Fatal(err)
	}
}

func (p *procFixture) process(pid, ppid int, nspid string, ticks uint64) {
	p.t.Helper()
	p.write(fmt.Sprintf("%d/status", pid), fmt.Sprintf("Name:\tlitestream\nPid:\t%d\nPPid:\t%d\nNSpid:\t%s\n", pid, ppid, nspid))
	p.stat(pid, ppid, ticks)
}

// stat uses a command name with a space and a ')' so parsing must count
// fields after the last parenthesis.
func (p *procFixture) stat(pid, ppid int, ticks uint64) {
	p.t.Helper()
	p.write(fmt.Sprintf("%d/stat", pid), fmt.Sprintf("%d (lite) stream) S %d 1 1 0 -1 0 0 0 0 0 0 0 0 0 20 0 1 0 %d 100 200\n", pid, ppid, ticks))
}

func (p *procFixture) fd(pid, fd int, target string) {
	p.t.Helper()
	dir := filepath.Join(p.root, strconv.Itoa(pid), "fd")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		p.t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(dir, strconv.Itoa(fd))); err != nil {
		p.t.Fatal(err)
	}
}

func (p *procFixture) listen(table, addrHex string, port int, inode uint64) {
	p.t.Helper()
	p.append("net/"+table, fmt.Sprintf("   0: %s:%04X 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 %d 1 0000000000000000 100 0 0 10 0\n", addrHex, port, inode))
}

func loopbackHex() string {
	return fmt.Sprintf("%08X", binary.NativeEndian.Uint32([]byte{127, 0, 0, 1}))
}

func attestedFixture(t *testing.T) (*procFixture, *Litestream, uint64) {
	t.Helper()
	p := newProcFixture(t)
	l := &Litestream{MetricsAddr: WorkspaceLitestreamMetricsAddr}
	l.child.procRoot = p.root
	seq, epoch := l.child.begin()
	if err := l.child.capture(seq, epoch, fixtureInner); err != nil {
		t.Fatalf("capture: %v", err)
	}
	return p, l, seq
}

func TestLitestreamMetricsChildVouchesOnlyForItsOwnListener(t *testing.T) {
	_, l, seq := attestedFixture(t)
	if got := l.MetricsChild(); got != seq || seq == 0 {
		t.Fatalf("owned listener: gauge = %d, want child sequence %d", got, seq)
	}
	c := l.child.current
	if c.hostPID != fixtureChild || c.parentPID != fixtureWriter || c.startTicks != fixtureTicks {
		t.Fatalf("captured %+v, want host %d parent %d ticks %d", *c, fixtureChild, fixtureWriter, fixtureTicks)
	}

	for name, change := range map[string]func(p *procFixture, l *Litestream){
		"pid now names another process": func(p *procFixture, _ *Litestream) { p.stat(fixtureChild, fixtureWriter, fixtureTicks+1) },
		"another process holds the port": func(p *procFixture, _ *Litestream) {
			p.write("net/tcp", "header\n")
			p.listen("tcp", loopbackHex(), 9909, 5555)
		},
		"a second listener on the port": func(p *procFixture, _ *Litestream) {
			p.listen("tcp6", "00000000000000000000000000000000", 9909, 6666)
		},
		"only a wildcard listener": func(p *procFixture, _ *Litestream) {
			p.write("net/tcp", "header\n")
			p.listen("tcp", "00000000", 9909, fixtureInode)
		},
		"descriptor table unreadable": func(p *procFixture, _ *Litestream) {
			if err := os.RemoveAll(filepath.Join(p.root, strconv.Itoa(fixtureChild), "fd")); err != nil {
				p.t.Fatal(err)
			}
		},
		"child exited":                 func(p *procFixture, _ *Litestream) { _ = os.RemoveAll(filepath.Join(p.root, strconv.Itoa(fixtureChild))) },
		"invalidated before a restart": func(_ *procFixture, l *Litestream) { l.child.invalidate() },
		"reaped":                       func(_ *procFixture, l *Litestream) { l.child.forget(1) },
		"next start not captured yet":  func(_ *procFixture, l *Litestream) { l.child.begin() },
		"legacy mount":                 func(_ *procFixture, l *Litestream) { l.MetricsAddr = "" },
	} {
		t.Run(name, func(t *testing.T) {
			p, l, _ := attestedFixture(t)
			change(p, l)
			if got := l.MetricsChild(); got != 0 {
				t.Fatalf("gauge = %d, want 0", got)
			}
		})
	}
}

func TestLitestreamMetricsCaptureRefusesAnAmbiguousChild(t *testing.T) {
	for name, change := range map[string]func(p *procFixture){
		"two matching children": func(p *procFixture) { p.process(400, fixtureWriter, "400\t2", 1000) },
		"no NSpid":              func(p *procFixture) { p.write("205/status", "Name:\tlitestream\nPPid:\t100\n") },
		"no such child":         func(p *procFixture) { _ = os.RemoveAll(filepath.Join(p.root, "205")) },
	} {
		t.Run(name, func(t *testing.T) {
			p := newProcFixture(t)
			change(p)
			l := &Litestream{MetricsAddr: WorkspaceLitestreamMetricsAddr}
			l.child.procRoot = p.root
			seq, epoch := l.child.begin()
			if err := l.child.capture(seq, epoch, fixtureInner); err == nil {
				t.Fatal("capture succeeded")
			}
			if got := l.MetricsChild(); got != 0 {
				t.Fatalf("gauge = %d, want 0", got)
			}
		})
	}
}

func TestLitestreamMetricsCaptureLosesToAnInvalidation(t *testing.T) {
	p := newProcFixture(t)
	l := &Litestream{MetricsAddr: WorkspaceLitestreamMetricsAddr}
	l.child.procRoot = p.root
	seq, epoch := l.child.begin()
	l.child.invalidate() // a Stop raced the capture
	if err := l.child.capture(seq, epoch, fixtureInner); err == nil {
		t.Fatal("a capture after an invalidation was recorded")
	}
	if got := l.MetricsChild(); got != 0 {
		t.Fatalf("gauge = %d, want 0", got)
	}
}

// The listener goes only into the replicate config of a mount that asked for
// it; restore configs and every other mount keep Addr empty.
func TestLitestreamMetricsAddrIsGatewayReplicateOnly(t *testing.T) {
	spec := &MountSpec{MetaPrefix: "agents-meta/v1/g3/", ObjectStore: ObjectStore{Endpoint: "https://s3", Bucket: "b", Region: "r"}}
	opts := ParseMountOptions(nil)
	for _, c := range []struct {
		name, addr, replicate string
	}{
		{"legacy", "", `addr: ""`},
		{"gateway", WorkspaceLitestreamMetricsAddr, `addr: "127.0.0.1:9909"`},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			l := &Litestream{ConfigPath: filepath.Join(dir, "litestream.yml"), SocketPath: filepath.Join(dir, "l.sock"), DBPath: filepath.Join(dir, "meta.db"), MetricsAddr: c.addr}
			if err := l.WriteConfig(spec, opts); err != nil {
				t.Fatal(err)
			}
			if err := l.writeRestoreConfig(spec, opts, "agents-meta/v1/g2/"); err != nil {
				t.Fatal(err)
			}
			replicate, err := os.ReadFile(l.ConfigPath)
			if err != nil {
				t.Fatal(err)
			}
			restore, err := os.ReadFile(l.restoreConfigPath())
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(replicate), c.replicate+"\n") {
				t.Errorf("replicate config lacks %s:\n%s", c.replicate, replicate)
			}
			if !strings.Contains(string(restore), `addr: ""`+"\n") {
				t.Errorf("restore config carries a metrics address:\n%s", restore)
			}
		})
	}
}

// Against the real /proc: every start advances the sequence before exec and
// captures the new child; stop, restart and a failed start leave no stale
// identity. The fake child never listens, so the gauge stays 0 throughout.
func TestLitestreamMetricsChildFollowsTheSupervisedChild(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "fake-litestream")
	sock := filepath.Join(dir, "l.sock")
	script := "#!/bin/sh\ntrap 'exit 0' TERM\n: > " + sock + "\nwhile true; do sleep 0.05; done\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	l := &Litestream{Bin: bin, ConfigPath: filepath.Join(dir, "l.yml"), SocketPath: sock, DBPath: filepath.Join(dir, "meta.db"), MetricsAddr: WorkspaceLitestreamMetricsAddr}
	t.Cleanup(func() { _ = l.Abort(context.Background()) })
	identity := func() *litestreamChild {
		l.child.mu.Lock()
		defer l.child.mu.Unlock()
		return l.child.current
	}

	if err := l.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	first := identity()
	if first == nil || first.seq != 1 || first.hostPID != l.cmd.Process.Pid {
		t.Fatalf("after Start: identity %+v, want sequence 1 for pid %d", first, l.cmd.Process.Pid)
	}
	if got := l.MetricsChild(); got != 0 {
		t.Fatalf("a child without a listener was vouched for: %d", got)
	}

	if err := l.Restart(context.Background()); err != nil {
		t.Fatalf("Restart: %v", err)
	}
	second := identity()
	if second == nil || second.seq != 2 || second.hostPID != l.cmd.Process.Pid {
		t.Fatalf("after Restart: identity %+v, want sequence 2 for pid %d", second, l.cmd.Process.Pid)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := l.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if got := identity(); got != nil {
		t.Fatalf("identity after Stop: %+v", got)
	}

	if err := os.WriteFile(bin, []byte("#!/bin/sh\nexit 3\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := l.Start(context.Background()); err == nil {
		t.Fatal("a child that exits during startup started")
	}
	if got := identity(); got != nil {
		t.Fatalf("identity after a failed start: %+v", got)
	}
	l.child.mu.Lock()
	seq := l.child.seq
	l.child.mu.Unlock()
	if seq != 3 {
		t.Fatalf("sequence after three starts = %d, want 3", seq)
	}
}
