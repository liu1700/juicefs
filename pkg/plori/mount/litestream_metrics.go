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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// WorkspaceLitestreamMetricsAddr is where a Workspace gateway writer's
// Litestream child serves Prometheus metrics. The gateway pod is one trusted
// container with its own network namespace; the port is never published.
const WorkspaceLitestreamMetricsAddr = "127.0.0.1:9909"

// LitestreamMetricsChildGauge is the writer metric that binds a scrape of
// WorkspaceLitestreamMetricsAddr to the child this writer supervises.
const LitestreamMetricsChildGauge = "juicefs_plori_litestream_metrics_child"

// Why the writer vouches for the listener instead of the collector trusting
// the port.
//
// Pinned Litestream v0.5.17 binds Addr in a goroutine and only logs a bind
// failure (cmd/litestream/replicate.go:361-366), so a child can run without
// the listener while something else holds the port. Its own
// process_start_time_seconds cannot identify it either: the default collector
// reads /proc/<os.Getpid()>, and the gateway writer is PID 1 of a private PID
// namespace that still sees the container's /proc, so that path names a
// different process.
//
// The writer is the child's parent. It resolves the child in /proc once, while
// the child cannot yet be reaped, and on every scrape proves that the one
// socket listening on the metrics port is held by that same process. The gauge
// is then the child's start sequence; anything missing, ambiguous or
// unreadable reports 0.

// Bounds for the /proc reads. The pod's process table and the child's
// descriptor table are small; exceeding a bound is reported as unattested.
const (
	maxProcEntries      = 16384
	maxChildDescriptors = 4096
	maxProcFileBytes    = 1 << 20
	maxProcNetLines     = 65536
)

type litestreamChild struct {
	seq        uint64
	parentPID  int    // the writer, in the /proc mount's PID namespace
	hostPID    int    // the child, in the /proc mount's PID namespace
	innerPID   int    // the child, as exec returned it
	startTicks uint64 // /proc/<pid>/stat field 22
}

type litestreamChildState struct {
	mu       sync.Mutex
	seq      uint64 // children started, advanced before each exec
	epoch    uint64 // advanced by every start and invalidation
	current  *litestreamChild
	procRoot string // "" means /proc; tests point it at a fixture
}

func (s *litestreamChildState) root() string {
	if s.procRoot == "" {
		return "/proc"
	}
	return s.procRoot
}

// begin names the next child and drops the previous one.
func (s *litestreamChildState) begin() (seq, epoch uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	s.epoch++
	s.current = nil
	return s.seq, s.epoch
}

// invalidate drops the current child before it is stopped, restarted or
// replaced, so no scrape vouches for a listener the writer is tearing down.
func (s *litestreamChildState) invalidate() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.epoch++
	s.current = nil
}

// forget runs once the child was reaped.
func (s *litestreamChildState) forget(seq uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current != nil && s.current.seq == seq {
		s.current = nil
		s.epoch++
	}
}

// capture resolves the child in /proc outside the lock and records it only if
// no invalidation happened meanwhile.
func (s *litestreamChildState) capture(seq, epoch uint64, innerPID int) error {
	c, err := resolveLitestreamChild(s.root(), innerPID)
	if err != nil {
		return err
	}
	c.seq = seq
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.epoch != epoch {
		return errors.New("litestream child was replaced during capture")
	}
	s.current = c
	return nil
}

// MetricsChild is the value of LitestreamMetricsChildGauge: the supervised
// child's start sequence when it alone holds the metrics listener, else 0.
// It never blocks on the child and holds the lock only to copy state.
func (l *Litestream) MetricsChild() uint64 {
	if l.MetricsAddr == "" {
		return 0
	}
	s := &l.child
	s.mu.Lock()
	c, epoch := s.current, s.epoch
	s.mu.Unlock()
	if c == nil || !litestreamListenerOwned(s.root(), c, l.MetricsAddr) {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current != c || s.epoch != epoch {
		return 0
	}
	return c.seq
}

func resolveLitestreamChild(root string, innerPID int) (*litestreamChild, error) {
	self, err := os.Readlink(filepath.Join(root, "self"))
	if err != nil {
		return nil, fmt.Errorf("resolve own pid: %w", err)
	}
	parent, err := strconv.Atoi(self)
	if err != nil || parent <= 0 {
		return nil, fmt.Errorf("own pid %q is not a pid", self)
	}
	dir, err := os.Open(root)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	var host []int
	seen := 0
	for {
		names, err := dir.Readdirnames(1024)
		for _, name := range names {
			seen++
			if seen > maxProcEntries {
				return nil, fmt.Errorf("more than %d /proc entries", maxProcEntries)
			}
			pid, perr := strconv.Atoi(name)
			if perr != nil || pid <= 0 {
				continue
			}
			ppid, nspid, serr := procStatusIDs(root, pid)
			if serr != nil || ppid != parent || len(nspid) == 0 || nspid[len(nspid)-1] != innerPID {
				continue
			}
			host = append(host, pid)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	if len(host) != 1 {
		return nil, fmt.Errorf("found %d processes for child pid %d of %d", len(host), innerPID, parent)
	}
	ticks, err := procStartTicks(root, host[0])
	if err != nil {
		return nil, err
	}
	return &litestreamChild{parentPID: parent, hostPID: host[0], innerPID: innerPID, startTicks: ticks}, nil
}

func litestreamListenerOwned(root string, c *litestreamChild, addr string) bool {
	ticks, err := procStartTicks(root, c.hostPID)
	if err != nil || ticks != c.startTicks {
		return false // exited, or the PID now names another process
	}
	ppid, nspid, err := procStatusIDs(root, c.hostPID)
	if err != nil || ppid != c.parentPID || len(nspid) == 0 || nspid[len(nspid)-1] != c.innerPID {
		return false
	}
	inode, err := uniqueLoopbackListener(root, addr)
	if err != nil {
		return false
	}
	return procHoldsSocket(root, c.hostPID, inode)
}

func readProcFile(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, maxProcFileBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxProcFileBytes {
		return nil, fmt.Errorf("%s is larger than %d bytes", path, maxProcFileBytes)
	}
	return data, nil
}

// procStatusIDs returns PPid and NSpid from /proc/<pid>/status. NSpid lists
// the PID in the /proc mount's namespace first and the innermost one last.
func procStatusIDs(root string, pid int) (ppid int, nspid []int, err error) {
	data, err := readProcFile(filepath.Join(root, strconv.Itoa(pid), "status"))
	if err != nil {
		return 0, nil, err
	}
	ppid = -1
	for _, line := range strings.Split(string(data), "\n") {
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		switch key {
		case "PPid":
			if ppid, err = strconv.Atoi(strings.TrimSpace(value)); err != nil {
				return 0, nil, err
			}
		case "NSpid":
			for _, field := range strings.Fields(value) {
				id, err := strconv.Atoi(field)
				if err != nil {
					return 0, nil, err
				}
				nspid = append(nspid, id)
			}
		}
	}
	if ppid < 0 || len(nspid) == 0 {
		return 0, nil, errors.New("status has no PPid or NSpid")
	}
	return ppid, nspid, nil
}

// procStartTicks returns field 22 of /proc/<pid>/stat, the start time in clock
// ticks since boot. Fields are counted after the last ')' because the command
// name may itself contain spaces and parentheses.
func procStartTicks(root string, pid int) (uint64, error) {
	data, err := readProcFile(filepath.Join(root, strconv.Itoa(pid), "stat"))
	if err != nil {
		return 0, err
	}
	end := strings.LastIndexByte(string(data), ')')
	if end < 0 {
		return 0, errors.New("stat has no command name")
	}
	fields := strings.Fields(string(data[end+1:]))
	const startTime = 22 - 3 // fields[0] is field 3, the state
	if len(fields) <= startTime {
		return 0, errors.New("stat is too short")
	}
	return strconv.ParseUint(fields[startTime], 10, 64)
}

// uniqueLoopbackListener returns the socket inode of the only TCP listener on
// addr's port, which must be addr itself (an IPv4 loopback address).
func uniqueLoopbackListener(root, addr string) (uint64, error) {
	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		return 0, err
	}
	want := net.ParseIP(host).To4()
	port, err := strconv.ParseUint(portText, 10, 16)
	if want == nil || !want.IsLoopback() || err != nil {
		return 0, fmt.Errorf("metrics address %q is not an IPv4 loopback address", addr)
	}
	var listeners int
	var inode uint64
	for _, table := range []string{"tcp", "tcp6"} {
		data, err := readProcFile(filepath.Join(root, "net", table))
		if err != nil {
			if table == "tcp6" && errors.Is(err, os.ErrNotExist) {
				continue // IPv6 disabled
			}
			return 0, err
		}
		scanner := bufio.NewScanner(strings.NewReader(string(data)))
		for lines := 0; scanner.Scan(); lines++ {
			if lines == 0 {
				continue // header
			}
			if lines > maxProcNetLines {
				return 0, fmt.Errorf("/proc/net/%s has more than %d lines", table, maxProcNetLines)
			}
			fields := strings.Fields(scanner.Text())
			if len(fields) < 10 || fields[3] != "0A" { // 0A is TCP_LISTEN
				continue
			}
			localHex, localPort, ok := strings.Cut(fields[1], ":")
			if !ok {
				continue
			}
			p, err := strconv.ParseUint(localPort, 16, 16)
			if err != nil || p != port {
				continue
			}
			listeners++
			if table != "tcp" || !procIPv4Equal(localHex, want) {
				continue
			}
			if inode, err = strconv.ParseUint(fields[9], 10, 64); err != nil {
				return 0, err
			}
		}
		if err := scanner.Err(); err != nil {
			return 0, err
		}
	}
	if listeners != 1 || inode == 0 {
		return 0, fmt.Errorf("%d listeners on port %d, want exactly one on %s", listeners, port, addr)
	}
	return inode, nil
}

// procIPv4Equal decodes a /proc/net/tcp address, which the kernel prints as the
// raw 32-bit value in host byte order.
func procIPv4Equal(hexAddr string, want net.IP) bool {
	v, err := strconv.ParseUint(hexAddr, 16, 32)
	if err != nil || len(hexAddr) != 8 {
		return false
	}
	var ip [4]byte
	binary.NativeEndian.PutUint32(ip[:], uint32(v))
	return net.IP(ip[:]).Equal(want)
}

func procHoldsSocket(root string, pid int, inode uint64) bool {
	dir, err := os.Open(filepath.Join(root, strconv.Itoa(pid), "fd"))
	if err != nil {
		return false
	}
	defer dir.Close()
	names, err := dir.Readdirnames(maxChildDescriptors + 1)
	if (err != nil && err != io.EOF) || len(names) > maxChildDescriptors {
		return false
	}
	want := "socket:[" + strconv.FormatUint(inode, 10) + "]"
	for _, name := range names {
		if target, err := os.Readlink(filepath.Join(dir.Name(), name)); err == nil && target == want {
			return true
		}
	}
	return false
}
