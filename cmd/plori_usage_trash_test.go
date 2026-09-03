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

package cmd

import (
	"bytes"
	"context"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
	pmount "github.com/juicedata/juicefs/pkg/plori/mount"
	"github.com/juicedata/juicefs/pkg/utils"
)

// The trash half of the usage report, and the one condition under which it must
// not run at all.
//
// A stop detaches the mount and closes the metadata session (supervisor
// shutdown step 4) before it posts the final usage (step 6). StatFS survives
// that — it answers from counters this process holds in memory — and the trash
// walk does not: it is a real Readdir against a session that is gone, it
// answers EIO, and the error is not a syscall.Errno, so pkg/meta's errno()
// logs it with a full Go stack trace. That trace is what a staging shutdown
// printed into the worker log (PLO-468).

// countingMeta is a meta.Meta that answers only the three calls Usage makes and
// counts the walk. Everything else is nil and would panic, which is the point:
// a change that makes Usage touch more of the engine should be seen here.
type countingMeta struct {
	meta.Meta
	readdirs     atomic.Int64
	readdirErrno syscall.Errno
}

func (m *countingMeta) StatFS(_ meta.Context, _ meta.Ino, total, avail, iused, iavail *uint64) syscall.Errno {
	*total, *avail, *iused, *iavail = 100<<20, 40<<20, 7, 993
	return 0
}

func (m *countingMeta) Readdir(_ meta.Context, _ meta.Ino, _ uint8, entries *[]*meta.Entry) syscall.Errno {
	m.readdirs.Add(1)
	if m.readdirErrno != 0 {
		return m.readdirErrno
	}
	*entries = nil
	return 0
}

func (m *countingMeta) Lookup(_ meta.Context, _ meta.Ino, _ string, _ *meta.Ino, _ *meta.Attr, _ bool) syscall.Errno {
	// No /.plori-trash: an Agent that has deleted nothing through the panel.
	return syscall.ENOENT
}

func trashTestVolume(m meta.Meta) *ploriVolume {
	return &ploriVolume{m: m, identity: pmount.FormatIdentity{Name: "agents/vol-plo468"}}
}

// captureLog redirects every juicefs logger into a buffer for the duration of
// the test.
func captureLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	utils.SetOutput(&buf)
	t.Cleanup(func() { utils.SetOutput(os.Stderr) })
	return &buf
}

// TestTheTrashIsNotWalkedOnceTheStopHasBegun is the fix: the final
// usage report keeps its used_bytes and simply carries no breakdown, which is
// the same fail-closed answer a failed walk already gives, and nothing reaches
// a metadata engine that is no longer there.
func TestTheTrashIsNotWalkedOnceTheStopHasBegun(t *testing.T) {
	m := &countingMeta{}
	p := trashTestVolume(m)

	// While the mount is serving, the breakdown is measured.
	u, err := p.Usage(context.Background())
	if err != nil {
		t.Fatalf("Usage on a live mount: %s", err)
	}
	if !u.TrashKnown {
		t.Error("a live mount reported usage with no trash breakdown")
	}
	if got := m.readdirs.Load(); got != 1 {
		t.Fatalf("the trash walk ran %d times on a live mount, want 1", got)
	}

	// The supervisor stops. `p.blob` is nil here so Close itself is not called;
	// the state it hands the volume is set directly, which is the whole of what
	// Usage reads.
	p.stopped = true

	u, err = p.Usage(context.Background())
	if err != nil {
		t.Fatalf("Usage after the stop began: %s", err)
	}
	if got := m.readdirs.Load(); got != 1 {
		t.Errorf("the trash walk ran again after the stop began (%d calls total)", got)
	}
	if u.TrashKnown {
		t.Error("a usage report taken after the stop began claims a trash breakdown it did not measure")
	}
	if u.Bytes != 60<<20 || u.Inodes != 7 {
		t.Errorf("used = %d bytes / %d inodes after the stop began; the usage numbers must survive the skip",
			u.Bytes, u.Inodes)
	}
}

// TestAFailedTrashWalkLogsOneLine is the other half of PLO-468's second
// finding. A walk that fails is a sentence in a card that cannot be written,
// not an incident: the usage report goes out intact, and the worker log gets
// ONE line — no stack, and nothing that would need `grep -v` to read a
// shutdown.
func TestAFailedTrashWalkLogsOneLine(t *testing.T) {
	buf := captureLog(t)
	m := &countingMeta{readdirErrno: syscall.EIO}
	p := trashTestVolume(m)

	u, err := p.Usage(context.Background())
	if err != nil {
		t.Fatalf("a failed trash walk failed the whole usage report: %s", err)
	}
	if u.TrashKnown {
		t.Error("a failed walk still claimed a trash breakdown")
	}
	if u.Bytes != 60<<20 || u.Inodes != 7 {
		t.Errorf("used = %d bytes / %d inodes; a failed walk must leave the usage report intact", u.Bytes, u.Inodes)
	}

	logged := strings.TrimRight(buf.String(), "\n")
	if logged == "" {
		t.Fatal("a failed trash walk logged nothing at all")
	}
	if n := strings.Count(logged, "\n") + 1; n != 1 {
		t.Errorf("a failed trash walk logged %d lines, want 1:\n%s", n, logged)
	}
	if strings.Contains(logged, "goroutine ") {
		t.Errorf("a failed trash walk printed a Go stack trace:\n%s", logged)
	}
	if !strings.Contains(logged, "measuring the trash") {
		t.Errorf("the one line does not say what failed:\n%s", logged)
	}
}
