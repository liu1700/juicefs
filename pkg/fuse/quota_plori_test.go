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

package fuse

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	pmount "github.com/juicedata/juicefs/pkg/plori/mount"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/prometheus/client_golang/prometheus"
)

const quotaTestRenew = 50 * time.Millisecond

func quotaFileSystem(t *testing.T) (*fileSystem, *prometheus.Registry) {
	t.Helper()
	mc := meta.DefaultConf()
	m := meta.NewClient("sqlite3://"+filepath.Join(t.TempDir(), "quota.db"), mc)
	t.Cleanup(func() { _ = m.Shutdown() })
	format := &meta.Format{Name: "quota", UUID: "quota", Storage: "mem", BlockSize: 4096, Capacity: 4096, Inodes: 10}
	if err := m.Init(format, true); err != nil {
		t.Fatal(err)
	}
	var ino meta.Ino
	if st := m.Create(meta.Background(), 1, "seed", 0600, 0, 0, &ino, &meta.Attr{}); st != 0 {
		t.Fatal(st)
	}
	sup := &pmount.Supervisor{Spec: &pmount.MountSpec{LeaseRenewInterval: pmount.Duration(quotaTestRenew)}}
	admitted := meta.PloriWithQuotaAdmission(m)
	meta.PloriSetQuotaAdmission(admitted, sup)
	registry := prometheus.NewRegistry()
	reg := prometheus.WrapRegistererWithPrefix("juicefs_", registry)
	vfs.InitMetrics(reg)
	conf := &vfs.Config{Meta: mc, Format: *format, Chunk: &chunk.Config{BlockSize: 4 << 20, BufferSize: 30 << 20, MaxUpload: 2, MaxDownload: 2}, FuseOpts: &vfs.FuseOptions{}}
	return newFileSystem(conf, vfs.NewVFS(conf, admitted, nil, nil, nil)), registry
}

func TestPloriFuseContextDoneIgnoresInterrupt(t *testing.T) {
	fs := &fileSystem{conf: &vfs.Config{}}
	cancel := make(chan struct{})
	ctx := fs.newContext(cancel, &gofuse.InHeader{})
	defer releaseContext(ctx)
	close(cancel)
	for _, request := range []meta.Context{ctx, ctx.WithValue(struct{}{}, true)} {
		interrupt, ok := request.(interface{ PloriInterrupt() <-chan struct{} })
		if !ok || interrupt.PloriInterrupt() != cancel {
			t.Fatal("request does not expose the kernel interrupt channel")
		}
		select {
		case <-request.Done():
			t.Fatal("kernel interrupt changed upstream Done behavior")
		default:
		}
		if request.Done() != nil {
			t.Fatal("Done should remain the background context's nil channel")
		}
	}
}

func TestPloriQuotaRealFuseContext(t *testing.T) {
	fs, _ := quotaFileSystem(t)
	for _, tc := range []struct {
		when      string
		withValue bool
	}{
		{"not interrupted", false}, {"before admission", false}, {"during admission", false},
		{"not interrupted", true}, {"before admission", true}, {"during admission", true},
	} {
		when := tc.when
		name := when
		if tc.withValue {
			name += "/with_value"
		}
		t.Run(name, func(t *testing.T) {
			cancel := make(chan struct{})
			ctx := fs.newContext(cancel, &gofuse.InHeader{NodeId: 1})
			defer releaseContext(ctx)
			if ctx.Err() != syscall.EINTR {
				t.Fatal("upstream Err behavior changed")
			}
			want := syscall.ENOSPC
			if when == "before admission" {
				close(cancel)
				want = syscall.EINTR
			}
			if when == "during admission" {
				timer := time.AfterFunc(20*time.Millisecond, func() { close(cancel) })
				defer timer.Stop()
				want = syscall.EINTR
			}
			start := time.Now()
			var ino meta.Ino
			var request meta.Context = ctx
			if tc.withValue {
				request = ctx.WithValue(struct{}{}, true)
			}
			got := fs.v.Meta.Create(request, 1, "refused", 0600, 0, 0, &ino, &meta.Attr{})
			if got != want {
				t.Fatalf("Create = %s, want %s", got, want)
			}
			elapsed := time.Since(start)
			if when == "not interrupted" && elapsed < pmount.AdmissionRenewRounds*quotaTestRenew {
				t.Fatalf("refused before admission bound: %s", elapsed)
			}
			if elapsed > time.Second {
				t.Fatalf("admission took %s", elapsed)
			}
		})
	}
}

// Run the Go caller in a subprocess so a regression cannot spin the test process.
func TestPloriQuotaCreateTempCaller(t *testing.T) {
	mp := os.Getenv("PLORI_QUOTA_CALLER_MOUNT")
	if mp == "" {
		t.Skip("subprocess helper")
	}
	f, err := os.CreateTemp(mp, "go-*")
	if f != nil {
		_ = f.Close()
	}
	if !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("CreateTemp = %v, want ENOSPC", err)
	}
	t.Log("os.CreateTemp: ENOSPC")
}

func quotaCreateCount(t *testing.T, registry *prometheus.Registry) float64 {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "juicefs_fuse_ops_total" {
			continue
		}
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if label.GetName() == "method" && label.GetValue() == "create" {
					return metric.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

func TestPloriQuotaRealFUSE(t *testing.T) {
	if os.Getenv("PLORI_QUOTA_FUSE_TEST") != "1" {
		t.Skip("set PLORI_QUOTA_FUSE_TEST=1; requires /dev/fuse, fusermount and python3")
	}
	fs, registry := quotaFileSystem(t)
	mp := t.TempDir()
	server, err := gofuse.NewServer(fs, mp, &gofuse.MountOptions{FsName: "plori-quota-test", Name: "juicefs"})
	if err != nil {
		t.Fatal(err)
	}
	go server.Serve()
	if err := server.WaitMount(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := server.Unmount(); err != nil {
			t.Error(err)
		}
		server.Wait()
	})
	for _, caller := range []string{"go", "python"} {
		t.Run(caller, func(t *testing.T) {
			before := quotaCreateCount(t, registry)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			var cmd *exec.Cmd
			if caller == "go" {
				cmd = exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPloriQuotaCreateTempCaller$", "-test.v")
				cmd.Env = append(os.Environ(), "PLORI_QUOTA_CALLER_MOUNT="+mp)
			} else {
				cmd = exec.CommandContext(ctx, "python3", "-c", `import errno, sys
try:
    open(sys.argv[1] + "/python-create", "w")
except OSError as e:
    assert e.errno == errno.ENOSPC, e
    print("Python open: ENOSPC")
else:
    raise AssertionError("open succeeded")`, mp)
			}
			start := time.Now()
			out, err := cmd.CombinedOutput()
			elapsed := time.Since(start)
			if err != nil {
				t.Fatalf("%s: %v: %s", caller, err, out)
			}
			creates := quotaCreateCount(t, registry) - before
			if creates < 1 || creates >= 10 {
				t.Fatalf("%s creates = %v, want 1..9", caller, creates)
			}
			bound := pmount.AdmissionRenewRounds * quotaTestRenew
			if elapsed > bound+time.Second {
				t.Fatalf("%s exceeded %s bound plus 1s process/scheduling allowance: %s", caller, bound, elapsed)
			}
			t.Logf("%s: %screates=%v elapsed=%s admission_bound=%s", caller, out, creates, elapsed, bound)
		})
	}
}
