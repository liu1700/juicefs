//go:build plori
// +build plori

package restore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/plori/mount"
)

// TestRealAbortResidualRestoresToTheRecordedPointOrRepairsTheLatest uses the
// pinned node-level Litestream daemon with a real JuiceFS SQLite metadata
// database and file object store. A late file is committed after the recorded
// transaction, its data block is removed, then NodeReplicator.Abort performs
// the v0.5.17 unregister. The recorded restore excludes the tail; an
// unanchored restore contains it and the production scan/quarantine path makes
// it readable only up to the safe boundary.
func TestRealAbortResidualRestoresToTheRecordedPointOrRepairsTheLatest(t *testing.T) {
	bin := os.Getenv("LITESTREAM_BIN")
	if bin == "" {
		t.Skip("set LITESTREAM_BIN to run against Litestream v0.5.17")
	}
	version, err := exec.Command(bin, "version").Output()
	if err != nil {
		t.Fatalf("litestream version: %v", err)
	}
	if strings.TrimSpace(string(version)) != "v0.5.17" {
		t.Fatalf("litestream version = %q, want v0.5.17", strings.TrimSpace(string(version)))
	}

	const blockSize = 1 << 20
	v := newVolume(t, volumeOptions{
		trashDays:    1,
		blockSizeKiB: blockSize >> 10,
		files:        map[string]int{"/durable.bin": blockSize},
	})
	socket := startNodeLitestream(t, bin)
	replica := "file://" + filepath.Join(v.dir, "replica")
	if _, err := nodeControl(t.Context(), socket, "/register", map[string]any{"path": v.metaPath, "replica_url": replica}); err != nil {
		t.Fatalf("register metadata database: %v", err)
	}
	initial, err := nodeControl(t.Context(), socket, "/sync", map[string]any{"path": v.metaPath, "wait": true, "timeout": 30})
	if err != nil {
		t.Fatalf("sync recorded durable point: %v", err)
	}
	var sync struct {
		ReplicatedTXID uint64 `json:"replicated_txid"`
	}
	if err := json.Unmarshal(initial, &sync); err != nil || sync.ReplicatedTXID == 0 {
		t.Fatalf("decode recorded replicated txid: %v / %s", err, initial)
	}
	recordedTXID := fmt.Sprintf("%016x", sync.ReplicatedTXID)

	late := bytes.Repeat([]byte("late"), blockSize/4)
	lateBlock := writeFixtureFile(t, v, "/late.bin", late)
	if err := os.Remove(v.blockPath(lateBlock.Key)); err != nil {
		t.Fatalf("remove late block %s: %v", lateBlock.Key, err)
	}
	if err := (&mount.NodeReplicator{SocketPath: socket, DBPath: v.metaPath}).Abort(t.Context()); err != nil {
		t.Fatalf("abort node replication: %v", err)
	}

	recordedDB := filepath.Join(v.dir, "recorded.db")
	if out, err := exec.Command(bin, "restore", "-o", recordedDB, "-txid", recordedTXID, replica).CombinedOutput(); err != nil {
		t.Fatalf("restore recorded point: %v: %s", err, out)
	}
	if err := IntegrityCheck(t.Context(), recordedDB, false); err != nil {
		t.Fatalf("recorded restore integrity: %v", err)
	}
	recordedMeta, recordedFS, closeRecorded := v.openMeta(t, recordedDB)
	durable, err := readAll(t, recordedFS, "/durable.bin")
	if err != nil {
		closeRecorded()
		t.Fatalf("read durable file from recorded restore: %v", err)
	}
	if !bytes.Equal(durable, v.files["/durable.bin"]) {
		closeRecorded()
		t.Fatal("recorded restore changed durable bytes")
	}
	var lateInRecorded meta.Ino
	var lateAttr meta.Attr
	if st := recordedMeta.Lookup(testCtx(), meta.RootInode, "late.bin", &lateInRecorded, &lateAttr, false); st != syscall.ENOENT {
		closeRecorded()
		t.Fatalf("recorded restore late file lookup = %v, want ENOENT", st)
	}
	closeRecorded()

	latestDB := filepath.Join(v.dir, "latest.db")
	if out, err := exec.Command(bin, "restore", "-o", latestDB, replica).CombinedOutput(); err != nil {
		t.Fatalf("restore latest residual: %v: %s", err, out)
	}
	if err := IntegrityCheck(t.Context(), latestDB, false); err != nil {
		t.Fatalf("latest restore integrity: %v", err)
	}
	m, _, closeLatest := v.openMeta(t, latestDB)
	lateIno := lookup(t, m, "late.bin")
	report, err := ScanMissingBlocks(t.Context(), m, v.blocks(t), ScanOptions{Format: v.format})
	if err != nil {
		closeLatest()
		t.Fatalf("scan latest residual: %v", err)
	}
	if len(report.Missing) != 1 || report.Missing[0].Inode != lateIno || report.Missing[0].Key != lateBlock.Key {
		closeLatest()
		t.Fatalf("missing blocks = %+v, want late %s", report.Missing, lateBlock.Key)
	}
	quarantine, err := Quarantine(t.Context(), m, report.Missing, ModeTruncate, v.format)
	var marked []byte
	markStatus := m.GetXattr(testCtx(), lateIno, QuarantineXattr, &marked)
	closeLatest()
	if err != nil {
		t.Fatalf("quarantine latest residual: %v", err)
	}
	if len(quarantine.Entries) != 1 || quarantine.Entries[0].Code != CodeBlockMissingAfterRestore || quarantine.Entries[0].TruncatedTo == nil || *quarantine.Entries[0].TruncatedTo != 0 {
		t.Fatalf("quarantine = %+v, want typed truncation at zero", quarantine)
	}
	if markStatus != 0 {
		t.Fatalf("read quarantine marker: %s", markStatus)
	}
	var mark marker
	if err := json.Unmarshal(marked, &mark); err != nil || mark.Code != CodeBlockMissingAfterRestore {
		t.Fatalf("quarantine marker = %q / %v, want %s", marked, err, CodeBlockMissingAfterRestore)
	}

	_, coldFS, closeCold := v.openMeta(t, latestDB)
	defer closeCold()
	lateRead, err := readAll(t, coldFS, "/late.bin")
	if err != nil {
		t.Fatalf("cold read quarantined late file: %v", err)
	}
	if len(lateRead) != 0 {
		t.Fatalf("cold read late bytes = %d, want 0 after truncate", len(lateRead))
	}
	durable, err = readAll(t, coldFS, "/durable.bin")
	if err != nil {
		t.Fatalf("cold read durable file: %v", err)
	}
	if !bytes.Equal(durable, v.files["/durable.bin"]) {
		t.Fatal("repair changed durable bytes")
	}
}

func writeFixtureFile(t *testing.T, v *volume, path string, data []byte) BlockRef {
	t.Helper()
	m, jfs, closeFn := v.openMeta(t, v.metaPath)
	ctx := testCtx()
	f, errno := jfs.Create(ctx, path, 0644, 0)
	if errno != 0 {
		closeFn()
		t.Fatalf("create %s: %s", path, errno)
	}
	if _, errno := f.Write(ctx, data); errno != 0 {
		_ = f.Close(ctx)
		closeFn()
		t.Fatalf("write %s: %s", path, errno)
	}
	if errno := f.Close(ctx); errno != 0 {
		closeFn()
		t.Fatalf("close %s: %s", path, errno)
	}
	if err := jfs.Flush(); err != nil {
		closeFn()
		t.Fatalf("flush %s: %v", path, err)
	}
	ino := lookup(t, m, strings.TrimPrefix(path, "/"))
	block := blockCovering(t, m, ino, 0, v.blockSize, v.format.HashPrefix)
	closeFn()
	return block
}

func startNodeLitestream(t *testing.T, bin string) string {
	t.Helper()
	dir := t.TempDir()
	socket := filepath.Join(dir, "litestream.sock")
	config := filepath.Join(dir, "litestream.yml")
	if err := os.WriteFile(config, []byte("socket:\n  enabled: true\n  path: "+socket+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, "replicate", "-config", config)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start litestream: %v", err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = cmd.Process.Signal(os.Interrupt)
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
			<-done
		}
	})
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(socket); err == nil {
			return socket
		}
		if time.Now().After(deadline) {
			t.Fatal("litestream control socket was not created")
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func nodeControl(ctx context.Context, socket, path string, body any) ([]byte, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}
	defer transport.CloseIdleConnections()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://litestream"+path, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	resp, err := (&http.Client{Transport: transport}).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("litestream %s status %d", path, resp.StatusCode)
	}
	return out, nil
}
