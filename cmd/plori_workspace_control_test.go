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
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/plori/gatewaycontrol"
	"github.com/juicedata/juicefs/pkg/vfs"
)

const (
	workspaceCloneSourceID = "11111111-1111-4111-8111-111111111111"
	workspaceCloneStageID  = "22222222-2222-4222-8222-222222222222"
)

func TestPloriWorkspaceCloneTreeUsesWrappedMetadataAndPrivateTrees(t *testing.T) {
	p, m := workspaceCloneTestVolume(t)
	copies, _, staging := workspaceClonePrivateRoots(t, m)
	source := workspaceCloneMkdir(t, m, copies, workspaceCloneSourceID)
	workspaceCloneMkdir(t, m, source, "nested")

	req := workspaceCloneRequest(source, "copies/"+workspaceCloneSourceID, ".plori-gateway/staging/copy-"+workspaceCloneStageID)
	if err := p.CloneTree(context.Background(), req); err != nil {
		t.Fatalf("CloneTree: %v", err)
	}
	clone := workspaceCloneLookup(t, m, staging, "copy-"+workspaceCloneStageID)
	if clone == source {
		t.Fatal("clone retained the source native inode")
	}
	_ = workspaceCloneLookup(t, m, clone, "nested")
}

func TestPloriWorkspaceCloneTreePreservesSourceAttributes(t *testing.T) {
	p, m := workspaceCloneTestVolume(t)
	copies, _, staging := workspaceClonePrivateRoots(t, m)
	source := workspaceCloneMkdir(t, m, copies, workspaceCloneSourceID)
	want := meta.Attr{Uid: 65532, Gid: 65533, Mode: 0o750}
	if st := m.SetAttr(meta.Background(), source, meta.SetAttrUID|meta.SetAttrGID|meta.SetAttrMode, 0, &want); st != 0 {
		t.Fatalf("set source attributes: %s", st)
	}

	req := workspaceCloneRequest(source, "copies/"+workspaceCloneSourceID, ".plori-gateway/staging/copy-"+workspaceCloneStageID)
	if err := p.CloneTree(context.Background(), req); err != nil {
		t.Fatalf("CloneTree: %v", err)
	}
	clone := workspaceCloneLookup(t, m, staging, "copy-"+workspaceCloneStageID)
	var got meta.Attr
	if st := m.GetAttr(meta.Background(), clone, &got); st != 0 {
		t.Fatalf("get cloned attributes: %s", st)
	}
	if got.Uid != want.Uid || got.Gid != want.Gid || got.Mode != want.Mode {
		t.Fatalf("cloned attributes = uid:%d gid:%d mode:%#o, want uid:%d gid:%d mode:%#o", got.Uid, got.Gid, got.Mode, want.Uid, want.Gid, want.Mode)
	}
}

func TestPloriWorkspaceCloneTreeUsesQuotaWrappedCloneRefusal(t *testing.T) {
	p, m := workspaceCloneTestVolume(t)
	copies, _, _ := workspaceClonePrivateRoots(t, m)
	source := workspaceCloneMkdir(t, m, copies, workspaceCloneSourceID)
	workspaceCloneMkdir(t, m, source, "nested")
	if err := meta.PloriApplyGrant(m, 1, 1); err != nil {
		t.Fatalf("restrict grant: %v", err)
	}
	admission := &workspaceCloneQuotaRefusal{}
	meta.PloriSetQuotaAdmission(p.v.Meta, admission)

	req := workspaceCloneRequest(source, "copies/"+workspaceCloneSourceID, ".plori-gateway/staging/copy-"+workspaceCloneStageID)
	err := p.CloneTree(context.Background(), req)
	if !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("CloneTree quota refusal = %v, want ENOSPC", err)
	}
	if got := admission.calls.Load(); got != 1 {
		t.Fatalf("quota admission calls = %d, want 1", got)
	}
}

func TestPloriWorkspaceCloneTreeRefusesOutsideMismatchedAndSymlinkRequests(t *testing.T) {
	p, m := workspaceCloneTestVolume(t)
	copies, revisions, _ := workspaceClonePrivateRoots(t, m)
	source := workspaceCloneMkdir(t, m, copies, workspaceCloneSourceID)

	valid := workspaceCloneRequest(source, "copies/"+workspaceCloneSourceID, ".plori-gateway/staging/copy-"+workspaceCloneStageID)
	cases := []struct {
		name string
		edit func(*gatewaycontrol.CloneRequest)
	}{
		{name: "source traversal", edit: func(r *gatewaycontrol.CloneRequest) { r.Source = "copies/../" + workspaceCloneSourceID }},
		{name: "source outside copies and revisions", edit: func(r *gatewaycontrol.CloneRequest) {
			r.Source = ".plori-gateway/staging/copy-" + workspaceCloneStageID
		}},
		{name: "destination outside staging", edit: func(r *gatewaycontrol.CloneRequest) { r.Destination = "copies/" + workspaceCloneStageID }},
		{name: "destination without typed UUID", edit: func(r *gatewaycontrol.CloneRequest) { r.Destination = ".plori-gateway/staging/copy-not-a-uuid" }},
		{name: "source inode mismatch", edit: func(r *gatewaycontrol.CloneRequest) { r.SourceNativeInode++ }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := valid
			tc.edit(&req)
			if err := p.CloneTree(context.Background(), req); err == nil {
				t.Fatal("CloneTree accepted invalid request")
			}
		})
	}

	var symlink meta.Ino
	if st := m.Symlink(meta.Background(), revisions, workspaceCloneSourceID, "../../copies/"+workspaceCloneSourceID, &symlink, &meta.Attr{}); st != 0 {
		t.Fatalf("create source-shaped symlink: %s", st)
	}
	symlinkReq := workspaceCloneRequest(symlink, ".plori-gateway/revisions/"+workspaceCloneSourceID, ".plori-gateway/staging/revision-"+workspaceCloneStageID)
	if err := p.CloneTree(context.Background(), symlinkReq); err == nil {
		t.Fatal("CloneTree followed a source-shaped symlink")
	}

	if err := p.CloneTree(context.Background(), valid); err != nil {
		t.Fatalf("first CloneTree: %v", err)
	}
	if err := p.CloneTree(context.Background(), valid); err == nil || !strings.Contains(err.Error(), "destination exists") {
		t.Fatalf("second CloneTree = %v, want destination-exists refusal", err)
	}
}

func TestPloriWorkspaceCloneTreeRefusesCanceledContextBeforeMutation(t *testing.T) {
	p, m := workspaceCloneTestVolume(t)
	copies, _, staging := workspaceClonePrivateRoots(t, m)
	source := workspaceCloneMkdir(t, m, copies, workspaceCloneSourceID)
	req := workspaceCloneRequest(source, "copies/"+workspaceCloneSourceID, ".plori-gateway/staging/copy-"+workspaceCloneStageID)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.CloneTree(ctx, req); !errors.Is(err, context.Canceled) {
		t.Fatalf("CloneTree canceled context = %v, want context.Canceled", err)
	}
	workspaceCloneAbsent(t, m, staging, "copy-"+workspaceCloneStageID)
}

func TestPloriWorkspaceGatewayRequiresInPod(t *testing.T) {
	if err := validateWorkspaceGatewayMode(true, mountModeNodeBroker); err == nil {
		t.Fatal("workspace gateway accepted node_broker")
	}
	if err := validateWorkspaceGatewayMode(true, mountModeInPod); err != nil {
		t.Fatalf("workspace gateway rejected in_pod: %v", err)
	}
	if err := validateWorkspaceGatewayMode(false, mountModeNodeBroker); err != nil {
		t.Fatalf("ordinary mount rejected: %v", err)
	}
}

func TestPloriWorkspaceBarrierReturnsNativeFenceEvidence(t *testing.T) {
	store := &workspaceCloneDurabilityStore{status: chunk.DurabilityStatus{
		Fence:                       17,
		LastSuccessfulFence:         17,
		LastSuccessfulBarrierUnixMs: 1234,
	}}
	p := &ploriVolume{store: store}
	got, err := p.Barrier(context.Background())
	if err != nil {
		t.Fatalf("Barrier: %v", err)
	}
	if got.Fence != 17 || got.LastSuccessfulFence != 17 || got.LastSuccessfulBarrierUnixMs != 1234 {
		t.Fatalf("Barrier evidence = %+v", got)
	}
}

type workspaceCloneQuotaRefusal struct{ calls atomic.Int32 }

func (a *workspaceCloneQuotaRefusal) Admit(context.Context) syscall.Errno {
	a.calls.Add(1)
	return syscall.ENOSPC
}

func (*workspaceCloneQuotaRefusal) Proactive() {}

type workspaceCloneDurabilityStore struct {
	chunk.ChunkStore
	status chunk.DurabilityStatus
}

func (s *workspaceCloneDurabilityStore) RemoteDurability(context.Context) (chunk.DurabilityStatus, error) {
	return s.status, nil
}

func (s *workspaceCloneDurabilityStore) RemoteDurabilityStatus() chunk.DurabilityStatus {
	return s.status
}

func workspaceCloneTestVolume(t *testing.T) (*ploriVolume, meta.Meta) {
	t.Helper()
	m := meta.NewClient("sqlite3://"+filepath.Join(t.TempDir(), "workspace-control.db"), nil)
	if err := m.Init(&meta.Format{Name: "workspace-control", DirStats: true, Capacity: 64 << 20, Inodes: 4096}, true); err != nil {
		t.Fatalf("initialize metadata: %v", err)
	}
	t.Cleanup(func() { _ = m.Shutdown() })
	return &ploriVolume{v: &vfs.VFS{Meta: meta.PloriWithQuotaAdmission(m)}}, m
}

func workspaceClonePrivateRoots(t *testing.T, m meta.Meta) (copies, revisions, staging meta.Ino) {
	t.Helper()
	copies = workspaceCloneMkdir(t, m, meta.RootInode, "copies")
	private := workspaceCloneMkdir(t, m, meta.RootInode, ".plori-gateway")
	revisions = workspaceCloneMkdir(t, m, private, "revisions")
	staging = workspaceCloneMkdir(t, m, private, "staging")
	return copies, revisions, staging
}

func workspaceCloneRequest(source meta.Ino, sourcePath, destination string) gatewaycontrol.CloneRequest {
	return gatewaycontrol.CloneRequest{
		Identity:          gatewaycontrol.Identity{StorageVolumeID: "volume", FormatUUID: "format", Generation: 1, FenceEpoch: 1},
		Source:            sourcePath,
		Destination:       destination,
		SourceNativeInode: uint64(source),
	}
}

func workspaceCloneMkdir(t *testing.T, m meta.Meta, parent meta.Ino, name string) meta.Ino {
	t.Helper()
	var inode meta.Ino
	if st := m.Mkdir(meta.Background(), parent, name, 0o700, 0, 0, &inode, nil); st != 0 {
		t.Fatalf("mkdir %s: %s", name, st)
	}
	return inode
}

func workspaceCloneLookup(t *testing.T, m meta.Meta, parent meta.Ino, name string) meta.Ino {
	t.Helper()
	var inode meta.Ino
	var attr meta.Attr
	if st := m.Lookup(meta.Background(), parent, name, &inode, &attr, true); st != 0 {
		t.Fatalf("lookup %s: %s", name, st)
	}
	return inode
}

func workspaceCloneAbsent(t *testing.T, m meta.Meta, parent meta.Ino, name string) {
	t.Helper()
	var inode meta.Ino
	var attr meta.Attr
	if st := m.Lookup(meta.Background(), parent, name, &inode, &attr, true); st != syscall.ENOENT {
		t.Fatalf("lookup %s = %s, want ENOENT", name, st)
	}
}
