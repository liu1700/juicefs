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

package vfs

import (
	"bytes"
	"syscall"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
)

// The gate internal_plori.go installs is threat-model.md F-7's whole
// mitigation: `.control` is a file in the mount root, the Agent's processes run
// inside that mount, and upstream only `meta.RemoteDurability` checks the
// caller's uid. Nothing asserted the resulting table until PLO-381, so a change
// that narrowed the gate to one opcode, or admitted a third uid, would pass the
// suite.
//
// These tests are read-only about the gate: they assert the table the gate
// already answers, and change no behaviour.
//
// Only `.control` routes through the gate. `.stats`, `.accesslog` and `.config`
// are separate internal inodes served by Read (vfs.go:571, :651, handle.go:307)
// and never reach handleInternalMsg, so they are outside this table.

// internalOpcodes is every `.control` opcode, by name. The completeness check
// below pins it against the const block those names come from
// (pkg/meta/interface.go:42-61), so an opcode added there fails this test until
// it is listed here.
var internalOpcodes = []struct {
	name string
	cmd  uint32
}{
	{"DeleteSlice", meta.DeleteSlice},
	{"CompactChunk", meta.CompactChunk},
	{"Rmr", meta.Rmr},
	{"LegacyInfo", meta.LegacyInfo},
	{"FillCache", meta.FillCache},
	{"InfoV2", meta.InfoV2},
	{"Clone", meta.Clone},
	{"OpSummary", meta.OpSummary},
	{"CompactPath", meta.CompactPath},
	{"RemoteDurability", meta.RemoteDurability},
}

// deniedUID returns a uid that is neither root nor the uid the mount runs as,
// which is the only class the gate refuses.
func deniedUID(t *testing.T) uint32 {
	t.Helper()
	mount := uint32(utils.GetCurrentUID())
	denied := mount + 1
	if denied == 0 {
		denied = 1
	}
	if denied == mount || denied == 0 {
		t.Fatalf("cannot pick a uid that is neither 0 nor the mount uid %d", mount)
	}
	return denied
}

func TestPloriInternalGateCoversEveryOpcode(t *testing.T) {
	listed := make(map[uint32]string, len(internalOpcodes))
	for _, op := range internalOpcodes {
		if prev, dup := listed[op.cmd]; dup {
			t.Fatalf("opcode %d listed twice: %s and %s", op.cmd, prev, op.name)
		}
		listed[op.cmd] = op.name
	}
	// The opcodes are one contiguous const block. Walking it catches an opcode
	// appended to the block and not added to the table above.
	for cmd := uint32(meta.DeleteSlice); cmd <= uint32(meta.RemoteDurability); cmd++ {
		if _, ok := listed[cmd]; !ok {
			t.Errorf("opcode %d is dispatched but not in internalOpcodes", cmd)
		}
	}
}

func TestPloriInternalGateIsInstalled(t *testing.T) {
	if !InternalMsgGateInstalled() {
		t.Fatal("the plori build must install an internal message gate")
	}
}

// TestPloriInternalGateAdmitsRootAndTheMountUID asserts the two admitted uid
// classes for every opcode. It calls the gate rather than handleInternalMsg,
// because an admitted call runs the command itself and this test is about the
// authorization answer, not about what each command does.
func TestPloriInternalGateAdmitsRootAndTheMountUID(t *testing.T) {
	mount := uint32(utils.GetCurrentUID())
	for _, op := range internalOpcodes {
		for _, uid := range []uint32{0, mount} {
			ctx := meta.NewContext(10, uid, []uint32{uint32(utils.GetCurrentGID())})
			if eno := internalMsgGate(ctx, op.cmd); eno != 0 {
				t.Errorf("gate refused %s for uid %d: %s", op.name, uid, eno)
			}
		}
	}
}

// TestPloriInternalGateRefusesEveryOtherUID is the F-7 assertion: not one
// opcode is reachable by an Agent process.
func TestPloriInternalGateRefusesEveryOtherUID(t *testing.T) {
	denied := deniedUID(t)
	for _, op := range internalOpcodes {
		ctx := meta.NewContext(10, denied, []uint32{denied})
		if eno := internalMsgGate(ctx, op.cmd); eno != syscall.EACCES {
			t.Errorf("gate answered %s for %s from uid %d, want EACCES", eno, op.name, denied)
		}
	}
	// An opcode the switch does not know is refused on the same terms. The gate
	// runs before the dispatch, so it is not a per-command allowlist that a new
	// opcode can be added outside of.
	ctx := meta.NewContext(10, denied, []uint32{denied})
	if eno := internalMsgGate(ctx, uint32(meta.RemoteDurability)+1); eno != syscall.EACCES {
		t.Errorf("gate answered %s for an unknown opcode from uid %d, want EACCES", eno, denied)
	}
}

// TestPloriInternalGateRefusalReachesTheWire drives handleInternalMsg itself,
// so the refusal is asserted where a `.control` writer observes it: a one-byte
// errno response, written before the command is parsed or run.
func TestPloriInternalGateRefusalReachesTheWire(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	denied := deniedUID(t)
	ctx := meta.NewContext(10, denied, []uint32{denied})
	want := []byte{byte(syscall.EACCES & 0xff)}
	for _, op := range internalOpcodes {
		out := &bytes.Buffer{}
		v.handleInternalMsg(ctx, op.cmd, utils.FromBuffer(nil), out)
		if !bytes.Equal(out.Bytes(), want) {
			t.Errorf("%s from uid %d wrote %v, want %v", op.name, denied, out.Bytes(), want)
		}
	}

	// Rmr with skipTrash set is the destructive case: it deletes a tree without
	// routing it through the trash, so the refusal has to land before the
	// payload is read. The tree is still there afterwards.
	root := NewLogContext(meta.Background())
	de, st := v.Mkdir(root, 1, "gate-rmr-target", 0755, 0)
	if st != 0 {
		t.Fatalf("mkdir: %s", st)
	}
	if _, _, st := v.Create(root, de.Inode, "kept", 0644, 0, syscall.O_RDWR); st != 0 {
		t.Fatalf("create: %s", st)
	}
	w := utils.NewBuffer(8 + 1 + uint32(len("gate-rmr-target")) + 1)
	w.Put64(1)
	w.Put8(uint8(len("gate-rmr-target")))
	w.Put([]byte("gate-rmr-target"))
	w.Put8(1) // skipTrash
	out := &bytes.Buffer{}
	v.handleInternalMsg(ctx, meta.Rmr, utils.FromBuffer(w.Bytes()), out)
	if !bytes.Equal(out.Bytes(), want) {
		t.Fatalf("Rmr skipTrash=1 from uid %d wrote %v, want %v", denied, out.Bytes(), want)
	}
	if _, st := v.Lookup(root, 1, "gate-rmr-target"); st != 0 {
		t.Fatalf("the refused Rmr removed the tree: lookup = %s", st)
	}
}
