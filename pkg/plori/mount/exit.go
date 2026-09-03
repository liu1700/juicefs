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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

// Exit codes. This table is the contract with fuse-csi-node (PLO-331); the
// plugin maps each one to a NodePublish error and a kubelet event, so a code
// may never be reused for a different meaning.
const (
	// CodeOK — clean stop after SIGTERM: fenced, barrier, unmount, final
	// sync, lease released.
	CodeOK = 0
	// CodeSpecInvalid — spec invalid, unsupported credential_source, or an
	// unknown field the worker must not ignore. Not retryable.
	CodeSpecInvalid = 64
	// CodeIdentityMismatch — Format Name/UUID disagrees with the spec or with
	// the `juicefs_uuid` object. Not retryable; the control-plane is told via
	// /lease/release reason=identity_mismatch.
	CodeIdentityMismatch = 65
	// CodeFenced — lease lost (renew returned stale_epoch/lease_held, or the
	// deadline passed) and the worker fenced itself.
	CodeFenced = 66
	// CodeRestoreFailed — the metadata replica is missing, corrupt, or failed
	// its integrity check.
	CodeRestoreFailed = 67
	// CodeObjectStore — object store unreachable or credential rejected at
	// startup. Retryable.
	CodeObjectStore = 68
	// CodeBarrierIncomplete — the barrier or the final sync did not complete
	// inside the write-stop window. Reported data loss; the lease is still
	// released.
	CodeBarrierIncomplete = 69
	// CodeRefused — a fail-closed startup refusal: `.control` would be
	// Agent-writable, the cache dir holds another tenant's staging, or
	// trash-days is 0.
	CodeRefused = 70
)

// Typed error identifiers carried in the stderr JSON `error` field. The plugin
// branches on these, never on the prose.
const (
	// ErrCodeStoppedBeforeMount reports the one exit 0 that is not a served
	// filesystem being handed back: the worker was told to stop while it was
	// still coming up, so it abandoned the startup and released the lease
	// without ever publishing a mount. Exit 0 because nothing failed and
	// nothing was lost — the process did exactly what it was asked — but the
	// plugin still has to tell it apart from a clean stop, because no `ready`
	// file was ever written and no NodePublish can succeed on it (PLO-393 F-3).
	ErrCodeStoppedBeforeMount = "E_STOPPED_BEFORE_MOUNT"
	ErrCodeSpecInvalid        = "E_SPEC_INVALID"
	ErrCodeIdentityMismatch   = "E_IDENTITY_MISMATCH"
	ErrCodeLeaseLost          = "E_LEASE_LOST"
	ErrCodeFenceMarkerHeld    = "E_FENCE_MARKER_HELD"
	// ErrCodeFencedOutOfBand reports that the epoch was taken away rather than
	// allowed to run out — stale_epoch/lease_held from a renew, or a fence
	// marker held by somebody else. Same exit code as any other fence (66); the
	// distinct identifier is what tells an operator that this worker stopped
	// WITHOUT a durability barrier and WITHOUT a final replica sync, so its
	// last writes are gone by design rather than by failure (PLO-323 F-1).
	ErrCodeFencedOutOfBand        = "E_FENCED_OUT_OF_BAND"
	ErrCodeRestoreFailed          = "E_RESTORE_FAILED"
	ErrCodeRestoreIntegrity       = "E_RESTORE_INTEGRITY"
	ErrCodeObjectStoreUnreachable = "E_OBJECT_STORE_UNREACHABLE"
	ErrCodeBarrierIncomplete      = "E_BARRIER_INCOMPLETE"
	ErrCodeVolumeTrashDisabled    = "E_VOLUME_TRASH_DISABLED"
	ErrCodeCacheDirTenantMismatch = "E_CACHE_DIR_TENANT_MISMATCH"
	ErrCodeControlWritable        = "E_CONTROL_FILE_AGENT_WRITABLE"
	// ErrCodeReplicationFailed reports that the metadata replica stopped
	// receiving this database. ADR B1 makes Litestream the metadata backup,
	// so a mount that keeps serving writes with replication off is losing
	// every transaction since the last successful sync — silently, because
	// the filesystem itself is fine. It carries CodeBarrierIncomplete for the
	// same reason a missed barrier does: the lease is released cleanly and
	// the loss is REPORTED rather than hidden (PLO-411).
	ErrCodeReplicationFailed = "E_REPLICATION_FAILED"
	// ErrCodeRestoredToBarrier reports that an unclean generation was
	// recovered to its pre-barrier durable point rather than to its last
	// write (crash-consistency.md §7 Rank 1). PLO-335 decides whether this is
	// ever Agent-visible; today it is operator-only.
	ErrCodeRestoredToBarrier = "E_RESTORED_TO_BARRIER"
)

// Fatal is a refusal that carries the exit code and the typed identifier the
// plugin needs. Everything the supervisor returns is either a Fatal or is
// wrapped into one by Classify.
type Fatal struct {
	Exit      int
	ErrCode   string
	Retryable bool
	Err       error
}

func (f *Fatal) Error() string { return f.Err.Error() }
func (f *Fatal) Unwrap() error { return f.Err }

func fatalf(exit int, code string, retryable bool, format string, args ...any) *Fatal {
	return &Fatal{Exit: exit, ErrCode: code, Retryable: retryable, Err: fmt.Errorf(format, args...)}
}

// Classify turns any error into a Fatal. An unclassified error is deliberately
// mapped to CodeFenced, not to a retryable code: parity-matrix §4a #3 records
// that today an unrecognised mount failure survives only as a stderr substring
// and is treated as retryable, which is how a fenced writer gets restarted.
func Classify(err error) *Fatal {
	if err == nil {
		return nil
	}
	var f *Fatal
	if errors.As(err, &f) {
		return f
	}
	if errors.Is(err, ErrSpec) {
		return &Fatal{Exit: CodeSpecInvalid, ErrCode: ErrCodeSpecInvalid, Err: err}
	}
	return &Fatal{Exit: CodeFenced, ErrCode: ErrCodeLeaseLost, Err: err}
}

// terminalLine is the last stderr line the plugin republishes into a kubelet
// event. threat-model F-11: the plugin republishes at most this line and only
// if it is valid JSON with no `secret`/`key` fields, so nothing here may ever
// carry a credential — which is why it is assembled from a closed field set
// rather than from `%+v` of anything.
type terminalLine struct {
	TS        string `json:"ts"`
	Level     string `json:"level"`
	Event     string `json:"event"`
	Volume    string `json:"volume"`
	Epoch     int64  `json:"epoch"`
	Error     string `json:"error"`
	Message   string `json:"message"`
	Exit      int    `json:"exit"`
	Retryable bool   `json:"retryable"`
}

// WriteTerminal emits the one machine-readable line the plugin reads. It is
// the only place the worker prints an error for a machine.
func WriteTerminal(w io.Writer, volume string, epoch int64, f *Fatal) {
	line := terminalLine{
		TS:        time.Now().UTC().Format(time.RFC3339Nano),
		Level:     "error",
		Event:     "plori_mount_terminal",
		Volume:    volume,
		Epoch:     epoch,
		Error:     f.ErrCode,
		Message:   f.Err.Error(),
		Exit:      f.Exit,
		Retryable: f.Retryable,
	}
	if f.Exit == CodeOK {
		line.Level = "info"
	}
	data, err := json.Marshal(line)
	if err != nil {
		// Marshalling a closed struct of strings cannot fail; if it somehow
		// does, say so without echoing any field.
		fmt.Fprintln(w, `{"level":"error","event":"plori_mount_terminal","error":"E_INTERNAL"}`)
		return
	}
	fmt.Fprintln(w, string(data))
}
