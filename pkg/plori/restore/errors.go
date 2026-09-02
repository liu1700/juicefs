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

package restore

import "errors"

// Typed error codes. The supervisor maps each onto a `plori-mount` exit code
// and puts the code verbatim into the last stderr JSON line, which the CSI
// plugin republishes as a kubelet event. They are part of the worker/plugin
// contract: never rename one, only add.
//
// The codes for the lifecycle steps this package does not own — restore,
// leases, fencing, the barrier — live in pkg/plori/mount/exit.go. These four
// are the ones this package can be the sole cause of.
const (
	// CodeIntegrityCheckFailed means `PRAGMA integrity_check` on the restored
	// database did not return "ok". It maps to mount.ErrCodeRestoreIntegrity.
	CodeIntegrityCheckFailed = "E_RESTORE_INTEGRITY"
	// CodeFormatMissing means the restored database carries no JuiceFS Format.
	CodeFormatMissing = "E_RESTORE_FORMAT_MISSING"
	// CodeFormatCarriesCredentials is threat-model F-9: a Format that still
	// holds AccessKey/SecretKey/SessionToken must never be replicated, because
	// the replica prefix has a wider blast radius than the mount.
	CodeFormatCarriesCredentials = "E_FORMAT_CARRIES_CREDENTIALS"
	// CodeTrashDisabled means TrashDays < 1. The Rank 1 crash-consistency
	// protocol needs the trash to keep deleted slices alive across a restore
	// that lands before the delete (crash-consistency.md §7).
	CodeTrashDisabled = "E_VOLUME_TRASH_DISABLED"
	// CodeBlockMissingAfterRestore is the restore-time repair marker
	// (crash-consistency.md §7 d3): metadata references a block the object
	// store does not hold.
	CodeBlockMissingAfterRestore = "E_BLOCK_MISSING_AFTER_RESTORE"
)

// Error is the typed error every exported function in this package returns.
// Code is one of the constants above; Retryable tells the CSI plugin whether a
// NodePublish retry can succeed without operator action.
type Error struct {
	Code      string
	Message   string
	Retryable bool
	Err       error
}

func (e *Error) Error() string {
	if e.Err != nil {
		return e.Code + ": " + e.Message + ": " + e.Err.Error()
	}
	return e.Code + ": " + e.Message
}

func (e *Error) Unwrap() error { return e.Err }

// Is matches on the code alone, so a caller can compare against the sentinels
// below without caring about the message or the wrapped cause.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Code == e.Code
}

func newError(code, message string, retryable bool, err error) *Error {
	return &Error{Code: code, Message: message, Retryable: retryable, Err: err}
}

// Sentinels for errors.Is.
var (
	ErrIntegrityCheckFailed     = &Error{Code: CodeIntegrityCheckFailed}
	ErrFormatMissing            = &Error{Code: CodeFormatMissing}
	ErrFormatCarriesCredentials = &Error{Code: CodeFormatCarriesCredentials}
	ErrTrashDisabled            = &Error{Code: CodeTrashDisabled}
	ErrBlockMissing             = &Error{Code: CodeBlockMissingAfterRestore}
)

// Code extracts the code from err, or "" if err carries none.
func Code(err error) string {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}

// Retryable reports whether the CSI plugin may retry NodePublish.
func Retryable(err error) bool {
	var e *Error
	if errors.As(err, &e) {
		return e.Retryable
	}
	return false
}
