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

package object

import (
	"errors"
	"net/http"
)

// The failure classes a backend can report about a refused operation. A caller
// that has to act differently on a full bucket than on a rejected credential
// (the writeback durability barrier in pkg/chunk is one) tests for these with
// errors.Is instead of matching the message text, which differs per backend and
// per store (PLO-458).
//
// A backend attaches a class with classify, which leaves the message and the
// backend's own error type reachable through errors.As.
var (
	// ErrInsufficientStorage: the store has no room for the write. HTTP 507,
	// or an S3 error code that names a quota or a full backend.
	ErrInsufficientStorage = errors.New("insufficient storage")
	// ErrAccessDenied: the store rejected the credential or the permission.
	// HTTP 403, or an S3 error code that names an access or signature failure.
	ErrAccessDenied = errors.New("access denied")
)

// classFromStatus returns the class an HTTP status code from an object store
// names, or nil when the status names none of them.
func classFromStatus(status int) error {
	switch status {
	case http.StatusInsufficientStorage:
		return ErrInsufficientStorage
	case http.StatusForbidden:
		return ErrAccessDenied
	}
	return nil
}

// classFromCode returns the class an S3-style error code names, or nil when the
// code names none of them. The code is more precise than the status it comes
// with: Ceph RGW reports a quota it cannot satisfy as 403 QuotaExceeded, so a
// backend that has a code must consult it before the status.
func classFromCode(code string) error {
	switch code {
	case "QuotaExceeded", "StorageFull", "InsufficientStorage", "ExceedQuota":
		return ErrInsufficientStorage
	case "AccessDenied", "AllAccessDisabled", "AccountProblem",
		"InvalidAccessKeyId", "SignatureDoesNotMatch", "ExpiredToken", "InvalidToken":
		return ErrAccessDenied
	}
	return nil
}

// classify marks err with class so that errors.Is(err, class) reports true. The
// message is the one err already had, and errors.As still reaches the wrapped
// error. It returns err unchanged when there is no class to attach.
func classify(class, err error) error {
	if class == nil || err == nil {
		return err
	}
	return &classifiedError{class: class, err: err}
}

// classifiedError carries a failure class alongside the error a backend
// produced, without restating either in the message.
type classifiedError struct {
	class error
	err   error
}

func (e *classifiedError) Error() string { return e.err.Error() }

func (e *classifiedError) Unwrap() []error { return []error{e.class, e.err} }
