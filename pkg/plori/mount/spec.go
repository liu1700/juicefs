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

// Package mount implements the `juicefs plori-mount` lifecycle supervisor: one
// process that owns exactly one Agent volume for its whole lifetime (PLO-321),
// renews and fails closed on its writer lease (PLO-323), and runs the ordered
// durability shutdown (PLO-326).
//
// The package deliberately knows nothing about JuiceFS internals. Everything
// that needs the mount stack reaches it through the FS/Volume interfaces in
// runtime.go, which `cmd/plori_mount.go` implements. That keeps Plori policy
// out of generic JuiceFS code (ADR D4) and lets the whole state machine be
// tested without FUSE.
package mount

import "github.com/juicedata/juicefs/pkg/plori/mountspec"

// The MountSpec wire contract lives in pkg/plori/mountspec, which carries no
// build tag. This file re-exports it under the names this package has always
// used, so the supervisor and `cmd/plori_mount.go` read the same as before.
//
// The split is not cosmetic. Everything below is plain data plus the validation
// that needs nothing but the spec itself, and the OTHER end of the contract has
// to be able to decode it: plori-runtime's services/storage-worker links this
// fork and asserts, in its own suite, that the control-plane's generated golden
// spec decodes here with its fields intact. Behind the `plori` tag that
// assertion cannot compile, and its absence is exactly how the two ends drifted
// until the worker could not decode a single real spec — `format` against
// `format_spec`, no `may_format`, exit 64 on every mount (PLO-395).
//
// Aliases rather than wrappers: a wrapper type would be a third definition of
// the wire, which is the problem, not the fix.
type (
	// MountSpec is the whole authority a trusted mount worker receives for one
	// writer generation of one Agent's volume.
	MountSpec = mountspec.MountSpec
	// FormatSpec is `juicefs format`'s input, mirrored from the control-plane's
	// storagelifecycle.FormatSpec.
	FormatSpec = mountspec.FormatSpec
	// DurablePointSpec is the control-plane's copy of the recovery anchor.
	DurablePointSpec = mountspec.DurablePointSpec
	// GrantSpec is the quota ceiling the allocator issued for this volume.
	GrantSpec = mountspec.GrantSpec
	// ObjectStore names the bucket the worker must open. It carries no credential.
	ObjectStore = mountspec.ObjectStore
	// Duration serialises as a Go duration string ("30s").
	Duration = mountspec.Duration
	// MountOptions is the resolved form of the MountSpec's `mount_options`.
	MountOptions = mountspec.MountOptions
)

// LoadSpec reads and validates a MountSpec file.
var LoadSpec = mountspec.Load

// ParseMountOptions resolves the closed `mount_options` vocabulary over the
// defaults.
var ParseMountOptions = mountspec.ParseMountOptions

// ErrSpec marks every refusal that must exit with CodeSpecInvalid. It is the
// same error value the wire package wraps, so `errors.Is` still holds across
// the split (exit.go Classify depends on it).
var ErrSpec = mountspec.ErrSpec

const (
	// CredentialSourceNodeSecret is the single member of the credential-source
	// vocabulary: the object key is already on the node.
	CredentialSourceNodeSecret = mountspec.CredentialSourceNodeSecret

	// Volume lifecycle states this worker recognises.
	VolumeStateAllocating = mountspec.VolumeStateAllocating
	VolumeStateFormatted  = mountspec.VolumeStateFormatted
	VolumeStateActive     = mountspec.VolumeStateActive

	// Format constants of the Plori profile — knobs `juicefs format` takes and
	// the control-plane does not issue.
	FormatStorage     = mountspec.FormatStorage
	FormatBlockSizeKB = mountspec.FormatBlockSizeKB

	// The measured mount-option defaults (PLO-316 wave 2).
	DefaultHeartbeat       = mountspec.DefaultHeartbeat
	DefaultBufferSizeMB    = mountspec.DefaultBufferSizeMB
	DefaultBarrierInterval = mountspec.DefaultBarrierInterval
	DefaultLitestreamSync  = mountspec.DefaultLitestreamSync
	DefaultTrashDays       = mountspec.DefaultTrashDays

	// The writeback backlog bounds (PLO-383).
	DefaultMaxStagingBacklog = mountspec.DefaultMaxStagingBacklog
	DefaultDrainPerBlock     = mountspec.DefaultDrainPerBlock
	MaxProjectedDrain        = mountspec.MaxProjectedDrain
)
