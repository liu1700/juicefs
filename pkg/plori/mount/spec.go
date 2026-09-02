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

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"time"
)

// CredentialSourceNodeSecret is the single member of the credential-source
// vocabulary. It means: the object key is already on the node, mounted into
// this process as a Kubernetes Secret, and the MountSpec carries none.
//
// A worker that does not recognise the value MUST refuse to mount rather than
// fall back to any other source; that fail-closed rule is what stops a future
// second delivery mode from silently downgrading an old worker
// (docs/design/per-agent-juicefs/mountspec.md §5).
const CredentialSourceNodeSecret = "node_secret"

// Volume lifecycle states this worker recognises. `active` is the only state a
// writable mount may be served from; `formatted` and `allocating` are the two
// states a first-boot format may complete from.
const (
	VolumeStateAllocating = "allocating"
	VolumeStateFormatted  = "formatted"
	VolumeStateActive     = "active"
)

// Duration serialises as a Go duration string ("30s"), mirroring
// storagespec.Duration on the control-plane side so one spelling crosses the
// whole path. This type is a copy on purpose: the fork must not import
// plori-runtime.
type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) {
	return []byte(`"` + time.Duration(d).String() + `"`), nil
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(parsed)
	return nil
}

// D is the plain time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

// GrantSpec is the quota ceiling the allocator issued for this volume plus the
// epoch the worker must acknowledge once it has applied it locally.
type GrantSpec struct {
	Bytes      int64 `json:"bytes"`
	Inodes     int64 `json:"inodes"`
	Epoch      int64 `json:"epoch"`
	AckedEpoch int64 `json:"acked_epoch"`
}

// ObjectStore names the bucket the worker must open. It carries no credential.
type ObjectStore struct {
	Endpoint         string `json:"endpoint"`
	Bucket           string `json:"bucket"`
	Region           string `json:"region,omitempty"`
	CredentialSource string `json:"credential_source"`
}

// Format constants of the Plori profile. `juicefs format` takes three more
// knobs than the control-plane issues, and they are constants here rather than
// wire fields on purpose: a field the server never sends is a field the two
// sides can disagree about for free, which is the class of bug PLO-395 was.
const (
	// FormatStorage is the object-storage driver every per-Agent volume is
	// formatted with. The Plori release profile registers no other remote
	// backend, so this is a constant rather than a choice.
	FormatStorage = "s3"
	// FormatBlockSizeKB is `--block-size`, in KiB, at JuiceFS's own default
	// (cmd/format.go:154). Changing it after the fact is impossible, so it is
	// pinned here where a change is a code review rather than a config edit.
	FormatBlockSizeKB = 4096
)

// FormatSpec is `juicefs format`'s whole input for one per-Agent volume,
// mirrored field-for-field from the control-plane's
// storagelifecycle.FormatSpec. The control-plane is the AUTHORITY on this
// wire and this struct is a copy of it; plori-runtime's
// services/storage-worker decodes the control-plane's own golden spec with
// this type so the copy cannot drift again (PLO-395).
//
// It carries NO credential. The worker already holds the bucket key from its
// node Secret (ADR §5 C1); a key here would be one in a struct that crosses a
// process boundary, gets logged, and lands in the replicated SQLite.
type FormatSpec struct {
	// VolumeID is the volume row's id. It is NOT `juicefs format`'s NAME
	// argument on its own — see (*MountSpec).VolumeName for why the name has to
	// carry the data root too.
	VolumeID string `json:"volume_id"`
	// Bucket is `--bucket`: `<endpoint>/<bucket>` and nothing deeper, because
	// JuiceFS's S3 backend reads at most `[ENDPOINT]/[BUCKET]` and discards the
	// rest (pkg/object/s3.go). It is the same string this spec's object_store
	// composes, and validateFormat refuses a spec where the two disagree.
	Bucket string `json:"bucket"`
	// DataPrefix and MetaPrefix are the volume's two disjoint object roots, as
	// the control-plane derived them. MetaPrefix here is the metadata ROOT
	// (`agents-meta/<vid>/`), not this writer's epoch inside it — that is
	// MountSpec.MetaPrefix.
	DataPrefix string `json:"data_prefix"`
	MetaPrefix string `json:"meta_prefix"`
	// TrashDays is `--trash-days`, always >= 1: the Rank 1 crash-consistency
	// protocol restores through the trash, so 0 is not a tunable value here.
	TrashDays int `json:"trash_days"`
	// CapacityBytes and Inodes are the account allocator's hard ceiling for
	// this volume. Zero means unlimited, which only an ungranted volume has —
	// and such a volume is not mountable.
	CapacityBytes int64 `json:"capacity_bytes"`
	Inodes        int64 `json:"inodes"`
	// GrantEpoch is the grant those numbers came from, echoed back on
	// /grant-ack so the allocator can tell an issued ceiling from an enforced
	// one.
	GrantEpoch int64 `json:"grant_epoch"`
	// ExpectedUUID is the Format.UUID this volume already has, and its
	// emptiness is what MayFormat reports. The worker compares it against the
	// Format it restores and refuses on a mismatch; it never formats over it.
	ExpectedUUID string `json:"expected_uuid,omitempty"`
}

// DurablePointSpec is the control-plane's copy of the recovery anchor: the
// pre-barrier wall clock T_before everything written before is provably durable
// past, the replica transaction observed at that barrier, and the fencing epoch
// that produced both — which is the epoch segment of RestoreFromPrefix.
//
// Same three fields as the local DurablePoint this worker writes to its state
// dir (health.go); a separate type because this one is a wire contract the
// server owns and the local one is a file this process owns.
type DurablePointSpec struct {
	DurableAt   time.Time `json:"durable_at"`
	ReplicaTxID string    `json:"replica_txid,omitempty"`
	FenceEpoch  int64     `json:"fence_epoch"`
}

// MountSpec is the whole authority a trusted mount worker receives for one
// writer generation of one Agent's volume. Field-for-field the control-plane's
// storagespec.MountSpec; see docs/design/per-agent-juicefs/mountspec.md §3.
type MountSpec struct {
	StorageVolumeID string `json:"storage_volume_id"`
	FormatUUID      string `json:"format_uuid"`
	Generation      int32  `json:"generation"`
	VolumeState     string `json:"volume_state"`

	FenceEpoch         int64     `json:"fence_epoch"`
	LeaseExpiresAt     time.Time `json:"lease_expires_at"`
	LeaseRenewInterval Duration  `json:"lease_renew_interval"`
	WriteStopMargin    Duration  `json:"write_stop_margin"`

	DataPrefix     string `json:"data_prefix"`
	MetaPrefix     string `json:"meta_prefix"`
	FenceMarkerKey string `json:"fence_marker_key"`

	// RestoreFromPrefix is the metadata prefix this generation must restore
	// FROM — the prefix of the epoch whose replica produced DurablePoint below.
	// MetaPrefix names only the prefix this writer replicates INTO, which is
	// empty by construction at startup.
	//
	// Empty means the control-plane has no recorded durable point for the
	// volume and therefore no source to name; the worker then discovers one by
	// listing the metadata root (Fencer.PriorMetaPrefix), which is what it did
	// for every mount before this field existed. The server does NOT fall back
	// to `epoch - 1`: an epoch that replicated nothing leaves a prefix holding
	// only its fence marker, and restoring from that would replace a live
	// filesystem with an empty one (PLO-391).
	RestoreFromPrefix string `json:"restore_from_prefix,omitempty"`
	// DurablePoint is the last barrier-backed restore point the control-plane
	// was told about. Nil when there is none.
	//
	// The worker persists its own copy next to the state dir, so this field
	// matters on exactly the case that copy cannot cover: a Pod rescheduled
	// onto a DIFFERENT node, where restoring without an anchor means restoring
	// the newest transaction in the replica rather than the newest provably
	// durable one.
	DurablePoint *DurablePointSpec `json:"durable_point,omitempty"`

	Grant       GrantSpec   `json:"grant"`
	ObjectStore ObjectStore `json:"object_store"`

	// Format is everything `juicefs format` needs, always sent. It is embedded
	// rather than re-derived from the rest of the spec so that ONE side decides
	// what a per-Agent volume's Format looks like; every other tuning knob
	// arrives through MountOptions, whose vocabulary is in options.go.
	Format FormatSpec `json:"format"`
	// MayFormat is the control-plane's AUTHORISATION to run `juicefs format`,
	// true exactly when the volume has never been formatted. It is a field
	// rather than an inference from an empty Format.ExpectedUUID because an
	// authorisation both sides infer from the absence of a value is one a
	// future rename silently grants.
	MayFormat bool `json:"may_format"`

	MountOptions []string `json:"mount_options"`

	IssuedAt time.Time `json:"issued_at"`
}

// ErrSpec marks every refusal that must exit with CodeSpecInvalid.
var ErrSpec = errors.New("mount spec")

// LoadSpec reads and validates a MountSpec file. Unknown fields are refused:
// the contract's exit code 64 covers "unknown field the worker must not
// ignore", and silently dropping a field a newer control-plane added is
// exactly the downgrade the closed credential vocabulary exists to prevent.
func LoadSpec(path string) (*MountSpec, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("%w: open: %w", ErrSpec, err)
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()
	var spec MountSpec
	if err := dec.Decode(&spec); err != nil {
		return nil, fmt.Errorf("%w: decode: %w", ErrSpec, err)
	}
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	return &spec, nil
}

// Validate rejects a spec the worker must not act on. Everything here is a
// refusal the plugin maps to exit 64 — a malformed authority, not a runtime
// failure.
func (s *MountSpec) Validate() error {
	if s.StorageVolumeID == "" {
		return fmt.Errorf("%w: storage_volume_id is empty", ErrSpec)
	}
	if s.FenceEpoch <= 0 {
		return fmt.Errorf("%w: fence_epoch must be positive, got %d", ErrSpec, s.FenceEpoch)
	}
	if s.LeaseExpiresAt.IsZero() {
		return fmt.Errorf("%w: lease_expires_at is unset", ErrSpec)
	}
	if s.LeaseRenewInterval <= 0 {
		return fmt.Errorf("%w: lease_renew_interval must be positive", ErrSpec)
	}
	if s.WriteStopMargin <= 0 {
		return fmt.Errorf("%w: write_stop_margin must be positive", ErrSpec)
	}
	// The worker re-checks the server's own inequality rather than trusting
	// it. mountspec.md §6 has the control-plane refuse to start on a bad
	// timing set; a spec that reaches here violating it means the two sides
	// disagree, and a lease whose margin is a lie is worse than no mount.
	if s.LeaseRenewInterval.D() >= s.WriteStopMargin.D() {
		return fmt.Errorf("%w: lease_renew_interval %s must be shorter than write_stop_margin %s",
			ErrSpec, s.LeaseRenewInterval.D(), s.WriteStopMargin.D())
	}
	for name, prefix := range map[string]string{
		"data_prefix": s.DataPrefix,
		"meta_prefix": s.MetaPrefix,
	} {
		if err := validPrefix(name, prefix); err != nil {
			return err
		}
	}
	if !strings.HasPrefix(s.FenceMarkerKey, s.MetaPrefix) {
		return fmt.Errorf("%w: fence_marker_key %q is not under meta_prefix %q",
			ErrSpec, s.FenceMarkerKey, s.MetaPrefix)
	}
	// ADR D2 as amended (#2093): the two roots are siblings, never nested. A
	// replica living under the data root would trip `format --create`'s
	// non-empty check, and a data-scoped credential policy must not reach the
	// replica — the replica *is* the filesystem.
	if strings.HasPrefix(s.MetaPrefix, s.DataPrefix) || strings.HasPrefix(s.DataPrefix, s.MetaPrefix) {
		return fmt.Errorf("%w: data_prefix %q and meta_prefix %q must be disjoint",
			ErrSpec, s.DataPrefix, s.MetaPrefix)
	}
	// `restore_from_prefix` and `durable_point` are two halves of one
	// instruction — restore THIS prefix TO THIS point — and half of it is worse
	// than none: a prefix with no anchor restores the newest transaction it
	// holds rather than the newest durable one, and an anchor with no prefix
	// has nothing to apply to. The control-plane sends both or neither for that
	// reason (PLO-391); a spec that arrives with one means the two sides
	// disagree about the restore, and this worker refuses rather than acting on
	// the half it got. Neither present is the normal case on a fresh volume:
	// the source is then discovered by listing.
	if err := s.validateRestoreInstruction(); err != nil {
		return err
	}
	if s.ObjectStore.CredentialSource != CredentialSourceNodeSecret {
		return fmt.Errorf("%w: unsupported credential_source %q (this worker only understands %q)",
			ErrSpec, s.ObjectStore.CredentialSource, CredentialSourceNodeSecret)
	}
	if s.ObjectStore.Endpoint == "" || s.ObjectStore.Bucket == "" {
		return fmt.Errorf("%w: object_store endpoint and bucket are required", ErrSpec)
	}
	switch s.VolumeState {
	case VolumeStateActive, VolumeStateFormatted, VolumeStateAllocating:
	default:
		return fmt.Errorf("%w: volume_state %q is not a state a writer may be served from",
			ErrSpec, s.VolumeState)
	}
	for _, opt := range s.MountOptions {
		if strings.ContainsAny(opt, "\x00\n") {
			return fmt.Errorf("%w: mount_options entry contains a control character", ErrSpec)
		}
	}
	return s.validateFormat()
}

// validateFormat checks the embedded format block against the rest of the spec.
//
// Every field in it is also spelled somewhere else on this wire — the volume id,
// the two prefixes, the bucket, the recorded Format.UUID — so the only way the
// two spellings can disagree is a control-plane that contradicted itself. Acting
// on half of a contradiction is how a mount formats against one bucket and
// replicates to another, so a disagreement is exit 64 rather than a preference.
func (s *MountSpec) validateFormat() error {
	f := s.Format
	if f.TrashDays < DefaultTrashDays {
		return fmt.Errorf("%w: format.trash_days %d is below the crash-consistency floor of %d",
			ErrSpec, f.TrashDays, DefaultTrashDays)
	}
	if f.VolumeID != s.StorageVolumeID {
		return fmt.Errorf("%w: format.volume_id %q is not this spec's volume %q",
			ErrSpec, f.VolumeID, s.StorageVolumeID)
	}
	if f.DataPrefix != s.DataPrefix {
		return fmt.Errorf("%w: format.data_prefix %q does not match the spec's data_prefix %q",
			ErrSpec, f.DataPrefix, s.DataPrefix)
	}
	// The control-plane's format block carries the metadata ROOT; the spec's
	// own meta_prefix is this writer's epoch inside it (storagevol.MetaRootPrefix
	// against storagevol.MetaPrefix). Comparing them the wrong way round would
	// pass on every volume and mean nothing.
	if f.MetaPrefix != s.MetaRoot() {
		return fmt.Errorf("%w: format.meta_prefix %q is not this volume's metadata root %q",
			ErrSpec, f.MetaPrefix, s.MetaRoot())
	}
	if want := formatBucketURL(s.ObjectStore.Endpoint, s.ObjectStore.Bucket); f.Bucket != want {
		return fmt.Errorf("%w: format.bucket %q does not name the spec's object store %q",
			ErrSpec, f.Bucket, want)
	}
	if f.ExpectedUUID != s.FormatUUID {
		return fmt.Errorf("%w: format.expected_uuid %q and format_uuid %q are two spellings of one fact and disagree",
			ErrSpec, f.ExpectedUUID, s.FormatUUID)
	}
	// MayFormat is the authorisation and ExpectedUUID is the reason for it. A
	// spec that grants the first without the second is a licence to format over
	// a filesystem that already exists.
	if s.MayFormat != (f.ExpectedUUID == "") {
		return fmt.Errorf("%w: may_format is %t but format.expected_uuid is %q",
			ErrSpec, s.MayFormat, f.ExpectedUUID)
	}
	if f.CapacityBytes < 0 || f.Inodes < 0 {
		return fmt.Errorf("%w: format ceiling is negative (%d bytes / %d inodes)",
			ErrSpec, f.CapacityBytes, f.Inodes)
	}
	return nil
}

// formatBucketURL composes `--bucket` from the two object-store coordinates,
// byte-for-byte as the control-plane does (storagevol.FormatBucketURL). It is a
// copy of that derivation rather than a looser comparison so that the check
// above is a drift guard and not a trap for a trailing slash in a deployment's
// endpoint.
func formatBucketURL(endpoint, bucket string) string {
	endpoint = strings.TrimRight(strings.TrimSpace(endpoint), "/")
	bucket = strings.Trim(strings.TrimSpace(bucket), "/")
	if endpoint == "" || bucket == "" {
		return ""
	}
	return endpoint + "/" + bucket
}

// validateRestoreInstruction checks the rev-3 pair. Everything it refuses is a
// server that contradicted itself, which is exit 64 territory: the alternative
// is mounting on a restore instruction nobody can vouch for.
func (s *MountSpec) validateRestoreInstruction() error {
	dp := s.DurablePoint
	if s.RestoreFromPrefix == "" && dp == nil {
		return nil
	}
	if s.RestoreFromPrefix == "" || dp == nil {
		return fmt.Errorf("%w: restore_from_prefix and durable_point must be sent together, got %q and %v",
			ErrSpec, s.RestoreFromPrefix, dp)
	}
	if err := validPrefix("restore_from_prefix", s.RestoreFromPrefix); err != nil {
		return err
	}
	if !strings.HasPrefix(s.RestoreFromPrefix, s.MetaRoot()) {
		return fmt.Errorf("%w: restore_from_prefix %q is outside this volume's metadata root %q",
			ErrSpec, s.RestoreFromPrefix, s.MetaRoot())
	}
	if dp.FenceEpoch <= 0 {
		return fmt.Errorf("%w: durable_point.fence_epoch must be positive, got %d", ErrSpec, dp.FenceEpoch)
	}
	// A durable point from an epoch at or ahead of this one means another
	// writer is live at an epoch this worker was told it holds. That is the
	// fencing invariant, not a restore detail, so it fails the spec.
	if dp.FenceEpoch > s.FenceEpoch {
		return fmt.Errorf("%w: durable_point.fence_epoch %d is ahead of this writer's epoch %d",
			ErrSpec, dp.FenceEpoch, s.FenceEpoch)
	}
	if dp.DurableAt.IsZero() {
		return fmt.Errorf("%w: durable_point.durable_at is unset", ErrSpec)
	}
	// The prefix must name the epoch inside the point. Two spellings of one
	// fact is a spelling too many; if they disagree the restore would read one
	// epoch's replica and stop at another epoch's transaction.
	if want := s.MetaPrefixForEpoch(dp.FenceEpoch); s.RestoreFromPrefix != want {
		return fmt.Errorf("%w: restore_from_prefix %q does not name durable_point.fence_epoch %d (expected %q)",
			ErrSpec, s.RestoreFromPrefix, dp.FenceEpoch, want)
	}
	return nil
}

// MetaPrefixForEpoch is where the writer holding `epoch` replicates this
// volume's metadata: the same composition the control-plane performs
// (storagevol/prefix.go MetaPrefix). One spelling, derived from MetaRoot so it
// cannot drift from the prefix this writer was issued.
func (s *MountSpec) MetaPrefixForEpoch(epoch int64) string {
	return fmt.Sprintf("%sg%d/", s.MetaRoot(), epoch)
}

func validPrefix(name, prefix string) error {
	if prefix == "" {
		return fmt.Errorf("%w: %s is empty", ErrSpec, name)
	}
	if !strings.HasSuffix(prefix, "/") {
		return fmt.Errorf("%w: %s %q must end in /", ErrSpec, name, prefix)
	}
	if strings.HasPrefix(prefix, "/") {
		return fmt.Errorf("%w: %s %q must be relative to the bucket root", ErrSpec, name, prefix)
	}
	// path.Clean as an equality oracle, the same technique the control-plane
	// uses on target_path: a non-canonical prefix never reaches a prefix
	// comparison, so `agents/../agents-meta/…` cannot masquerade as data.
	if cleaned := path.Clean(prefix); cleaned+"/" != prefix {
		return fmt.Errorf("%w: %s %q is not canonical", ErrSpec, name, prefix)
	}
	return nil
}

// VolumeName is the JuiceFS Format.Name for this volume.
//
// JuiceFS composes every data key as `<Format.Name>/…`
// (cmd/format.go:286 `object.WithPrefix(blob, format.Name+"/")`), and the S3
// backend accepts no key prefix of its own — `newS3` reads at most
// `[ENDPOINT]/[BUCKET]` out of the bucket URL and discards any deeper path
// (pkg/object/s3.go). So the ONLY way to land chunks under the control-plane's
// `agents/<vid>/` data root is to make that whole string the volume name.
// storagevol/prefix.go's comment ("DataRoot is what Format.Bucket points at")
// does not hold for S3; this is the reconciliation, and identityMatches pins
// it so it cannot drift.
func (s *MountSpec) VolumeName() string { return strings.TrimSuffix(s.DataPrefix, "/") }

// MetaRoot is the volume's whole metadata subtree, the parent every writer
// generation's prefix hangs under. It is derived from the epoch-partitioned
// prefix the control-plane issued rather than carried separately, so the two
// cannot drift.
func (s *MountSpec) MetaRoot() string {
	trimmed := strings.TrimSuffix(s.MetaPrefix, "/")
	if i := strings.LastIndex(trimmed, "/"); i >= 0 {
		return trimmed[:i+1]
	}
	return trimmed
}
