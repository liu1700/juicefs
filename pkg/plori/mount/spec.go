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

// FormatSpec is the first-boot format contract. PLO-330 owns the server-side
// half; until it lands the control-plane omits the object entirely and the
// worker derives everything below from the rest of the spec (see
// (*MountSpec).EffectiveFormat).
type FormatSpec struct {
	TrashDays   int    `json:"trash_days"`
	BlockSizeKB int    `json:"block_size_kb,omitempty"`
	Compression string `json:"compression,omitempty"`
	// Storage is the JuiceFS object-storage driver name. The Plori profile
	// registers exactly two ("s3" for remote, "file" for the metadata-backup
	// staging path), so anything else is refused.
	Storage string `json:"storage,omitempty"`
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

	Grant       GrantSpec   `json:"grant"`
	ObjectStore ObjectStore `json:"object_store"`

	MountOptions []string `json:"mount_options"`

	IssuedAt time.Time `json:"issued_at"`

	// Format is the first-boot format contract (PLO-330). Optional.
	Format *FormatSpec `json:"format_spec,omitempty"`
	// BarrierInterval is how often the periodic durability barrier runs. Spec
	// driven so PLO-316's measurement can change it without a worker rollout;
	// zero means DefaultBarrierInterval.
	BarrierInterval Duration `json:"barrier_interval,omitempty"`
	// ReplicaSyncInterval is Litestream's sync interval. Zero means
	// DefaultReplicaSyncInterval.
	ReplicaSyncInterval Duration `json:"replica_sync_interval,omitempty"`
	// UsageReportEvery is how many renew ticks pass between /usage reports.
	// Zero means DefaultUsageReportEvery.
	UsageReportEvery int `json:"usage_report_every,omitempty"`
}

// Defaults for the spec-driven knobs the control-plane does not issue yet.
const (
	// DefaultBarrierInterval is the periodic `juicefs durability` cadence.
	// 60 s is the middle of the three periods PLO-316 is measuring
	// (15/60/300 s stall cost); it is a placeholder, not a measurement.
	DefaultBarrierInterval = 60 * time.Second
	// DefaultReplicaSyncInterval matches the M0 harness (one LTX object per
	// second, wave-2 brief blocker 2).
	DefaultReplicaSyncInterval = time.Second
	// DefaultUsageReportEvery reports usage every 15th renew: at a 20 s renew
	// interval that is one /usage call per five minutes per mount.
	DefaultUsageReportEvery = 15
	// DefaultTrashDays is the crash-consistency D1 floor. A volume formatted
	// with trash-days 0 cannot satisfy crash-consistency.md §7 Rank 1, so the
	// worker never formats one and refuses to mount one (exit 70).
	DefaultTrashDays = 1
)

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
	if s.UsageReportEvery < 0 {
		return fmt.Errorf("%w: usage_report_every must not be negative", ErrSpec)
	}
	if f := s.Format; f != nil {
		if f.TrashDays < DefaultTrashDays {
			return fmt.Errorf("%w: format_spec.trash_days %d is below the crash-consistency floor of %d",
				ErrSpec, f.TrashDays, DefaultTrashDays)
		}
		if f.Storage != "" && f.Storage != "s3" {
			return fmt.Errorf("%w: format_spec.storage %q is outside the Plori profile", ErrSpec, f.Storage)
		}
	}
	return nil
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

// EffectiveFormat is what a first-boot format uses. When the control-plane
// omits `format_spec` (PLO-330 has not shipped it yet) the worker derives the
// contract from the rest of the spec and the crash-consistency floor rather
// than refusing, because refusing would make every volume unformattable.
func (s *MountSpec) EffectiveFormat() FormatSpec {
	f := FormatSpec{TrashDays: DefaultTrashDays, Storage: "s3"}
	if s.Format != nil {
		f = *s.Format
		if f.TrashDays < DefaultTrashDays {
			f.TrashDays = DefaultTrashDays
		}
		if f.Storage == "" {
			f.Storage = "s3"
		}
	}
	return f
}

func (s *MountSpec) barrierInterval() time.Duration {
	if s.BarrierInterval > 0 {
		return s.BarrierInterval.D()
	}
	return DefaultBarrierInterval
}

func (s *MountSpec) replicaSyncInterval() time.Duration {
	if s.ReplicaSyncInterval > 0 {
		return s.ReplicaSyncInterval.D()
	}
	return DefaultReplicaSyncInterval
}

func (s *MountSpec) usageReportEvery() int {
	if s.UsageReportEvery > 0 {
		return s.UsageReportEvery
	}
	return DefaultUsageReportEvery
}

// EffectiveMountOptions applies the PLORI_MOUNT_OPTIONS operator escape hatch.
// The override replaces the server's list wholesale rather than merging: a
// merge would make the resulting kernel mount string a function of two
// authorities and neither would be auditable.
func (s *MountSpec) EffectiveMountOptions(env func(string) string) []string {
	if raw := env("PLORI_MOUNT_OPTIONS"); raw != "" {
		var out []string
		for _, part := range strings.Split(raw, ",") {
			if part = strings.TrimSpace(part); part != "" {
				out = append(out, part)
			}
		}
		return out
	}
	return append([]string(nil), s.MountOptions...)
}
