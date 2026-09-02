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
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Ready is the readiness file the plugin polls. It is written once, after the
// whole startup chain has succeeded, and never rewritten.
type Ready struct {
	Epoch     int64     `json:"epoch"`
	MountedAt time.Time `json:"mounted_at"`
	Volume    string    `json:"volume"`
}

// Health is rewritten on every renew tick. Field names are the CLI contract's
// ("Health" section); the plugin exposes them through its metrics endpoint,
// which PLO-325 will consume.
type Health struct {
	Epoch             int64     `json:"epoch"`
	LeaseExpiresAt    time.Time `json:"lease_expires_at"`
	LastRenewOK       bool      `json:"last_renew_ok"`
	ReplicaLagMs      int64     `json:"replica_lag_ms"`
	PendingBlocks     uint64    `json:"pending_blocks"`
	LastBarrierAt     time.Time `json:"last_barrier_at"`
	UsedBytes         int64     `json:"used_bytes"`
	UsedInodes        int64     `json:"used_inodes"`
	GrantEpochApplied int64     `json:"grant_epoch_applied"`
	// QuotaExhausted is true from the moment the volume ceiling refuses an
	// operation until a larger grant epoch is applied. It is what tells an
	// operator (and PLO-325's metrics) the difference between an Agent that is
	// idle and one that is stuck against a ceiling the account cannot raise —
	// which, with a grant conversation that is otherwise invisible, is
	// otherwise indistinguishable from a healthy mount.
	QuotaExhausted bool `json:"quota_exhausted"`
	Fenced         bool `json:"fenced"`
	// CredentialRefreshFailed is true while the worker is running on the last
	// object key it managed to read, because the current read of the
	// credential file fails or the store is refusing the key it produced. It
	// is a warning, not a failure: the worker keeps serving until
	// CredentialRejectGrace runs out, and this is the only signal an operator
	// has that a rotation is halfway through (PLO-322).
	CredentialRefreshFailed bool `json:"credential_refresh_failed"`
	// CredentialGeneration counts the object keys this worker has run on,
	// starting at 1. It is how a rotation drill answers "has the fleet picked
	// the new key up yet" without anything having to name the key. A worker
	// whose credential cannot rotate at all (the environment-variable path)
	// stays at 1 forever, which is the same signal read the other way.
	CredentialGeneration int64 `json:"credential_generation"`
}

// DurablePoint is the persisted recovery anchor.
//
// crash-consistency.md §5 rules out the two values that look like anchors:
// DurabilityStatus.LastSuccessfulBarrierUnixMs is a barrier COMPLETION time
// (pkg/chunk/cached_store.go:1302-1303), and Fence is a per-process in-memory
// sequence that means nothing across restarts (:1122-1141). The anchor is
// T_before — wall clock captured before the barrier started — because
// everything written before it is provably durable once the barrier returns,
// while anything written during the barrier may not be.
type DurablePoint struct {
	Volume      string    `json:"volume"`
	FenceEpoch  int64     `json:"fence_epoch"`
	DurableAt   time.Time `json:"durable_at"`
	BarrierAt   time.Time `json:"barrier_at"`
	ReplicaTxID string    `json:"replica_txid"`
}

// writeJSONAtomic writes 0600 through a temp file and a rename, so a reader
// (the plugin polling `ready`, or the next generation reading the durable
// point) never observes a half-written document.
func writeJSONAtomic(path string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp in %s: %w", dir, err)
	}
	name := tmp.Name()
	defer os.Remove(name)
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(append(data, '\n')); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}

// ReadDurablePoint returns the anchor a previous generation left behind on
// this node, if any. A missing file is not an error: a fresh node has none,
// which is exactly why the control-plane also records it.
func ReadDurablePoint(path string) (*DurablePoint, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var dp DurablePoint
	if err := json.Unmarshal(data, &dp); err != nil {
		return nil, fmt.Errorf("decode durable point: %w", err)
	}
	return &dp, nil
}
