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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/juicedata/juicefs/pkg/plori/mountspec"
)

// Refusal codes from docs/design/per-agent-juicefs/mountspec.md §3. A caller
// branches on these, never on the HTTP status alone, because two distinct
// refusals share 409.
const (
	CPCodeTokenInvalid         = "token_invalid"
	CPCodeIdentityMismatch     = "identity_mismatch"
	CPCodeBadRequest           = "bad_request"
	CPCodeVolumeNotProvisioned = "volume_not_provisioned"
	CPCodeVolumeNotActive      = "volume_not_active"
	CPCodeLeaseHeld            = "lease_held"
	CPCodeStaleEpoch           = "stale_epoch"
	CPCodeRateLimited          = "rate_limited"
	CPCodeNotConfigured        = "not_configured"
	CPCodeInternal             = "internal"
	// CPCodeFormatMismatch answers /format-ack when the volume already carries a
	// DIFFERENT Format.UUID. Two filesystems on one object prefix is ADR D2's
	// data loss, so it is terminal on sight: a worker that retried it would be
	// asking to be told a second time that it is serving the wrong volume.
	CPCodeFormatMismatch = "format_mismatch"
)

// CPError is a typed refusal from the control-plane.
type CPError struct {
	Status int
	Code   string
	Msg    string
}

func (e *CPError) Error() string {
	return fmt.Sprintf("control-plane %d %s: %s", e.Status, e.Code, e.Msg)
}

// Fenced reports whether this refusal means the worker has lost the epoch.
// Both members are terminal: mountspec.md says stale_epoch means "the
// presented epoch was moved past or never issued", and lease_held on a renew
// means someone else holds the volume. Neither is retryable, and retrying
// either is how a fenced writer keeps writing.
func (e *CPError) Fenced() bool {
	return e.Code == CPCodeStaleEpoch || e.Code == CPCodeLeaseHeld || e.Code == CPCodeIdentityMismatch
}

// Retryable reports whether presenting the same request again could get a
// different answer. Exactly two refusals can: the control-plane did not produce
// one (5xx — a replica rolling, a database failing over), and it asked the
// caller to slow down. Every other status is its considered answer about this
// volume, and asking again only gets it a second time.
func (e *CPError) Retryable() bool {
	return e.Status >= http.StatusInternalServerError || e.Code == CPCodeRateLimited
}

// RenewRequest is what the worker tells the control-plane on a renew beyond
// "I am still here". Both fields are about the grant, and both ride the renew
// because renewal is the only regular round trip a live mount makes: one call,
// one authorisation, one place where a fenced writer stops being heard.
type RenewRequest struct {
	// AckedGrantEpoch is the grant epoch this worker has applied locally, or 0
	// when there is nothing new to acknowledge. The allocator counts an
	// un-acknowledged grant as reserved but not enforced, so this is what lets
	// it tell an issued ceiling from a live one.
	AckedGrantEpoch int64 `json:"acked_grant_epoch,omitempty"`
	// Grow asks for one more increment because the volume ceiling has refused
	// an operation since the last renew. It carries no size: how much a volume
	// may grow is an account-budget decision, and a number chosen by the mount
	// would be request input deciding an allocation (threat-model R14). The
	// worker says "I am full"; the control-plane says how much that is worth.
	Grow bool `json:"grow,omitempty"`
}

// LeaseResponse is the answer to renew and release.
type LeaseResponse struct {
	StorageVolumeID string    `json:"storage_volume_id"`
	FenceEpoch      int64     `json:"fence_epoch"`
	LeaseExpiresAt  time.Time `json:"lease_expires_at"`
	Grant           GrantSpec `json:"grant"`
	Released        bool      `json:"released"`
	// OverBudget answers a Grow the account could not fund: the grant epoch is
	// unchanged, the worker keeps the ceiling it has, and the user sees the
	// quota-full surface (PLO-337). It is deliberately not an error — a renew
	// that failed because the account is full would fence a mount over a
	// billing state.
	OverBudget bool `json:"over_budget"`
}

// ClientRoutes is every route this Client speaks, declared once in the untagged
// wire package so the control-plane's own published surface can be compared
// against it from a module that cannot compile behind the `plori` tag.
var ClientRoutes = mountspec.ClientRoutes

// Client speaks the five follow-up routes of /v1/internal/storage — renew,
// release, usage, durable-point and format-ack (mountspec.ClientRoutes). It
// never calls /mount-spec: by the time the worker runs, the plugin has already
// spent that call and the resulting spec is in --spec-file.
type Client struct {
	BaseURL   string
	TokenFile string
	HTTP      *http.Client
}

// NewClient builds a client with a timeout short enough that a hung
// control-plane cannot swallow a whole renew interval. mountspec.md §6 sizes
// the TTL for three lost renewals, so a renew must fail fast enough for the
// next attempt to still fit inside the margin.
func NewClient(baseURL, tokenFile string, timeout time.Duration) *Client {
	return &Client{
		BaseURL:   strings.TrimSuffix(baseURL, "/"),
		TokenFile: tokenFile,
		HTTP:      &http.Client{Timeout: timeout},
	}
}

// token re-reads the projected ServiceAccount token from disk on every call.
// The kubelet rotates it in place; a cached token is a mount that stops being
// able to renew somewhere around the 80% mark of the token's lifetime.
func (c *Client) token() (string, error) {
	data, err := os.ReadFile(c.TokenFile)
	if err != nil {
		return "", fmt.Errorf("read token file: %w", err)
	}
	tok := strings.TrimSpace(string(data))
	if tok == "" {
		return "", errors.New("token file is empty")
	}
	return tok, nil
}

func (c *Client) post(ctx context.Context, route string, body, out any) error {
	tok, err := c.token()
	if err != nil {
		return err
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("encode %s request: %w", route, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+route, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("build %s request: %w", route, err)
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("%s: %w", route, err)
	}
	defer resp.Body.Close()
	// A refusal body is small and typed; cap the read so a misrouted response
	// cannot be used to grow this process's heap.
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("%s: read response: %w", route, err)
	}
	if resp.StatusCode != http.StatusOK {
		var e struct {
			Code  string `json:"code"`
			Error string `json:"error"`
		}
		_ = json.Unmarshal(data, &e)
		if e.Code == "" {
			e.Code = CPCodeInternal
		}
		return &CPError{Status: resp.StatusCode, Code: e.Code, Msg: e.Error}
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("%s: decode response: %w", route, err)
	}
	return nil
}

func (c *Client) RenewLease(ctx context.Context, volumeID string, epoch int64, req RenewRequest) (LeaseResponse, error) {
	var out LeaseResponse
	err := c.post(ctx, mountspec.RouteLeaseRenew, map[string]any{
		"volume_id":         volumeID,
		"fence_epoch":       epoch,
		"acked_grant_epoch": req.AckedGrantEpoch,
		"grow":              req.Grow,
	}, &out)
	return out, err
}

func (c *Client) ReleaseLease(ctx context.Context, volumeID string, epoch int64, reason string) error {
	return c.post(ctx, mountspec.RouteLeaseRelease, map[string]any{
		"volume_id":   volumeID,
		"fence_epoch": epoch,
		"reason":      reason,
	}, nil)
}

func (c *Client) ReportUsage(ctx context.Context, volumeID string, epoch int64, u Usage, at time.Time) error {
	return c.post(ctx, mountspec.RouteUsage, map[string]any{
		"volume_id":   volumeID,
		"fence_epoch": epoch,
		"used_bytes":  u.Bytes,
		"used_inodes": u.Inodes,
		"observed_at": at,
	}, nil)
}

func (c *Client) ReportDurablePoint(ctx context.Context, volumeID string, epoch int64, r BarrierResult, replicaTxID string) error {
	return c.post(ctx, mountspec.RouteDurablePoint, map[string]any{
		"volume_id":    volumeID,
		"fence_epoch":  epoch,
		"durable_at":   r.DurableAt,
		"barrier_at":   r.BarrierAt,
		"replica_txid": replicaTxID,
	}, nil)
}

// VolumeStateResponse is what /format-ack answers with: where the control-plane
// put the volume, and the ceiling it carries once it is there.
type VolumeStateResponse struct {
	StorageVolumeID string    `json:"storage_volume_id"`
	State           string    `json:"state"`
	Grant           GrantSpec `json:"grant"`
	UsedBytes       int64     `json:"used_bytes"`
	UsedInodes      int64     `json:"used_inodes"`
}

// AckFormat reports the Format.UUID this volume's filesystem carries. It is the
// other half of the FormatSpec the MountSpec delivered: the control-plane
// authorises the format, this worker executes it, and this call is what tells
// the control-plane which filesystem now exists — the transition that moves a
// generation-1 volume `allocating -> formatted -> active` and, with it, the one
// that makes the Agent's own storage the storage the Files panel reads
// (PLO-373, PLO-420).
//
// It is idempotent on the same UUID (a replayed ack answers 200 with the state
// the volume has since reached) and terminal on a different one
// (409 format_mismatch).
func (c *Client) AckFormat(ctx context.Context, volumeID string, epoch int64, formatUUID string) (VolumeStateResponse, error) {
	var out VolumeStateResponse
	err := c.post(ctx, mountspec.RouteFormatAck, map[string]any{
		"volume_id":   volumeID,
		"fence_epoch": epoch,
		"format_uuid": formatUUID,
	}, &out)
	return out, err
}
