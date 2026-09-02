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

// LeaseResponse is the answer to renew and release.
type LeaseResponse struct {
	StorageVolumeID string    `json:"storage_volume_id"`
	FenceEpoch      int64     `json:"fence_epoch"`
	LeaseExpiresAt  time.Time `json:"lease_expires_at"`
	Grant           GrantSpec `json:"grant"`
	Released        bool      `json:"released"`
}

// Client speaks the five follow-up routes of /v1/internal/storage. It never
// calls /mount-spec: by the time the worker runs, the plugin has already
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

func (c *Client) RenewLease(ctx context.Context, volumeID string, epoch int64) (LeaseResponse, error) {
	var out LeaseResponse
	err := c.post(ctx, "/v1/internal/storage/lease/renew", map[string]any{
		"volume_id":   volumeID,
		"fence_epoch": epoch,
	}, &out)
	return out, err
}

func (c *Client) ReleaseLease(ctx context.Context, volumeID string, epoch int64, reason string) error {
	return c.post(ctx, "/v1/internal/storage/lease/release", map[string]any{
		"volume_id":   volumeID,
		"fence_epoch": epoch,
		"reason":      reason,
	}, nil)
}

func (c *Client) ReportUsage(ctx context.Context, volumeID string, epoch int64, u Usage, at time.Time) error {
	return c.post(ctx, "/v1/internal/storage/usage", map[string]any{
		"volume_id":   volumeID,
		"fence_epoch": epoch,
		"used_bytes":  u.Bytes,
		"used_inodes": u.Inodes,
		"observed_at": at,
	}, nil)
}

func (c *Client) ReportDurablePoint(ctx context.Context, volumeID string, epoch int64, r BarrierResult, replicaTxID string) error {
	return c.post(ctx, "/v1/internal/storage/durable-point", map[string]any{
		"volume_id":    volumeID,
		"fence_epoch":  epoch,
		"durable_at":   r.DurableAt,
		"barrier_at":   r.BarrierAt,
		"replica_txid": replicaTxID,
	}, nil)
}

func (c *Client) AckGrant(ctx context.Context, volumeID string, epoch, grantEpoch int64) error {
	return c.post(ctx, "/v1/internal/storage/grant-ack", map[string]any{
		"volume_id":   volumeID,
		"fence_epoch": epoch,
		"grant_epoch": grantEpoch,
	}, nil)
}
