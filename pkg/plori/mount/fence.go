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
	"sort"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// ErrFenceMarkerHeld means the store refused the conditional PUT: another
// writer already claimed this epoch's metadata prefix.
var ErrFenceMarkerHeld = errors.New("fence marker already claimed")

// ErrFenceMarkerMissing means the marker key answered 404. A 412 followed by a
// 404 is a marker that was deleted between the two calls; it is not a claim
// this worker may take over.
var ErrFenceMarkerMissing = errors.New("fence marker not found")

// FenceMarker is the body of `agents-meta/<vid>/g<epoch>/fence`.
//
// The key already encodes the volume and the epoch, so the body repeating them
// is not redundancy for its own sake: it is what lets a worker that gets a 412
// decide whether the claim standing in its way is its own predecessor at the
// same epoch or a stranger. A body that disagrees with its key is a marker this
// worker refuses to reason about at all.
//
// Holder is the lease identity of the process that claimed it. It is empty
// today because the control-plane's MountSpec carries no holder — see the
// finding filed against PLO-395 — and the same-epoch reclaim therefore proves
// holder identity against the control-plane instead (Supervisor.reclaimOwnMarker).
type FenceMarker struct {
	Volume    string `json:"volume"`
	Epoch     int64  `json:"epoch"`
	Holder    string `json:"holder,omitempty"`
	ClaimedAt string `json:"claimed_at"`
}

// S3Fencer claims the epoch fence marker with `If-None-Match: *`.
//
// This is a direct AWS SDK call rather than a pkg/object one because
// object.ObjectStorage has no conditional-write verb: Put takes a key and a
// reader and nothing else, so there is no way to express the precondition
// that makes the marker a fence. The SDK is already a first-class dependency
// of the Plori profile (hack/verify_plori_sbom.py REQUIRED lists
// aws-sdk-go-v2/service/s3), so this adds no module.
//
// PLO-351 verified the production store enforces the precondition: a
// conditional PUT over an existing key returns 412.
type S3Fencer struct {
	client *s3.Client
	bucket string
}

// NewS3Fencer builds a fencer from the MountSpec's object-store coordinates
// and the process's credential provider.
//
// It takes the provider rather than a key pair so that the fencer and the data
// path cannot end up on different keys: the provider is the one the worker
// installed into pkg/object, and a rotation moves both at the same instant
// (PLO-322). The credential is never logged, never written to the spec, and
// never persisted (mountspec.md §5).
func NewS3Fencer(ctx context.Context, store ObjectStore, provider aws.CredentialsProvider) (*S3Fencer, error) {
	if provider == nil {
		return nil, fmt.Errorf("%w: no object credential provider", ErrSpec)
	}
	region := store.Region
	if region == "" {
		region = "us-east-1"
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(region),
		awsconfig.WithCredentialsProvider(provider),
	)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	endpoint := store.Endpoint
	if !strings.Contains(endpoint, "://") {
		endpoint = "https://" + endpoint
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		// Path style keeps one endpoint working for both a virtual-hosted
		// production bucket and the CI MinIO, which has no wildcard DNS.
		o.UsePathStyle = true
		o.RetryMaxAttempts = 3
	})
	return &S3Fencer{client: client, bucket: store.Bucket}, nil
}

// Claim writes the marker, failing closed on a precondition conflict.
func (f *S3Fencer) Claim(ctx context.Context, key string, body []byte) error {
	_, err := f.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(f.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(body),
		IfNoneMatch: aws.String("*"),
	})
	if err == nil {
		return nil
	}
	if isPreconditionFailed(err) {
		return fmt.Errorf("%w: %s", ErrFenceMarkerHeld, key)
	}
	return fmt.Errorf("claim fence marker: %w", err)
}

// ReadMarker fetches the marker standing at `key`. It is called only after a
// 412, to find out whose claim it is.
func (f *S3Fencer) ReadMarker(ctx context.Context, key string) (FenceMarker, error) {
	out, err := f.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFound(err) {
			return FenceMarker{}, fmt.Errorf("%w: %s", ErrFenceMarkerMissing, key)
		}
		return FenceMarker{}, fmt.Errorf("read fence marker: %w", err)
	}
	defer out.Body.Close()
	// The marker is a handful of bytes this worker's own predecessor wrote;
	// cap the read anyway so a misrouted object cannot grow the heap.
	body, err := io.ReadAll(io.LimitReader(out.Body, 64<<10))
	if err != nil {
		return FenceMarker{}, fmt.Errorf("read fence marker body: %w", err)
	}
	var m FenceMarker
	if err := json.Unmarshal(body, &m); err != nil {
		return FenceMarker{}, fmt.Errorf("decode fence marker %s: %w", key, err)
	}
	return m, nil
}

// isNotFound recognises the 404 a GET of an absent key produces.
func isNotFound(err error) bool {
	var respErr interface{ HTTPStatusCode() int }
	if errors.As(err, &respErr) && respErr.HTTPStatusCode() == http.StatusNotFound {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NoSuchKey", "NotFound":
			return true
		}
	}
	return false
}

// isPreconditionFailed recognises the 412 the conditional PUT produces. The
// SDK surfaces it as a transport-level HTTP status rather than a modelled
// error shape for PutObject, so both forms are checked.
func isPreconditionFailed(err error) bool {
	var respErr interface{ HTTPStatusCode() int }
	if errors.As(err, &respErr) && respErr.HTTPStatusCode() == http.StatusPreconditionFailed {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "PreconditionFailed", "ConditionalRequestConflict":
			return true
		}
	}
	return false
}

// PriorMetaPrefix finds the metadata prefix this generation must restore FROM.
//
// The metadata root is partitioned per writer epoch — `agents-meta/<vid>/g<N>/`
// — precisely so a fenced-but-alive writer cannot collide with its successor's
// LTX history. The consequence, which only shows up end to end, is that a fresh
// epoch's own prefix is empty at startup: the previous generation replicated
// into `g<N-1>/`, and the MountSpec names only `g<N>/`. So the worker
// replicates forward into its own prefix and restores backward from the newest
// populated one at or below it.
//
// "At or below", not "below": a worker that crashes and is restarted by the
// kubelet comes back at the SAME epoch (the issuer replays the epoch for the
// same Pod — storagespec/issuer.go), and by then `g<N>/` holds that epoch's own
// LTX history. Restoring from `g<N-1>/` there silently drops everything epoch N
// wrote (PLO-323 F-6c). A prefix that holds nothing but its own fence marker is
// skipped by prefixHasReplica, so a fresh epoch — which has just PUT its marker
// and replicated nothing — still falls through to the generation before it.
//
// It is one LIST per mount start, and it is a read of the store rather than of
// authority: the control-plane still decides which epoch this writer holds; the
// listing only says where the bytes are.
func (f *S3Fencer) PriorMetaPrefix(ctx context.Context, root string, epoch int64) (string, error) {
	out, err := f.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:    aws.String(f.bucket),
		Prefix:    aws.String(root),
		Delimiter: aws.String("/"),
	})
	if err != nil {
		return "", fmt.Errorf("list metadata generations under %s: %w", root, err)
	}
	var found []string
	for _, p := range out.CommonPrefixes {
		if p.Prefix != nil {
			found = append(found, *p.Prefix)
		}
	}
	for _, candidate := range priorPrefixCandidates(found, root, epoch) {
		populated, err := f.prefixHasReplica(ctx, candidate)
		if err != nil {
			return "", err
		}
		if populated {
			return candidate, nil
		}
	}
	return "", nil
}

// prefixHasReplica reports whether a generation prefix holds anything beyond
// its own fence marker. A prefix containing only `fence` is a writer that
// claimed an epoch and died before replicating, which must not be mistaken for
// a restorable generation.
func (f *S3Fencer) prefixHasReplica(ctx context.Context, prefix string) (bool, error) {
	out, err := f.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(f.bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(10),
	})
	if err != nil {
		return false, fmt.Errorf("list %s: %w", prefix, err)
	}
	for _, obj := range out.Contents {
		if obj.Key != nil && *obj.Key != prefix+"fence" {
			return true, nil
		}
	}
	return false, nil
}

// priorPrefixCandidates orders the generation prefixes at or below `epoch`
// newest first. Split out from the S3 call so the ordering is testable.
func priorPrefixCandidates(prefixes []string, root string, epoch int64) []string {
	type candidate struct {
		epoch  int64
		prefix string
	}
	var candidates []candidate
	for _, p := range prefixes {
		seg := strings.TrimSuffix(strings.TrimPrefix(p, root), "/")
		if !strings.HasPrefix(seg, "g") {
			continue
		}
		n, err := strconv.ParseInt(seg[1:], 10, 64)
		if err != nil || n > epoch {
			continue
		}
		candidates = append(candidates, candidate{n, p})
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].epoch > candidates[j].epoch })
	out := make([]string, 0, len(candidates))
	for _, c := range candidates {
		out = append(out, c.prefix)
	}
	return out
}
