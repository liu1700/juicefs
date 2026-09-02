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
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// ErrFenceMarkerHeld means the store refused the conditional PUT: another
// writer already claimed this epoch's metadata prefix.
var ErrFenceMarkerHeld = errors.New("fence marker already claimed")

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
// and the credential the worker holds in its environment. The credential is
// read from the process environment and never logged, never written to the
// spec, and never persisted (mountspec.md §5).
func NewS3Fencer(ctx context.Context, store ObjectStore, accessKey, secretKey string) (*S3Fencer, error) {
	if accessKey == "" || secretKey == "" {
		return nil, fmt.Errorf("%w: object credential is not present in the worker environment", ErrSpec)
	}
	region := store.Region
	if region == "" {
		region = "us-east-1"
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
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
// LTX history. The consequence, which only shows up end to end, is that a new
// epoch's own prefix is always empty at startup: the previous generation
// replicated into `g<N-1>/`, and the MountSpec names only `g<N>/`. So the
// worker replicates forward into its own prefix and restores backward from the
// newest populated one below it.
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

// priorPrefixCandidates orders the generation prefixes below `epoch` newest
// first. Split out from the S3 call so the ordering is testable.
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
		if err != nil || n >= epoch {
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
