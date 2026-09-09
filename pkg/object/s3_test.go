//go:build !nos3
// +build !nos3

/*
 * JuiceFS, Copyright 2018 Juicedata, Inc.
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
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
)

func Test_s3client_full_string(t *testing.T) {
	tests := []struct {
		endpoint string
		want     string
	}{
		{endpoint: "s3.compatible.site/bucket", want: "s3://s3.compatible.site/bucket/"},
		{endpoint: "http://s3.compatible.site/bucket", want: "s3://s3.compatible.site/bucket/"},
		{endpoint: "s3://s3.compatible.site/bucket", want: "s3://s3.compatible.site/bucket/"},
		{endpoint: "https://mybucket.s3.us-east-2.amazonaws.com", want: "s3://mybucket/"},
	}
	for _, tt := range tests {
		t.Run(tt.endpoint, func(t *testing.T) {
			stor, err := newS3(tt.endpoint, "", "", "")
			if err != nil {
				t.Fatalf("newS3() error = %v", err)
			}
			assert.Equalf(t, tt.want, stor.String(), "Display full address of s3 compatible object storage")
		})
	}
}

func TestS3OCIRegion(t *testing.T) {
	tests := []struct {
		name          string
		endpoint      string
		wantBucket    string
		wantRegion    string
		wantPathStyle bool
	}{
		{
			name:          "legacy endpoint",
			endpoint:      "bucket.namespace.compat.objectstorage.ap-singapore-1.oraclecloud.com",
			wantBucket:    "bucket",
			wantRegion:    "ap-singapore-1",
			wantPathStyle: true,
		},
		{
			name:          "dedicated endpoint",
			endpoint:      "https://prod-sandbox-juicefs.axywvpcvts33.compat.objectstorage.us-sanjose-1.oci.customer-oci.com",
			wantBucket:    "prod-sandbox-juicefs",
			wantRegion:    "us-sanjose-1",
			wantPathStyle: true,
		},
		{
			name:          "path style endpoint",
			endpoint:      "https://axywvpcvts33.compat.objectstorage.us-sanjose-1.oci.customer-oci.com/prod-sandbox-juicefs",
			wantBucket:    "prod-sandbox-juicefs",
			wantRegion:    "us-sanjose-1",
			wantPathStyle: true,
		},
		{
			name:          "virtual hosted style endpoint",
			endpoint:      "https://prod-sandbox-juicefs.vhcompat.objectstorage.us-sanjose-1.oci.customer-oci.com",
			wantBucket:    "prod-sandbox-juicefs",
			wantRegion:    "us-sanjose-1",
			wantPathStyle: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("AWS_REGION", "eu-frankfurt-1")
			t.Setenv("AWS_DEFAULT_REGION", "eu-frankfurt-1")
			stor, err := newS3(tt.endpoint, "", "", "")
			if err != nil {
				t.Fatalf("newS3() error = %v", err)
			}
			client := stor.(*s3client)
			assert.Equal(t, tt.wantBucket, client.bucket)
			assert.Equal(t, tt.wantRegion, client.region)
			assert.Equal(t, tt.wantPathStyle, client.s3.Options().UsePathStyle)
		})
	}
}

// A refused PUT has to reach the writeback durability barrier with its class
// intact: the barrier tells a full bucket from a rotated-out credential with
// errors.Is, not by matching the SDK's message (PLO-458).
func TestS3PutClassifiesRefusals(t *testing.T) {
	const accessDenied = `<?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>`
	const quotaExceeded = `<?xml version="1.0" encoding="UTF-8"?><Error><Code>QuotaExceeded</Code><Message>User quota exceeded</Message></Error>`

	cases := []struct {
		name    string
		status  int
		body    string
		want    error
		notWant error
	}{
		{"507 without an error code", http.StatusInsufficientStorage, "", ErrInsufficientStorage, ErrAccessDenied},
		{"403 AccessDenied", http.StatusForbidden, accessDenied, ErrAccessDenied, ErrInsufficientStorage},
		// Ceph RGW reports a quota it cannot satisfy as 403, so the error code
		// has to win over the status.
		{"403 QuotaExceeded", http.StatusForbidden, quotaExceeded, ErrInsufficientStorage, ErrAccessDenied},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv("JFS_S3_VHOST_STYLE", "0")
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "application/xml")
				w.WriteHeader(c.status)
				_, _ = io.WriteString(w, c.body)
			}))
			defer srv.Close()

			s, err := newS3(srv.URL+"/bucket", "ak", "sk", "")
			if err != nil {
				t.Fatalf("newS3() error = %v", err)
			}
			err = s.Put(context.Background(), "key", bytes.NewReader([]byte("payload")))
			if err == nil {
				t.Fatalf("Put should have failed with %d", c.status)
			}
			assert.ErrorIs(t, err, c.want)
			assert.NotErrorIs(t, err, c.notWant)
			// The class is attached to the SDK's error, not substituted for it.
			var apiErr smithy.APIError
			assert.ErrorAs(t, err, &apiErr)
		})
	}
}

func TestS3PutLeavesOtherFailuresUnclassified(t *testing.T) {
	t.Setenv("JFS_S3_VHOST_STYLE", "0")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	s, err := newS3(srv.URL+"/bucket", "ak", "sk", "")
	if err != nil {
		t.Fatalf("newS3() error = %v", err)
	}
	err = s.Put(context.Background(), "key", bytes.NewReader([]byte("payload")))
	if err == nil {
		t.Fatal("Put should have failed with 500")
	}
	assert.NotErrorIs(t, err, ErrInsufficientStorage)
	assert.NotErrorIs(t, err, ErrAccessDenied)
}
