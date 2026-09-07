//go:build plori
// +build plori

/*
 * JuiceFS, Copyright 2026 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mount

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/plori/mountspec"
)

func TestReleaseCapabilitySurvivesRevokedPodTokenAndOtherCallsDoNotUseIt(t *testing.T) {
	var releaseCalls, podTokenCalls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case mountspec.RouteLeaseReleaseAfterStop:
			releaseCalls++
			if got := r.Header.Get("Authorization"); got != "Bearer one-generation-capability" {
				t.Errorf("release authorization = %q", got)
			}
		case mountspec.RouteLeaseRenew:
			fallthrough
		case mountspec.RouteUsage, mountspec.RouteDurablePoint, mountspec.RouteFormatAck:
			podTokenCalls++
			if got := r.Header.Get("Authorization"); got != "Bearer revoked-pod-token" {
				t.Errorf("%s authorization = %q; capability must not authorize this route", r.URL.Path, got)
			}
			w.WriteHeader(http.StatusUnauthorized)
			return
		default:
			t.Errorf("unexpected route %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "token")
	capabilityFile := filepath.Join(dir, "release-capability")
	if err := os.WriteFile(tokenFile, []byte("revoked-pod-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(capabilityFile, []byte("one-generation-capability\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := NewClient(srv.URL, tokenFile, time.Second)
	c.ReleaseCapabilityFile = capabilityFile

	if err := c.ReleaseLease(context.Background(), "vol-1", 7, ReasonShutdown); err != nil {
		t.Fatalf("release with capability: %v", err)
	}
	if _, err := c.RenewLease(context.Background(), "vol-1", 7, RenewRequest{}); err == nil {
		t.Fatal("renew unexpectedly succeeded with a revoked Pod token")
	}
	if err := c.ReportUsage(context.Background(), "vol-1", 7, Usage{}, time.Now()); err == nil {
		t.Fatal("usage unexpectedly succeeded with a revoked Pod token")
	}
	if err := c.ReportDurablePoint(context.Background(), "vol-1", 7, BarrierResult{}, ""); err == nil {
		t.Fatal("durable point unexpectedly succeeded with a revoked Pod token")
	}
	if _, err := c.AckFormat(context.Background(), "vol-1", 7, "format-uuid"); err == nil {
		t.Fatal("format acknowledgement unexpectedly succeeded with a revoked Pod token")
	}
	if releaseCalls != 1 || podTokenCalls != 4 {
		t.Fatalf("release calls = %d, Pod-token calls = %d; want 1 and 4", releaseCalls, podTokenCalls)
	}
}

func TestReleaseCapabilityFileErrorsDoNotFallBackToPodToken(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls++ }))
	defer srv.Close()

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("projected-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, capability := range map[string]string{
		"missing": filepath.Join(t.TempDir(), "missing"),
		"empty":   filepath.Join(t.TempDir(), "empty"),
	} {
		t.Run(name, func(t *testing.T) {
			if name == "empty" {
				if err := os.WriteFile(capability, nil, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			c := NewClient(srv.URL, tokenFile, time.Second)
			c.ReleaseCapabilityFile = capability
			if err := c.ReleaseLease(context.Background(), "vol-1", 7, ReasonShutdown); err == nil {
				t.Fatal("release unexpectedly fell back to the Pod token")
			}
		})
	}
	if calls != 0 {
		t.Fatalf("server calls = %d, want no fallback request", calls)
	}
}

func TestReleaseLeaseWithoutCapabilityUsesProjectedPodToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != mountspec.RouteLeaseRelease {
			t.Errorf("route = %s, want %s", r.URL.Path, mountspec.RouteLeaseRelease)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer projected-token" {
			t.Errorf("authorization = %q", got)
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("projected-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := NewClient(srv.URL, tokenFile, time.Second).ReleaseLease(context.Background(), "vol-1", 7, ReasonShutdown); err != nil {
		t.Fatalf("legacy release: %v", err)
	}
}
