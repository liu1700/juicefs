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
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// conditionalPutShim is the smallest S3 surface the fencer needs: a PUT that
// honours `If-None-Match: *` by answering 412 when the key exists. PLO-351
// verified the production store behaves exactly this way, so the shim encodes
// the probe's finding rather than a guess.
type conditionalPutShim struct {
	mu   sync.Mutex
	keys map[string]bool
}

func newShim() *httptest.Server {
	s := &conditionalPutShim{keys: map[string]bool{}}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		key := strings.TrimPrefix(r.URL.Path, "/")
		if r.Header.Get("If-None-Match") == "*" && s.keys[key] {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusPreconditionFailed)
			_, _ = w.Write([]byte(`<Error><Code>PreconditionFailed</Code><Message>At least one of the pre-conditions you specified did not hold</Message></Error>`))
			return
		}
		s.keys[key] = true
		w.Header().Set("ETag", `"d41d8cd98f00b204e9800998ecf8427e"`)
		w.WriteHeader(http.StatusOK)
	}))
}

func TestFenceMarkerIsClaimedOnceAndRefusedTwice(t *testing.T) {
	srv := newShim()
	defer srv.Close()

	ctx := context.Background()
	store := ObjectStore{Endpoint: srv.URL, Bucket: "plorifs", Region: "us-east-1", CredentialSource: CredentialSourceNodeSecret}
	fencer, err := NewS3Fencer(ctx, store, "key", "secret")
	if err != nil {
		t.Fatalf("NewS3Fencer: %v", err)
	}
	key := "agents-meta/v1/g3/fence"
	if err := fencer.Claim(ctx, key, []byte(`{"epoch":3}`)); err != nil {
		t.Fatalf("first claim must succeed: %v", err)
	}
	err = fencer.Claim(ctx, key, []byte(`{"epoch":3}`))
	if !errors.Is(err, ErrFenceMarkerHeld) {
		t.Fatalf("second claim must report ErrFenceMarkerHeld, got %v", err)
	}
}

func TestFencerRefusesWithoutACredential(t *testing.T) {
	_, err := NewS3Fencer(context.Background(), ObjectStore{Endpoint: "https://example.invalid", Bucket: "b"}, "", "")
	if !errors.Is(err, ErrSpec) {
		t.Fatalf("a missing credential must be a spec-class refusal, got %v", err)
	}
}
