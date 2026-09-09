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

package object

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// refusingServer answers every request with one status and body.
func refusingServer(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newTestRestful(endpoint string) *RestfulStorage {
	return &RestfulStorage{
		endpoint: endpoint,
		signer:   func(*http.Request, string, string, string) {},
	}
}

// The restful backend has only the HTTP status to classify a refusal with, so
// that is what parseError maps (PLO-458).
func TestRestfulPutClassifiesRefusals(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		body    string
		want    error
		notWant error
	}{
		{"507 is a full store", http.StatusInsufficientStorage, "no space left", ErrInsufficientStorage, ErrAccessDenied},
		{"403 is a refused credential", http.StatusForbidden, "signature mismatch", ErrAccessDenied, ErrInsufficientStorage},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv := refusingServer(t, c.status, c.body)
			err := newTestRestful(srv.URL).Put(context.Background(), "key", bytes.NewReader([]byte("payload")))
			if err == nil {
				t.Fatalf("Put should have failed with %d", c.status)
			}
			if !errors.Is(err, c.want) {
				t.Fatalf("error %v is not %v", err, c.want)
			}
			if errors.Is(err, c.notWant) {
				t.Fatalf("error %v should not be %v", err, c.notWant)
			}
			// The class is added to the error, not to its message.
			if !strings.Contains(err.Error(), c.body) {
				t.Fatalf("error %q lost the store's message %q", err, c.body)
			}
			if strings.Contains(err.Error(), c.want.Error()) {
				t.Fatalf("error %q restates the class", err)
			}
		})
	}
}

func TestRestfulPutLeavesOtherFailuresUnclassified(t *testing.T) {
	srv := refusingServer(t, http.StatusInternalServerError, "boom")
	err := newTestRestful(srv.URL).Put(context.Background(), "key", bytes.NewReader([]byte("payload")))
	if err == nil {
		t.Fatal("Put should have failed with 500")
	}
	if errors.Is(err, ErrInsufficientStorage) || errors.Is(err, ErrAccessDenied) {
		t.Fatalf("a 500 should carry no failure class, got %v", err)
	}
}

func TestRestfulHeadClassifiesRefusals(t *testing.T) {
	srv := refusingServer(t, http.StatusForbidden, "")
	_, err := newTestRestful(srv.URL).Head(context.Background(), "key")
	if !errors.Is(err, ErrAccessDenied) {
		t.Fatalf("error %v is not %v", err, ErrAccessDenied)
	}
}

// classify keeps the wrapped error reachable, so a backend can attach a class
// without hiding the error type its own callers already match on.
func TestClassifyKeepsTheWrappedError(t *testing.T) {
	inner := errors.New("status: 507, message: over quota")
	err := classify(ErrInsufficientStorage, inner)
	if !errors.Is(err, ErrInsufficientStorage) || !errors.Is(err, inner) {
		t.Fatalf("classified error %v lost a link", err)
	}
	if err.Error() != inner.Error() {
		t.Fatalf("message changed: %q != %q", err.Error(), inner.Error())
	}
	if classify(nil, inner) != inner {
		t.Fatal("classify with no class should return the error unchanged")
	}
	if classify(ErrInsufficientStorage, nil) != nil {
		t.Fatal("classify of a nil error should stay nil")
	}
}
