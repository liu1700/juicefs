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
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The usage report is contract rev 3.5's only change: `trash_bytes`,
// `trash_inodes` and `trash_partial` join `used_bytes`/`used_inodes` on
// POST /v1/internal/storage/usage.
//
// The interesting half is what happens when the trash could NOT be measured. Zero is a
// real answer — an Agent that has deleted nothing — so sending zero for "we could not
// look" would make the dashboard tell a user that emptying the trash frees nothing,
// which is a claim nobody made. The fields are therefore ABSENT, and the control-plane
// stores nothing rather than a guess.

// usageCapture runs one ReportUsage against a fake control-plane and hands back the
// decoded body.
func usageCapture(t *testing.T, u Usage) map[string]any {
	t.Helper()
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/internal/storage/usage" {
			t.Errorf("posted to %s", r.URL.Path)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %s", err)
		}
		if err := json.Unmarshal(body, &got); err != nil {
			t.Errorf("decode body: %s", err)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("t"), 0o600); err != nil {
		t.Fatalf("write token: %s", err)
	}
	c := NewClient(srv.URL, tokenFile, 5*time.Second)
	if err := c.ReportUsage(context.Background(), "v-1", 7, u, time.Unix(1700000000, 0).UTC()); err != nil {
		t.Fatalf("report usage: %s", err)
	}
	return got
}

func TestTheUsageReportCarriesTheTrashBreakdown(t *testing.T) {
	got := usageCapture(t, Usage{
		Bytes: 100 << 20, Inodes: 900,
		TrashKnown: true, TrashBytes: 12 << 20, TrashInodes: 40,
	})
	for field, want := range map[string]float64{
		"used_bytes": 100 << 20, "used_inodes": 900,
		"trash_bytes": 12 << 20, "trash_inodes": 40,
	} {
		if got[field] != want {
			t.Errorf("%s = %v, want %v", field, got[field], want)
		}
	}
	if got["trash_partial"] != false {
		t.Errorf("trash_partial = %v, want false", got["trash_partial"])
	}
}

func TestAnUnmeasurableTrashIsAbsentFromTheReportNotZero(t *testing.T) {
	got := usageCapture(t, Usage{Bytes: 100 << 20, Inodes: 900})
	if got["used_bytes"] != float64(100<<20) {
		t.Errorf("used_bytes = %v: a failed trash walk must not cost the usage report", got["used_bytes"])
	}
	for _, field := range []string{"trash_bytes", "trash_inodes", "trash_partial"} {
		if _, present := got[field]; present {
			t.Errorf("%s was sent for a trash nobody could measure; absent is the only honest answer", field)
		}
	}
}

// A partial walk still reports its numbers, flagged. The floor is worth having — it
// drives metrics and an operator's judgement — and the flag is what stops the product
// from spending it on a sentence that promises an amount.
func TestAPartialWalkReportsItsFloorWithTheFlagSet(t *testing.T) {
	got := usageCapture(t, Usage{
		Bytes: 1 << 30, Inodes: 1_000_000,
		TrashKnown: true, TrashBytes: 1 << 20, TrashInodes: 200_000, TrashPartial: true,
	})
	if got["trash_bytes"] != float64(1<<20) {
		t.Errorf("trash_bytes = %v, want the floor", got["trash_bytes"])
	}
	if got["trash_partial"] != true {
		t.Error("a capped walk was reported as a complete one")
	}
}
