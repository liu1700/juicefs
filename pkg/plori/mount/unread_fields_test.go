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
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// PLO-521: the four mount-wire fields this worker declared and never read.
//
// A decoded-and-ignored field decodes perfectly, logs nothing and fails no test,
// which is why the control-plane's own comments could describe /format-ack as
// the channel that delivers a freshly formatted volume its ceiling while nothing
// on this side ever looked at it — the same shape PLO-478 found on /usage and
// /durable-point. Three of the four are deleted here and one is read; these
// tests pin the consequence of each, because a struct field removed with no test
// behind it comes back the next time somebody wants "just one more thing on the
// answer".
//
// The plori-runtime guard (services/storage-worker/internal/mountwire) proves the
// two ends AGREE on which keys exist. It cannot prove what this worker does with
// one, and that is what is asserted here.

// answerFrom stands a control-plane up that answers `body` verbatim to whatever
// this client posts, and returns a client pointed at it.
func answerFrom(t *testing.T, body string) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("projected-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	return NewClient(srv.URL, tokenFile, 5*time.Second)
}

// reEncode is the whole assertion idiom of the two tests below: decode the
// control-plane's answer into the struct this worker declares, then encode that
// struct again. What comes back is exactly the surface a future reader of this
// type can reach — a field added to the struct shows up here whether or not any
// code reads it, and a field deleted from the struct cannot.
func reEncode(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// The /format-ack answer is read for `state` and nothing else, and after PLO-521
// there is nothing else to read: the ceiling and the counters are off the wire on
// both sides (contract rev 3.12).
//
// The answer here is the PRE-3.12 body, deliberately. A control-plane that has
// not yet dropped the keys is exactly what this worker meets between the fork tag
// and the pin bump, and the property that makes that safe — unknown keys are
// ignored, and a ceiling in one of them cannot reach the volume because no field
// receives it — is the property worth a test.
func TestTheFormatAckAnswerYieldsNothingButItsState(t *testing.T) {
	c := answerFrom(t, `{
		"storage_volume_id": "vol-1",
		"state": "active",
		"grant": {"bytes": 8796093022208, "inodes": 99999999, "epoch": 42},
		"used_bytes": 123456,
		"used_inodes": 789,
		"trash_bytes": 1,
		"trash_inodes": 2
	}`)
	res, err := c.AckFormat(context.Background(), "vol-1", 7, "6c1e5f2c-0f0a-4a1c-9f2d-2b4e6a8c0d1e")
	if err != nil {
		t.Fatal(err)
	}
	if res.State != VolumeStateActive {
		t.Errorf("state = %q, want the control-plane's answer decoded", res.State)
	}
	// A ceiling that reached a field here would be a ceiling a future reader
	// applies — from a body carrying no fence_epoch, which is the unattributable
	// answer PLO-520 refused on the renew. It must not be reachable at all.
	if got, want := reEncode(t, res), `{"state":"active"}`; got != want {
		t.Errorf("VolumeStateResponse = %s, want %s\n"+
			"Every key beyond `state` on this answer was deleted by PLO-521: the ceiling because the "+
			"MountSpec already delivered it and Supervisor.start applies it before the mount serves "+
			"anything, the counters because they describe a filesystem this call just created. Adding "+
			"one back means naming who reads it AND giving this answer a fence epoch to be attributed by.",
			got, want)
	}
}

// `released` was only ever true on /lease/release, and ReleaseLease posts that
// route with a nil decode target — so the one call that could have read it threw
// the body away before it existed. On a renew the control-plane answers false by
// construction. PLO-521 (2) deletes it rather than giving the release a body,
// because the release is the last call a stopping worker makes: there is no step
// left that could act on `released: false`.
func TestTheRenewAnswerCarriesNoReleaseFlagForAnyoneToTrust(t *testing.T) {
	c := answerFrom(t, `{
		"storage_volume_id": "vol-1",
		"fence_epoch": 7,
		"lease_expires_at": "2026-09-03T12:00:00Z",
		"grant": {"bytes": 1024, "inodes": 16, "epoch": 3},
		"released": true,
		"over_budget": false
	}`)
	res, err := c.RenewLease(context.Background(), "vol-1", 7, RenewRequest{})
	if err != nil {
		t.Fatal(err)
	}
	// The fields the worker DOES act on still arrive: this test must fail on a
	// deletion that went too far, not only on one that did not happen.
	if res.LeaseExpiresAt.IsZero() || res.Grant.Epoch != 3 {
		t.Fatalf("the renew answer lost a field the worker acts on: %+v", res)
	}
	want := `{"storage_volume_id":"vol-1","fence_epoch":7,"lease_expires_at":"2026-09-03T12:00:00Z",` +
		`"grant":{"bytes":1024,"inodes":16,"epoch":3,"acked_epoch":0},"over_budget":false}`
	if got := reEncode(t, res); got != want {
		t.Errorf("LeaseResponse = %s\nwant %s\n"+
			"`released` must not be reachable from this struct: a stopping worker has already "+
			"unmounted and decided its exit code by the time the control-plane answers the release, "+
			"so a flag saying the release did not land could only be logged twice.", got, want)
	}
}

// PLO-521 (4): MountSpec.IssuedAt exists "for the worker's log" and no line
// logged it, which made the field a claim rather than a fact. The `ready` line
// logs it now, with the age spelled out, because the gap between issuance and
// serving is the one number neither side can compute alone — the control-plane
// sees only when it built the spec, and the worker only when it finished using
// it.
func TestTheReadyLineCarriesTheSpecsIssueTimeAndAge(t *testing.T) {
	spec := testSpec()
	// A spec issued well before this mount: the shape of a volume that was
	// re-published, or a node that stalled between NodeStage and NodePublish.
	spec.IssuedAt = time.Now().UTC().Add(-90 * time.Second).Truncate(time.Millisecond)

	sup := newSup(t, spec, &fakeFS{vol: healthyVolume()}, &fakeCP{}, &fakeReplicator{}, &fakeFencer{})
	var ready []any
	sup.Deps.Log = func(event string, kv ...any) {
		if event == "ready" {
			ready = append([]any(nil), kv...)
		}
	}
	if f := sup.Run(context.Background(), stopOnReady(sup)); f.Exit != CodeOK {
		t.Fatalf("exit = %d, want 0 (%v)", f.Exit, f.Err)
	}

	fields := map[string]any{}
	for i := 0; i+1 < len(ready); i += 2 {
		key, ok := ready[i].(string)
		if !ok {
			t.Fatalf("ready line is not key/value pairs: %v", ready)
		}
		fields[key] = ready[i+1]
	}
	if got, want := fields["spec_issued_at"], spec.IssuedAt.Format(time.RFC3339Nano); got != want {
		t.Errorf("spec_issued_at = %v, want %v; the line has to join to the control-plane's own "+
			"issuance record, so it carries the timestamp and not only the age", got, want)
	}
	age, err := time.ParseDuration(fields["spec_age"].(string))
	if err != nil {
		t.Fatalf("spec_age = %v, which does not parse as a duration: %v", fields["spec_age"], err)
	}
	if age < 89*time.Second || age > 2*time.Minute {
		t.Errorf("spec_age = %s, want ~90s: the age is the whole point of logging IssuedAt, and an "+
			"age computed from the wrong end is worse than none", age)
	}
}
