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
	"net/http"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The renew answer carries the fencing echo (`storage_volume_id`,
// `fence_epoch`) and, in the same body, the two things the worker acts on: the
// renewed deadline and the grant. Until PLO-520 the echo was decoded and
// ignored, so a body this worker could not attribute — a request answered
// after the epoch moved, a proxy handing back another volume's response — was
// applied as if it were this holder's, and invariant 1 rests on the opposite.
//
// The refusal is the one the renew already has: a *CPError whose Fenced() is
// true takes the existing out-of-band branch. That is what these rows assert,
// one consequence at a time.
func TestARenewAnswerWhoseEchoIsNotOursIsRefusedAndFencesTheWorker(t *testing.T) {
	// A ceiling nothing in the spec carries, so "the grant was applied" is a
	// fact about THIS answer rather than about the one start() applies from
	// the MountSpec.
	foreignGrant := GrantSpec{Bytes: 777 << 20, Inodes: 7777, Epoch: 9}

	tests := map[string]struct {
		echo func(volumeID string, epoch int64) (string, int64)
	}{
		"another epoch": {
			echo: func(v string, e int64) (string, int64) { return v, e + 1 },
		},
		"an epoch this worker has already left behind": {
			echo: func(v string, e int64) (string, int64) { return v, e - 1 },
		},
		"another volume": {
			echo: func(_ string, e int64) (string, int64) {
				return "99999999-9999-9999-9999-999999999999", e
			},
		},
		"neither field is ours": {
			echo: func(string, int64) (string, int64) {
				return "99999999-9999-9999-9999-999999999999", 4242
			},
		},
		"an empty echo, which is what a body from a different route looks like": {
			echo: func(string, int64) (string, int64) { return "", 0 },
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			spec := testSpec()
			vol := healthyVolume()
			cp := &fakeCP{
				renewEcho: tc.echo,
				grant:     foreignGrant,
				// A deadline far past the spec's, so "the deadline was not
				// extended" is visible rather than inferred.
				expiry: func() time.Time { return time.Now().UTC().Add(90 * time.Minute) },
			}
			sup := newSup(t, spec, &fakeFS{vol: vol}, cp, &fakeReplicator{}, &fakeFencer{})

			// Bounded, and the bound is part of the assertion: a worker that
			// accepted the answer would take the 90-minute deadline above and
			// serve happily forever, so "it stopped at all" is the first thing
			// this proves. Without the bound that failure is a hung test.
			done := make(chan *Fatal, 1)
			go func() { done <- sup.Run(context.Background(), make(chan os.Signal)) }()
			var got *Fatal
			select {
			case got = <-done:
			case <-time.After(20 * time.Second):
				t.Fatal("the worker kept serving on an answer it could not attribute")
			}

			// Same terminal shape as stale_epoch, because it is the same
			// conclusion: this worker cannot prove it still holds the epoch.
			if got.Exit != CodeFenced || got.ErrCode != ErrCodeFencedOutOfBand {
				t.Fatalf("exit %d/%s, want %d/%s (%v)",
					got.Exit, got.ErrCode, CodeFenced, ErrCodeFencedOutOfBand, got.Err)
			}
			if !vol.Fenced() {
				t.Error("writes must be sealed before the process exits")
			}
			// Out of band means no barrier and no final sync: a writer that
			// may not own the epoch must not push its remaining history into
			// the prefix a successor restores from (F-1).
			if cp.released != ReasonFencedOutOfBand {
				t.Errorf("release reason = %q, want %q", cp.released, ReasonFencedOutOfBand)
			}

			// The deadline is the spec's, untouched. Update() is called only
			// after the echo is accepted, so an unattributable answer buys the
			// mount no extra lifetime.
			if wall := sup.deadline.WallExpiry(); !wall.Equal(spec.LeaseExpiresAt) {
				t.Errorf("deadline moved to %s; the spec's %s must stand", wall, spec.LeaseExpiresAt)
			}

			// The grant rides the same body as the echo. Refusing the body and
			// applying its ceiling would be the whole bug with one extra step.
			for _, g := range vol.appliedGrants() {
				if g[0] == foreignGrant.Bytes || g[1] == foreignGrant.Inodes {
					t.Errorf("the ceiling %v from an unattributable answer was applied", g)
				}
			}

			// Terminal, not retried: asking again is the fenced writer still
			// believing it holds the volume.
			renews := 0
			for _, c := range cp.order() {
				if c == "renew" {
					renews++
				}
			}
			if renews != 1 {
				t.Errorf("renew was attempted %d times; an unattributable answer must not be retried", renews)
			}

			// One typed line, and it carries both sides of the comparison: the
			// volume and epoch this worker presented (the closed fields the
			// plugin republishes) and the ones the answer named (the message).
			var buf bytes.Buffer
			WriteTerminal(&buf, spec.StorageVolumeID, spec.FenceEpoch, got)
			lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
			if len(lines) != 1 {
				t.Fatalf("terminal output has %d lines, want exactly 1:\n%s", len(lines), buf.String())
			}
			var line struct {
				Event   string `json:"event"`
				Volume  string `json:"volume"`
				Epoch   int64  `json:"epoch"`
				Error   string `json:"error"`
				Message string `json:"message"`
			}
			if err := json.Unmarshal([]byte(lines[0]), &line); err != nil {
				t.Fatalf("terminal line is not JSON: %v (%s)", err, lines[0])
			}
			if line.Error != ErrCodeFencedOutOfBand || line.Volume != spec.StorageVolumeID || line.Epoch != spec.FenceEpoch {
				t.Errorf("terminal line = %+v, want %s for volume %s epoch %d",
					line, ErrCodeFencedOutOfBand, spec.StorageVolumeID, spec.FenceEpoch)
			}
			answerVolume, answerEpoch := tc.echo(spec.StorageVolumeID, spec.FenceEpoch)
			if !strings.Contains(line.Message, answerVolume) {
				t.Errorf("terminal message %q does not name the volume the answer carried (%q)",
					line.Message, answerVolume)
			}
			if !strings.Contains(line.Message, strconv.FormatInt(answerEpoch, 10)) {
				t.Errorf("terminal message %q does not name the epoch the answer carried (%d)",
					line.Message, answerEpoch)
			}
		})
	}
}

// The other direction, and the reason the check is a comparison rather than a
// requirement that the fields be present: the honest answer — the one the
// control-plane actually sends, echoing what the request presented — is applied
// exactly as it was before. Without this row the check above could be satisfied
// by a worker that refuses every renew.
func TestTheHonestRenewAnswerIsStillApplied(t *testing.T) {
	spec := testSpec()
	vol := healthyVolume()
	grant := GrantSpec{Bytes: 8 << 30, Inodes: 250000, Epoch: 12}
	expiry := time.Now().UTC().Add(45 * time.Minute)
	cp := &fakeCP{grant: grant, expiry: func() time.Time { return expiry }}
	sup := newSup(t, spec, &fakeFS{vol: vol}, cp, &fakeReplicator{}, &fakeFencer{})

	// Stop once the ceiling from the RENEW has landed, so the assertion does
	// not race the loop and does not need a sleep. The epoch is what tells it
	// apart from the spec's own grant, which start() applies before the mount
	// serves anything.
	stop := make(chan os.Signal, 2)
	sup.Deps.Log = func(event string, kv ...any) {
		if event != "grant_applied" {
			return
		}
		for i := 0; i+1 < len(kv); i += 2 {
			if kv[i] == "epoch" && kv[i+1] == grant.Epoch {
				stop <- syscall.SIGTERM
			}
		}
	}

	if got := sup.Run(context.Background(), stop); got.Exit != CodeOK {
		t.Fatalf("exit = %d/%s (%v), want a clean stop", got.Exit, got.ErrCode, got.Err)
	}
	if wall := sup.deadline.WallExpiry(); !wall.Equal(expiry) {
		t.Errorf("deadline = %s, want the renewed %s", wall, expiry)
	}
	found := false
	for _, g := range vol.appliedGrants() {
		if g[0] == grant.Bytes && g[1] == grant.Inodes {
			found = true
		}
	}
	if !found {
		t.Errorf("the renewed ceiling %d/%d was never applied; applied %v",
			grant.Bytes, grant.Inodes, vol.appliedGrants())
	}
}

// The comparison itself, stated once. It is exact on both fields, it produces a
// refusal the existing fencing branch already routes (Fenced), and that refusal
// is not retryable — a 200 is the control-plane's considered answer, so asking
// again only gets it a second time.
func TestNotOursNamesBothSidesOfTheComparison(t *testing.T) {
	const vid = "550e8400-e29b-41d4-a716-446655440000"
	honest := LeaseResponse{StorageVolumeID: vid, FenceEpoch: 7}

	if err := honest.notOurs(vid, 7); err != nil {
		t.Fatalf("the honest answer was refused: %v", err)
	}

	tests := map[string]LeaseResponse{
		"epoch ahead":  {StorageVolumeID: vid, FenceEpoch: 8},
		"epoch behind": {StorageVolumeID: vid, FenceEpoch: 6},
		"another volume": {
			StorageVolumeID: "99999999-9999-9999-9999-999999999999", FenceEpoch: 7,
		},
		// The uppercase form of the same UUID is a DIFFERENT answer, not the
		// same one spelled differently: both sides render the id from one
		// uuid.UUID, so there is no honest sender of this body and no
		// normalisation for a real mismatch to hide behind.
		"the same uuid in another case": {
			StorageVolumeID: strings.ToUpper(vid), FenceEpoch: 7,
		},
		"nothing at all": {},
	}
	for name, resp := range tests {
		t.Run(name, func(t *testing.T) {
			err := resp.notOurs(vid, 7)
			if err == nil {
				t.Fatal("an answer that is not ours was accepted")
			}
			var cpErr *CPError
			if !errors.As(err, &cpErr) {
				t.Fatalf("refusal is %T, want *CPError so the renew's existing branch routes it", err)
			}
			if cpErr.Code != CPCodeAnswerNotOurs {
				t.Errorf("code = %q, want %q", cpErr.Code, CPCodeAnswerNotOurs)
			}
			if cpErr.Status != http.StatusOK {
				t.Errorf("status = %d, want %d: this is what the server really answered",
					cpErr.Status, http.StatusOK)
			}
			if !cpErr.Fenced() {
				t.Error("an unattributable answer must classify as a fence, or the renew loop keeps writing")
			}
			if cpErr.Retryable() {
				t.Error("a 200 is the server's considered answer; retrying it is not a different question")
			}
			// Both values, so an operator reading one line knows which two
			// addresses disagreed.
			for _, want := range []string{resp.StorageVolumeID, strconv.FormatInt(resp.FenceEpoch, 10), vid, "7"} {
				if want != "" && !strings.Contains(cpErr.Error(), want) {
					t.Errorf("refusal %q does not name %q", cpErr.Error(), want)
				}
			}
		})
	}
}
