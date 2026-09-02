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
	"testing"
	"time"
)

func TestDeadlineStopsWritesOneMarginBeforeExpiry(t *testing.T) {
	now := time.Now()
	d := NewDeadline(now.UTC().Add(2*time.Minute), 45*time.Second, now)

	if !d.WriteAllowed(now) {
		t.Fatal("writes must be allowed at the start of a fresh lease")
	}
	if !d.WriteAllowed(now.Add(74 * time.Second)) {
		t.Error("writes must still be allowed one second before the margin")
	}
	if d.WriteAllowed(now.Add(76 * time.Second)) {
		t.Error("writes must stop once the margin is reached")
	}
	if d.Expired(now.Add(119 * time.Second)) {
		t.Error("the lease itself is not gone until expiry")
	}
	if !d.Expired(now.Add(121 * time.Second)) {
		t.Error("the lease must be expired after its TTL")
	}
}

// The window is `server expiry - margin` measured on our own clock, whichever
// instant the conversion happened at. mountspec.md §6 is explicit that the
// deadline is "as the DATABASE measured it": the worker does not add its own
// safety term on top, it converts once and then never re-reads the wall clock.
func TestWriteWindowIsTheServerExpiryMinusTheMargin(t *testing.T) {
	sent := time.Now()
	serverExpiry := sent.UTC().Add(2 * time.Minute)
	received := sent.Add(5 * time.Second)

	atSend := NewDeadline(serverExpiry, 45*time.Second, sent)
	atReceipt := NewDeadline(serverExpiry, 45*time.Second, received)

	for name, d := range map[string]*Deadline{"converted at send": atSend, "converted at receipt": atReceipt} {
		got := d.Remaining(received)
		want := serverExpiry.Sub(received.UTC()) - 45*time.Second
		if got < want-time.Millisecond || got > want+time.Millisecond {
			t.Errorf("%s: remaining window = %s, want %s", name, got, want)
		}
	}
}

// threat-model §7.2: a paused writer that re-reads the wall clock resumes
// believing it still holds the lease. clockJump is the detector.
func TestClockJumpSeesAWallClockStep(t *testing.T) {
	if jump := clockJump(30*time.Second, 30*time.Second+2*time.Millisecond); jump > MaxClockJump {
		t.Errorf("ordinary NTP slew reported a fence trip: %s", jump)
	}
	if jump := clockJump(30*time.Second, 10*time.Minute); jump <= MaxClockJump {
		t.Errorf("a wall clock 10 minutes ahead of the monotonic clock reported %s", jump)
	}
	// A process frozen for ten minutes sees the wall clock advance while its
	// own monotonic clock barely moves. Backwards steps count the same.
	if jump := clockJump(10*time.Minute, 30*time.Second); jump <= MaxClockJump {
		t.Errorf("a backwards wall-clock step reported %s", jump)
	}
	// ClockJump on two real readings taken back to back must be quiet.
	a := time.Now()
	if jump := ClockJump(a, time.Now()); jump > MaxClockJump {
		t.Errorf("two consecutive time.Now() readings reported a jump of %s", jump)
	}
}

func TestDeadlineUpdateMovesTheWindowForward(t *testing.T) {
	now := time.Now()
	d := NewDeadline(now.UTC().Add(2*time.Minute), 45*time.Second, now)
	later := now.Add(20 * time.Second)
	d.Update(later.UTC().Add(2*time.Minute), 45*time.Second, later)

	if !d.WriteAllowed(now.Add(90 * time.Second)) {
		t.Error("a successful renewal must extend the write window")
	}
	if got, want := d.WallExpiry().Sub(later.UTC()), 2*time.Minute; got < want-time.Second || got > want+time.Second {
		t.Errorf("wall expiry moved to %s after the renewal, want ~%s", got, want)
	}
}
