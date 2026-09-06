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
	"os"
	"testing"
)

// The slice-ID floor is raised once, and before anything in this process can
// allocate a slice: before the session purge, before the post-crash repair and
// before the mount is served (PLO-569).
func TestTheSliceIDFloorIsRaisedBeforeTheFirstWrite(t *testing.T) {
	vol := healthyVolume()
	vol.floorFrom, vol.floorTo = 42, 21_000_000_000_000
	sup := newSup(t, testSpec(), &fakeFS{vol: vol}, &fakeCP{}, &fakeReplicator{}, &fakeFencer{})

	var floorLogged [2]int64
	prev := sup.Deps.Log
	sup.Deps.Log = func(event string, kv ...any) {
		if prev != nil {
			prev(event, kv...)
		}
		if event == "slice_id_floor" {
			for i := 0; i+1 < len(kv); i += 2 {
				switch kv[i] {
				case "from":
					floorLogged[0], _ = kv[i+1].(int64)
				case "to":
					floorLogged[1], _ = kv[i+1].(int64)
				}
			}
		}
	}

	got := sup.Run(context.Background(), stopOnReady(sup))
	if got.Exit != CodeOK {
		t.Fatalf("exit = %d, want 0 (%v)", got.Exit, got.Err)
	}
	if vol.floorRaises != 1 {
		t.Errorf("the floor was raised %d times, want exactly 1", vol.floorRaises)
	}
	if floorLogged != [2]int64{42, 21_000_000_000_000} {
		t.Errorf("slice_id_floor logged %v, want the counter before and after", floorLogged)
	}

	order := vol.order()
	floor := indexOf(order, "slice_id_floor")
	purge := indexOf(order, "purge_sessions")
	if floor < 0 {
		t.Fatalf("the floor was never raised: %v", order)
	}
	if purge >= 0 && floor > purge {
		t.Errorf("the floor must be raised before the session purge: %v", order)
	}
}

// An unclean generation runs the restore-time repair, which writes to the
// metadata. The floor has to be up before that, not after.
func TestTheSliceIDFloorPrecedesTheRestoreRepair(t *testing.T) {
	vol := healthyVolume()
	sup := newSup(t, testSpec(), &fakeFS{vol: vol}, &fakeCP{}, &fakeReplicator{}, &fakeFencer{})
	// No clean marker in the state dir means the previous generation died
	// mid-flight, which is what makes the repair run.
	if err := os.MkdirAll(sup.Paths.StateDir, 0o700); err != nil {
		t.Fatalf("create state dir: %s", err)
	}

	if got := sup.Run(context.Background(), stopOnReady(sup)); got.Exit != CodeOK {
		t.Fatalf("exit = %d, want 0 (%v)", got.Exit, got.Err)
	}
	order := vol.order()
	floor, repair := indexOf(order, "slice_id_floor"), indexOf(order, "repair")
	if floor < 0 || repair < 0 {
		t.Fatalf("expected both a floor raise and a repair: %v", order)
	}
	if floor > repair {
		t.Errorf("the floor must be raised before the repair writes: %v", order)
	}
}

// A floor that cannot be written refuses the mount. Serving a volume whose
// writes may land on another generation's object keys is worse than not
// serving it, and a retry in place would produce the same answer.
func TestAFailedSliceIDFloorRefusesTheMount(t *testing.T) {
	vol := healthyVolume()
	vol.floorErr = errors.New("database is locked")
	cp := &fakeCP{}
	sup := newSup(t, testSpec(), &fakeFS{vol: vol}, cp, &fakeReplicator{}, &fakeFencer{})

	got := sup.Run(context.Background(), make(chan os.Signal))
	if got.Exit != CodeRestoreFailed || got.ErrCode != ErrCodeSliceFloorFailed {
		t.Fatalf("got exit %d / %s, want %d / %s", got.Exit, got.ErrCode, CodeRestoreFailed, ErrCodeSliceFloorFailed)
	}
	if got.Retryable {
		t.Error("a failed floor is not retryable in place")
	}
	if contains(vol.order(), "purge_sessions") {
		t.Errorf("nothing may run after the refusal: %v", vol.order())
	}
	if _, err := os.Stat(sup.Paths.ReadyPath()); err == nil {
		t.Error("a refused mount must not publish readiness")
	}
}
