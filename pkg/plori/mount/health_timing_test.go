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
	"os"
	"sync"
	"testing"
	"time"
)

func TestReadyAndHealthPublishTheSamePhaseTimings(t *testing.T) {
	spec := testSpec()
	spec.LeaseExpiresAt = time.Now().Add(time.Minute)
	sup := newSup(t, spec, &fakeFS{vol: healthyVolume()}, &fakeCP{}, &fakeReplicator{}, &fakeFencer{})
	var mu sync.Mutex
	now := time.Now()
	sup.Deps.Now = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		now = now.Add(time.Millisecond)
		return now
	}
	stop := stopOnReady(sup)
	if got := sup.Run(context.Background(), stop); got == nil || got.Exit != CodeOK {
		t.Fatalf("Run() = %v", got)
	}
	read := func(path string, out any) {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		if err := json.Unmarshal(raw, out); err != nil {
			t.Fatalf("decode %s: %v", path, err)
		}
	}
	var ready Ready
	read(sup.Paths.ReadyPath(), &ready)
	var health Health
	read(sup.Paths.HealthPath(), &health)
	if ready.ReadyMS < ready.RestoreMS || ready.ReadyMS < ready.MountMS {
		t.Fatalf("ready timings restore=%d mount=%d ready=%d are not cumulative", ready.RestoreMS, ready.MountMS, ready.ReadyMS)
	}
	if health.RestoreMS != ready.RestoreMS || health.MountMS != ready.MountMS || health.ReadyMS != ready.ReadyMS {
		t.Fatalf("health timings = (%d,%d,%d), ready timings = (%d,%d,%d)", health.RestoreMS, health.MountMS, health.ReadyMS, ready.RestoreMS, ready.MountMS, ready.ReadyMS)
	}
}
