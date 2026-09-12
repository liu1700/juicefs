/*
 * JuiceFS, Copyright 2024 Juicedata, Inc.
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

package chunk

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setState(s *diskCache, state int) {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()
	s.state.stop()
	s.state = newDCState(state, s)
}

func testDiskCacheState(t *testing.T, cacheNum int) {
	// Registered first, so it runs last: every store this test creates is
	// stopped before the tunables go back to their package defaults, and
	// nothing this invocation started is still running when the next one
	// rewrites them.
	oriTickDurForUnstable, oriMinIOSuccToNormal, oriMaxDurToDown := tickDurForUnstable, minIOSuccToNormal, maxDurToDown
	defer func() {
		tickDurForUnstable, minIOSuccToNormal, maxDurToDown = oriTickDurForUnstable, oriMinIOSuccToNormal, oriMaxDurToDown
	}()

	genDirs := func(num int) []string {
		dirs := make([]string, 0, num)
		for i := 0; i < num; i++ {
			dirs = append(dirs, t.TempDir())
		}
		return dirs
	}

	conf := defaultConf
	dirs := genDirs(cacheNum)
	conf.CacheDir = strings.Join(dirs, ":")
	conf.AutoCreate = true

	manager := newCacheManager(&conf, nil, nil)
	defer manager.stop()
	require.False(t, manager.isEmpty())

	m, ok := manager.(*cacheManager)
	require.True(t, ok)
	require.Equal(t, cacheNum, m.length())

	// case: cache
	data := []byte{1, 2, 3}
	page := NewPage(data)
	defer page.Release()
	k1 := probeCacheKey(0, len(data))
	m.cache(k1, page, true, false)
	time.Sleep(time.Second)

	// case: normal -> unstable
	// s1.state is written by event under s1.stateLock from the state tickers,
	// so every read of it here goes through curState too.
	s1 := m.getStore(k1)
	for i := 0; i <= int(numIOErrToUnstable); i++ {
		s1.curState().onIOErr()
	}
	require.Equal(t, dcUnstable, s1.curState().state())

	// case: probe in unstable
	time.Sleep(time.Second)
	require.GreaterOrEqual(t, atomic.LoadUint32(&s1.curState().(*unstableDC).ioCnt), uint32(1))

	// case: unstable concurrency limit
	unstable := s1.curState()
	for i := 0; i < int(maxConcurrencyForUnstable); i++ {
		unstable.beforeCacheOp()
	}
	_, err := m.load(k1)
	assert.Equal(t, errUnstableCoLimit, err)
	for i := 0; i < int(maxConcurrencyForUnstable); i++ {
		unstable.afterCacheOp()
	}

	// case: unstable -> normal
	tickDurForUnstable = time.Second
	minIOSuccToNormal = 1
	setState(s1, dcUnstable)
	s1.curState().(*unstableDC).doProbe(k1, page)
	time.Sleep(2 * time.Second)
	require.Equal(t, dcNormal, s1.curState().state())

	// case: unstable -> down
	tickDurForUnstable = time.Second
	maxDurToDown = 1
	minIOSuccToNormal = 5 * 60
	setState(s1, dcUnstable)
	time.Sleep(2 * time.Second)
	require.Equal(t, dcDown, s1.curState().state())
}

func TestDiskCacheState(t *testing.T) {
	testDiskCacheState(t, 1)
	testDiskCacheState(t, 10)
}
