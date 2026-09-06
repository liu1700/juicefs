/*
 * JuiceFS, Copyright 2020 Juicedata, Inc.
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
	"sync"
)

type prefetcher struct {
	sync.Mutex
	pending chan string
	busy    map[string]bool
	op      func(key string)
	// closed is shut by stop. The worker goroutines select on it rather than
	// on the channel being closed, so a fetch racing the stop drops its key
	// instead of panicking on a send to a closed channel.
	closed    chan struct{}
	closeOnce sync.Once
}

func newPrefetcher(parallel int, fetch func(string)) *prefetcher {
	p := &prefetcher{
		pending: make(chan string, max(parallel*4, 10)),
		busy:    make(map[string]bool),
		op:      fetch,
		closed:  make(chan struct{}),
	}
	for range parallel {
		go p.do()
	}
	return p
}

func (p *prefetcher) do() {
	for {
		var key string
		select {
		case <-p.closed:
			return
		case key = <-p.pending:
		}
		p.op(key)

		p.Lock()
		delete(p.busy, key)
		p.Unlock()
	}
}

// stop releases the worker goroutines. It is idempotent, and a prefetch
// submitted after it is discarded.
func (p *prefetcher) stop() {
	p.closeOnce.Do(func() { close(p.closed) })
}

func (p *prefetcher) fetch(key string) {
	p.Lock()
	defer p.Unlock()
	if _, ok := p.busy[key]; ok {
		return
	}
	select {
	case p.pending <- key:
		p.busy[key] = true
	default:
	}
}
