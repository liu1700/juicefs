//go:build windows
// +build windows

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

package winfsp

import (
	"fmt"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/fs"
	"github.com/juicedata/juicefs/pkg/utils"
)

const RotateAccessLog = 300 << 20 // 300 MiB

func (j *juice) log(ctx fs.LogContext, format string, args ...interface{}) {
	var failed bool
	for _, a := range args {
		if eno, ok := a.(syscall.Errno); ok && eno == syscall.EIO {
			failed = true
		}
	}
	j.logM.Lock()
	buffer := j.logBuffer
	j.logM.Unlock()
	if buffer == nil && !failed {
		return
	}
	now := utils.Now()
	cmd := fmt.Sprintf(format, args...)
	ts := now.Format("2006.01.02 15:04:05.000000")
	used := ctx.Duration()
	cmd += fmt.Sprintf(" <%.6f>", used.Seconds())
	line := fmt.Sprintf("%s [uid:%d,gid:%d,pid:%d] %s\n", ts, ctx.Uid(), ctx.Gid(), ctx.Pid(), cmd)
	if failed {
		logger.Errorf("failed operation: %s", line)
	}
	if buffer == nil {
		return
	}
	select {
	case buffer <- line:
	default:
		logger.Debugf("log dropped: %s", line[:len(line)-1])
	}
}

func (j *juice) flushLog(f *fs.AccessLog, path string, rotateCount int) {
	buf := make([]byte, 0, 128<<10)
	var lastcheck = time.Now()
	numFiles := rotateCount
	defer func() { _ = f.Close() }()

	for {
		line, ok := <-j.logBuffer
		if !ok {
			return
		}
		buf = append(buf[:0], []byte(line)...)
	LOOP:
		for len(buf) < (128 << 10) {
			select {
			case line, ok = <-j.logBuffer:
				if !ok {
					break LOOP
				}
				buf = append(buf, []byte(line)...)
			default:
				break LOOP
			}
		}
		_, err := f.Write(buf)
		if err != nil {
			logger.Errorf("write access log: %s", err)
			break
		}
		if lastcheck.Add(time.Minute).After(time.Now()) {
			continue
		}
		lastcheck = time.Now()
		if _, err = f.Rotate(RotateAccessLog, numFiles); err != nil {
			logger.Errorf("rotate access log %s: %s; continuing with the open log file", path, err)
		}
	}
}
