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

package vfs

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/stretchr/testify/require"
)

type durabilityControlStore struct {
	*blockingChunkStore
	status chunk.DurabilityStatus
	wait   bool
}

func (s *durabilityControlStore) RemoteDurability(ctx context.Context) (chunk.DurabilityStatus, error) {
	if s.wait {
		<-ctx.Done()
		return s.status, ctx.Err()
	}
	return s.status, nil
}

func (s *durabilityControlStore) RemoteDurabilityStatus() chunk.DurabilityStatus {
	return s.status
}

type durabilityControlWriter struct{}

func (durabilityControlWriter) Open(Ino, uint64, uint8) FileWriter    { return nil }
func (durabilityControlWriter) Flush(meta.Context, Ino) syscall.Errno { return 0 }
func (durabilityControlWriter) GetLength(Ino) uint64                  { return 0 }
func (durabilityControlWriter) Truncate(Ino, uint64)                  {}
func (durabilityControlWriter) UpdateMtime(Ino, time.Time)            {}
func (durabilityControlWriter) FlushAll() error                       { return nil }

func runDurabilityControl(v *VFS, uid uint32, payload []byte) ([]byte, syscall.Errno) {
	out := &bytes.Buffer{}
	v.handleInternalMsg(meta.NewContext(1, uid, []uint32{uid}), meta.RemoteDurability, utils.FromBuffer(payload), out)
	return decodeControlOutput(out.Bytes())
}

func TestDurabilityControlAuthorizationAndStatus(t *testing.T) {
	store := &durabilityControlStore{
		blockingChunkStore: &blockingChunkStore{},
		status:             chunk.DurabilityStatus{Fence: 7, PendingBlocks: 2, PendingBytes: 4096},
	}
	v := &VFS{Store: store, writer: durabilityControlWriter{}}

	uid := uint32(utils.GetCurrentUID())
	data, errno := runDurabilityControl(v, uid, []byte{1})
	require.Zero(t, errno)
	var resp DurabilityResponse
	require.NoError(t, json.Unmarshal(data, &resp))
	require.Equal(t, store.status.Fence, resp.Fence)
	require.Equal(t, store.status.PendingBlocks, resp.PendingBlocks)

	unauthorized := uid + 1
	if unauthorized == 0 {
		unauthorized = 1
	}
	_, errno = runDurabilityControl(v, unauthorized, []byte{1})
	require.Equal(t, syscall.EACCES, errno)
}

func TestDurabilityControlTimeout(t *testing.T) {
	store := &durabilityControlStore{
		blockingChunkStore: &blockingChunkStore{},
		status:             chunk.DurabilityStatus{Fence: 9, PendingBlocks: 1},
		wait:               true,
	}
	v := &VFS{Store: store, writer: durabilityControlWriter{}}
	payload := utils.NewBuffer(9)
	payload.Put8(0)
	payload.Put64(20)

	data, errno := runDurabilityControl(v, 0, payload.Bytes())
	require.Zero(t, errno)
	var resp DurabilityResponse
	require.NoError(t, json.Unmarshal(data, &resp))
	require.True(t, strings.Contains(resp.Error, context.DeadlineExceeded.Error()), resp.Error)
}
