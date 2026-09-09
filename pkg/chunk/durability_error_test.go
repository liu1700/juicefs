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

package chunk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
	"github.com/stretchr/testify/require"
)

// errUploadRefused stands for any object-store refusal. What this test pins is
// the property that makes the classes below distinguishable at all: whatever
// the backend returned reaches the fence's caller as a value rather than as
// text (PLO-458).
var errUploadRefused = errors.New("upload refused")

// erroringPutStore refuses every upload with one error value.
type erroringPutStore struct {
	object.ObjectStorage
	err error
}

func (s *erroringPutStore) Put(ctx context.Context, key string, in io.Reader, getters ...object.AttrGetter) error {
	return s.err
}

func TestRemoteDurabilityFenceKeepsTheUploadErrorType(t *testing.T) {
	blob := &erroringPutStore{ObjectStorage: newTestStorage(t), err: errUploadRefused}
	store := newDurabilityTestStore(t, blob)
	durable := store.(RemoteDurabilityStore)
	writeDurabilityTestSlice(t, store, 107, []byte("refused"))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	status, err := durable.RemoteDurability(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, errUploadRefused)
	// The text half of the report is unchanged: DurabilityStatus.LastError is a
	// JSON wire field and stays a string.
	require.Contains(t, status.LastError, errUploadRefused.Error())
}

// The classes themselves: a backend that refuses the upload with a classified
// error (pkg/object attaches the class in the S3 and restful backends) reaches
// the fence's caller with the class intact, through the retry wrap in upload()
// and the fence's own wrap. This is what lets the mount supervisor answer "the
// bucket is full" rather than "upload failed" (PLO-458).
func TestRemoteDurabilityFenceKeepsTheObjectStoreClass(t *testing.T) {
	cases := []struct {
		class   error
		refused error
	}{
		{object.ErrInsufficientStorage, fmt.Errorf("status: 507, message: over quota: %w", object.ErrInsufficientStorage)},
		{object.ErrAccessDenied, fmt.Errorf("status: 403, message: signature mismatch: %w", object.ErrAccessDenied)},
	}
	for _, c := range cases {
		class := c.class
		t.Run(class.Error(), func(t *testing.T) {
			blob := &erroringPutStore{ObjectStorage: newTestStorage(t), err: c.refused}
			store := newDurabilityTestStore(t, blob)
			durable := store.(RemoteDurabilityStore)
			writeDurabilityTestSlice(t, store, 108, []byte("classified"))

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			_, err := durable.RemoteDurability(ctx)
			require.Error(t, err)
			require.ErrorIs(t, err, class)
		})
	}
}
