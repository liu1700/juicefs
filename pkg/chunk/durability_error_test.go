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
	"io"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
	"github.com/stretchr/testify/require"
)

// errUploadRefused stands for the class of object-store refusal the mount
// supervisor has to be able to tell apart: a 507 from a full bucket, a 403 on a
// rotated credential, a connection failure. pkg/object defines no sentinel for
// any of them — the S3 backend returns the SDK's error unchanged
// (pkg/object/s3.go:182-188) — so what this test pins is the property that
// makes such a distinction possible at all: whatever the backend returned
// reaches the fence's caller as a value rather than as text (PLO-458).
var errUploadRefused = errors.New("insufficient storage")

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
