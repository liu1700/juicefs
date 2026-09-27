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

// Package gatewaycontrol defines the private Workspace writer control wire.
package gatewaycontrol

const (
	BarrierRoute = "/v1/barrier"
	CloneRoute   = "/v1/clone"
)

// Identity binds each private control request and response to one mounted
// storage generation.
type Identity struct {
	StorageVolumeID string `json:"storage_volume_id"`
	FormatUUID      string `json:"format_uuid"`
	Generation      int64  `json:"generation"`
	FenceEpoch      int64  `json:"fence_epoch"`
}

// BarrierRequest asks the mounted writer to run one data-plane durability
// barrier. It carries no mount path, options, or timeout.
type BarrierRequest struct {
	Identity
}

// BarrierResponse is the native durability evidence returned by the writer.
type BarrierResponse struct {
	Identity
	Fence                       *uint64 `json:"fence"`
	LastSuccessfulFence         *uint64 `json:"last_successful_fence"`
	LastSuccessfulBarrierUnixMs int64   `json:"last_successful_barrier_unix_ms"`
}

// CloneRequest asks the mounted writer to clone one verified native source
// inode into a fixed private staging destination.
type CloneRequest struct {
	Identity
	Source            string `json:"source"`
	Destination       string `json:"destination"`
	SourceNativeInode uint64 `json:"source_native_inode"`
}

// CloneResponse acknowledges a completed native clone.
type CloneResponse struct {
	Identity
	Cloned bool `json:"cloned"`
}
