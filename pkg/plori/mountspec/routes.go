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

package mountspec

// The control-plane's internal storage surface, as a mount worker calls it: the
// full path of every route, one constant each.
//
// They live here rather than beside the client in pkg/plori/mount for the same
// reason the MountSpec types moved here (liu1700/juicefs#42): the other end of
// this contract is in another repository, and the `plori` build tag hides
// pkg/plori/mount from every test that could compare the two. A route the
// control-plane serves and nobody calls is invisible to both sides otherwise —
// which is exactly what happened to /format-ack, registered, handled,
// authorised, audited and DB-tested in the control-plane with no caller
// anywhere in the system, until a volume that never reached `active` split one
// Agent across two filesystems in staging (PLO-420).
const (
	RouteMountSpec    = "/v1/internal/storage/mount-spec"
	RouteLeaseRenew   = "/v1/internal/storage/lease/renew"
	RouteLeaseRelease = "/v1/internal/storage/lease/release"
	RouteUsage        = "/v1/internal/storage/usage"
	RouteDurablePoint = "/v1/internal/storage/durable-point"
	RouteFormatAck    = "/v1/internal/storage/format-ack"
)

// ClientRoutes is every route pkg/plori/mount's Client speaks, and it is a
// claim rather than a comment: the fork asserts it against the paths the client
// actually posts to (TestTheClientSpeaksEveryRouteItDeclares), and
// plori-runtime's services/storage-worker/internal/mountwire asserts it against
// the control-plane's own published surface, both ways.
//
// RouteMountSpec is deliberately absent. It is the CSI node plugin's call: by
// the time this worker runs, the plugin has already spent it and the resulting
// spec is in --spec-file.
func ClientRoutes() []string {
	return []string{
		RouteLeaseRenew,
		RouteLeaseRelease,
		RouteUsage,
		RouteDurablePoint,
		RouteFormatAck,
	}
}
