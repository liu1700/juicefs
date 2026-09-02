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
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// NodeReplicator replicates this worker's metadata database through ONE
// Litestream process shared by every mount on the node.
//
// # Why this exists
//
// One `litestream replicate` per mount costs about 36 MiB fixed, measured, and
// eight of them is 288 MiB of a 2 GiB node — roughly 1.5 of the eight slots
// (PLO-385). A node-level replicator watching N databases was measured at
// 35.5 + 0.48·N MiB (PLO-346, `benchmark-real-node.md` §3), so the same eight
// mounts cost 39.3 MiB instead of 288. That is the difference between six of
// eight Agents being able to `git clone` at once and all eight, which is what
// ADR §4 B9's headroom arithmetic assumed all along.
//
// # Why registration and not a directory watch
//
// v0.5.17 offers two ways to give one daemon many databases, and only one of
// them can express the destination this design needs.
//
//   - `DBConfig{Dir, Pattern, Recursive, Watch}` (cmd/litestream/main.go:722-727,
//     cmd/litestream/directory_watcher.go) discovers databases under a
//     directory. Every database discovered that way shares the entry's replica
//     configuration, so the destination is a function of the file's path.
//   - `POST /register {path, replica_url}` (server.go:78, 571-627) registers
//     ONE database with ITS OWN replica client, built by
//     `NewReplicaClientFromURL` from a URL that carries bucket, prefix,
//     endpoint, region and path-style (s3/replica_client.go:145-228).
//
// The metadata replica prefix is `agents-meta/<vid>/g<epoch>/`, and the epoch
// is minted by the control-plane when the lease is acquired — it is not a
// property of any path on disk, and it changes for the same volume on the same
// state directory across generations (ADR D2). A directory watch cannot name
// it. Registration can, and it needs no config file to regenerate and no
// signal to reload, which is the second reason to prefer it: the daemon's
// config is written once by the plugin and never touched again.
//
// # What the shared daemon cannot do
//
// `Abort` — stop replicating WITHOUT a final sync — has no route. Both
// `POST /unregister` and `POST /stop` reach `db.Close(ctx)`, which performs a
// final local sync and a `syncReplicaWithRetry` before releasing
// (store.go:346-419, db.go:818-847). With a per-mount child, Abort was a
// SIGKILL and nothing more went out. See Abort for what replaces it and what
// that costs.
type NodeReplicator struct {
	// SocketPath is the node replicator's control socket. The plugin creates
	// it, owns its permissions, and passes it as `--replicator`.
	SocketPath string
	// DBPath is this worker's metadata database — the key every control call
	// is scoped by, and the reason one daemon serving many mounts is still
	// one mount's business per call.
	DBPath string

	// Restorer performs the one-shot restore. Restore is deliberately NOT a
	// control-socket operation: it runs before the database exists, it reads
	// from a DIFFERENT prefix than the one this generation writes to, and it
	// is over in seconds. `litestream restore` is a short-lived process whose
	// 36 MiB is a cold-start cost, not a steady-state one.
	Restorer *Litestream

	spec *MountSpec
}

// ProbeTimeout bounds one replication probe. It is short because the probe
// runs on the health tick and a replicator that cannot answer in this long is
// already the condition the probe exists to find.
const ProbeTimeout = 5 * time.Second

// AbortUnregisterTimeout is the budget given to the abort path's unregister.
// See Abort.
const AbortUnregisterTimeout = 1

var _ Replicator = (*NodeReplicator)(nil)
var _ ReplicationSupervisor = (*NodeReplicator)(nil)

// Configure records the spec and options the replica URL is built from. It
// mirrors Litestream.WriteConfig, which is the call the per-mount path makes
// at the same point in startup.
func (n *NodeReplicator) Configure(spec *MountSpec, opts MountOptions) error {
	n.spec = spec
	if n.Restorer != nil {
		return n.Restorer.WriteConfig(spec, opts)
	}
	return nil
}

// ReplicaURL is the destination this worker's database replicates to.
//
// Credentials are deliberately absent: the daemon reads
// AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY from its own environment
// (s3/replica_client.go:230-235), which the plugin supplies from the node
// Secret. A URL is an argv-adjacent string that ends up in the daemon's logs
// and in `litestream databases`, so it may never carry a key (threat-model
// F-9/F-11).
func (n *NodeReplicator) ReplicaURL() (string, error) {
	if n.spec == nil {
		return "", fmt.Errorf("%w: node replicator used before Configure", ErrSpec)
	}
	store := n.spec.ObjectStore
	if store.Bucket == "" {
		return "", fmt.Errorf("%w: object store has no bucket", ErrSpec)
	}
	prefix := strings.Trim(strings.TrimSuffix(n.spec.MetaPrefix, "/"), "/")
	if prefix == "" {
		return "", fmt.Errorf("%w: mount spec has no metadata prefix", ErrSpec)
	}
	q := url.Values{}
	if store.Endpoint != "" {
		q.Set("endpoint", store.Endpoint)
	}
	if store.Region != "" {
		q.Set("region", store.Region)
	}
	// Explicit rather than inherited: the query parser defaults path style on
	// for a custom endpoint but leaves it off without one, and every bucket
	// this design writes to is path-style.
	q.Set("force-path-style", "true")
	return "s3://" + store.Bucket + "/" + prefix + "?" + q.Encode(), nil
}

// Restore delegates to the one-shot restorer.
func (n *NodeReplicator) Restore(ctx context.Context, sourcePrefix string, opt RestoreOptions) error {
	if n.Restorer == nil {
		return fmt.Errorf("%w: node replicator has no restorer", ErrSpec)
	}
	return n.Restorer.Restore(ctx, sourcePrefix, opt)
}

type registerResponse struct {
	Status string `json:"status"`
	Path   string `json:"path"`
}

// Start registers this worker's database with the node replicator.
//
// `already_registered` is accepted rather than refused. It is what a crash
// restart at the same epoch looks like from the daemon's side — the daemon
// outlives workers by design (PLO-369) — and the registration it already
// holds is for the same database and the same prefix, because both are
// derived from the same spec.
func (n *NodeReplicator) Start(ctx context.Context) error {
	replica, err := n.ReplicaURL()
	if err != nil {
		return err
	}
	body, err := n.control(ctx, "/register", map[string]any{
		"path":        n.DBPath,
		"replica_url": replica,
	})
	if err != nil {
		return fmt.Errorf("register with the node replicator: %w", err)
	}
	var resp registerResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return fmt.Errorf("decode register response: %w", err)
	}
	if resp.Status != "registered" && resp.Status != "already_registered" {
		return fmt.Errorf("node replicator refused the registration: %q", resp.Status)
	}
	return nil
}

// SyncAndWait forces a sync of this database and blocks until the replica has
// it — the same call the per-mount child makes, on a different socket.
func (n *NodeReplicator) SyncAndWait(ctx context.Context) error {
	_, err := n.control(ctx, "/sync", map[string]any{
		"path":    n.DBPath,
		"wait":    true,
		"timeout": 30,
	})
	return err
}

// TxID is the replica's confirmed position for this database.
//
// It goes through `/sync -wait` rather than `GET /txid` on purpose: `/txid`
// answers `db.Pos()`, the LOCAL LTX position (server.go:287-315), while the
// durable point needs the position the object store actually holds, which is
// `db.Replica.Pos()` and is only returned by `/sync`
// (store.go:463-472). Naming a local position as the durable anchor would
// point a restore at transactions that never left the node.
func (n *NodeReplicator) TxID(ctx context.Context) (string, error) {
	body, err := n.control(ctx, "/sync", map[string]any{
		"path":    n.DBPath,
		"wait":    true,
		"timeout": 30,
	})
	if err != nil {
		return "", err
	}
	var resp syncResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return "", fmt.Errorf("decode sync response: %w", err)
	}
	if resp.ReplicatedTXID == 0 {
		return "", nil
	}
	return fmt.Sprintf("%016x", resp.ReplicatedTXID), nil
}

// Stop unregisters this database, which is where the shared daemon performs
// the final sync: `UnregisterDB` removes it from the store and calls
// `db.Close(ctx)`, which syncs locally and then to the replica before
// releasing (store.go:346-376, db.go:818-847). The daemon keeps running for
// every other mount on the node.
func (n *NodeReplicator) Stop(ctx context.Context) error {
	_, err := n.control(ctx, "/unregister", map[string]any{
		"path":    n.DBPath,
		"timeout": 30,
	})
	if err != nil {
		return fmt.Errorf("unregister from the node replicator: %w", err)
	}
	return nil
}

// Abort is the one place the shared daemon is weaker than a per-mount child,
// and it is weaker in a bounded, stated way.
//
// A writer fenced out of band must not push its remaining LTX into the
// metadata prefix its successor restores from: no barrier ran, so that
// history can reference blocks the object store never received (PLO-323 F-1).
// With a child, Abort was SIGKILL and nothing more went out. v0.5.17 has no
// route that detaches a database without a final sync — `/unregister` and
// `/stop` both reach `db.Close`, which syncs — so the strongest thing
// available is to unregister with a one-second budget, which lets the close
// fail on its own deadline after at most one more LTX file.
//
// What still holds after that:
//
//   - No `clean` marker is written on this path, so the next generation takes
//     the unconditional `fsck` and the restore-time missing-block repair
//     (crash-consistency.md §7 Rank 1). The protocol already assumes an
//     unclean generation's replica may reference blocks that are not there.
//   - A successor with a recorded `durable_point` restores TO it (PLO-391,
//     fork #47), and anything this writer pushes after the fence is later
//     than that point, so it is not in the restored tree at all.
//
// What is lost: the narrowing for a successor with NO durable point, which
// restores the latest transaction. That is a volume that has never completed
// a barrier — a first boot — and it is the case the fork issue filed against
// this documents.
func (n *NodeReplicator) Abort(ctx context.Context) error {
	_, err := n.control(ctx, "/unregister", map[string]any{
		"path":    n.DBPath,
		"timeout": AbortUnregisterTimeout,
	})
	if err != nil && !isNotRegistered(err) {
		// Reported, not returned: the caller is already stopping for a reason
		// this cannot change, and the shutdown path logs it.
		return fmt.Errorf("detach from the node replicator: %w", err)
	}
	return nil
}

// Probe asks the node replicator whether it is still replicating THIS
// database. A 404 means it is not — the daemon restarted and lost every
// registration, which is exactly the failure PLO-411 exists to notice — and
// Restart re-registers.
func (n *NodeReplicator) Probe(ctx context.Context) error {
	probe, cancel := context.WithTimeout(ctx, ProbeTimeout)
	defer cancel()
	_, err := n.control(probe, "/sync", map[string]any{"path": n.DBPath, "wait": false})
	return err
}

// Restart re-registers. `/register` is idempotent, so this is also the right
// repair for a probe that failed for a reason other than a lost registration:
// it either restores the registration or fails the same way the probe did.
func (n *NodeReplicator) Restart(ctx context.Context) error {
	return n.Start(ctx)
}

func (n *NodeReplicator) control(ctx context.Context, route string, body any) ([]byte, error) {
	return litestreamControl(ctx, n.SocketPath, route, body)
}

func isNotRegistered(err error) bool {
	var status *controlStatusError
	if !errors.As(err, &status) {
		return false
	}
	return status.Status == http.StatusNotFound
}
