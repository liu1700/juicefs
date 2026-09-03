//go:build plori
// +build plori

package mount

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// fakeLitestreamNode is a stand-in for the node-level `litestream replicate`
// daemon, speaking the v0.5.17 control protocol on a unix socket. It records
// every request so a test can assert on the WIRE rather than on the client's
// own bookkeeping — the client's whole job is to produce the right requests.
type fakeLitestreamNode struct {
	socket string
	srv    *httptest.Server

	mu         sync.Mutex
	calls      []call
	registered map[string]string // db path -> replica url
	// syncStatus, when set, is returned instead of 200 for /sync. It is how a
	// test makes the probe fail the way a restarted daemon makes it fail.
	syncStatus int
	replicated uint64
}

type call struct {
	Route string
	Body  map[string]any
}

func newFakeLitestreamNode(t *testing.T) *fakeLitestreamNode {
	t.Helper()
	// A unix socket path is capped near 100 bytes on macOS, so it goes in the
	// shortest temp dir available rather than under the test's own name.
	dir, err := os.MkdirTemp("", "ls")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	f := &fakeLitestreamNode{
		socket:     filepath.Join(dir, "s.sock"),
		registered: map[string]string{},
		replicated: 9,
	}
	ln, err := net.Listen("unix", f.socket)
	if err != nil {
		t.Fatalf("listen on %s: %v", f.socket, err)
	}
	f.srv = httptest.NewUnstartedServer(http.HandlerFunc(f.serve))
	f.srv.Listener.Close()
	f.srv.Listener = ln
	f.srv.Start()
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeLitestreamNode) serve(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)
	f.mu.Lock()
	f.calls = append(f.calls, call{Route: r.URL.Path, Body: body})
	status := f.syncStatus
	f.mu.Unlock()

	path, _ := body["path"].(string)
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case "/register":
		f.mu.Lock()
		_, exists := f.registered[path]
		f.registered[path], _ = body["replica_url"].(string)
		f.mu.Unlock()
		st := "registered"
		if exists {
			st = "already_registered"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": st, "path": path})
	case "/unregister":
		f.mu.Lock()
		delete(f.registered, path)
		f.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "unregistered", "path": path, "txid": 9})
	case "/sync":
		f.mu.Lock()
		_, known := f.registered[path]
		replicated := f.replicated
		f.mu.Unlock()
		if status != 0 {
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "database not found"})
			return
		}
		if !known {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "database not found"})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "synced", "path": path, "txid": 9, "replicated_txid": replicated,
		})
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (f *fakeLitestreamNode) routes() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.calls))
	for _, c := range f.calls {
		out = append(out, c.Route)
	}
	return out
}

func (f *fakeLitestreamNode) last(route string) map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := len(f.calls) - 1; i >= 0; i-- {
		if f.calls[i].Route == route {
			return f.calls[i].Body
		}
	}
	return nil
}

func newNodeReplicator(t *testing.T, f *fakeLitestreamNode) *NodeReplicator {
	t.Helper()
	n := &NodeReplicator{SocketPath: f.socket, DBPath: filepath.Join(t.TempDir(), "meta.db")}
	if err := n.Configure(testSpec(), ParseMountOptions(nil)); err != nil {
		t.Fatalf("configure: %v", err)
	}
	return n
}

// The whole reason to register per database rather than watch a directory is
// that the destination carries the writer epoch, which no path on disk does.
// This asserts the URL the worker actually puts on the wire: the volume's own
// per-epoch metadata prefix, the endpoint and region from the spec, path style
// on, and — the part a leak would be invisible without — no credential.
func TestRegistrationNamesThisEpochsPrefixAndNoCredential(t *testing.T) {
	f := newFakeLitestreamNode(t)
	n := newNodeReplicator(t, f)

	if err := n.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}

	body := f.last("/register")
	if body == nil {
		t.Fatal("no /register call was made")
	}
	if got := body["path"]; got != n.DBPath {
		t.Errorf("registered path = %v, want %s", got, n.DBPath)
	}
	raw, _ := body["replica_url"].(string)
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("replica url %q does not parse: %v", raw, err)
	}
	spec := testSpec()
	if u.Scheme != "s3" || u.Host != spec.ObjectStore.Bucket {
		t.Errorf("replica url = %q, want s3://%s/...", raw, spec.ObjectStore.Bucket)
	}
	wantPrefix := strings.Trim(spec.MetaPrefix, "/")
	if strings.TrimPrefix(u.Path, "/") != wantPrefix {
		t.Errorf("replica prefix = %q, want %q — the epoch is in this string or it is nowhere", u.Path, wantPrefix)
	}
	q := u.Query()
	if q.Get("endpoint") != spec.ObjectStore.Endpoint {
		t.Errorf("endpoint = %q, want %q", q.Get("endpoint"), spec.ObjectStore.Endpoint)
	}
	if q.Get("force-path-style") != "true" {
		t.Errorf("force-path-style = %q, want true", q.Get("force-path-style"))
	}
	// The daemon reads the key from its own environment. A URL is logged, and
	// `litestream databases` prints it.
	for _, banned := range []string{"AKIA", "access", "secret", "key", "@"} {
		if strings.Contains(strings.ToLower(raw), strings.ToLower(banned)) {
			t.Errorf("replica url %q contains %q — it must carry no credential", raw, banned)
		}
	}
}

// A crash restart at the same epoch finds its own registration still there,
// because the daemon outlives workers by design (PLO-369). It is a success,
// not a conflict: same database, same prefix, both derived from one spec.
func TestReRegisteringTheSameDatabaseSucceeds(t *testing.T) {
	f := newFakeLitestreamNode(t)
	n := newNodeReplicator(t, f)
	if err := n.Start(context.Background()); err != nil {
		t.Fatalf("first start: %v", err)
	}
	if err := n.Start(context.Background()); err != nil {
		t.Fatalf("second start on an already-registered database: %v", err)
	}
}

// Stop is where the shared daemon performs this database's final sync, and it
// must not disturb the other mounts on the node: unregister, never a signal.
func TestStopUnregistersAndLeavesTheDaemonRunning(t *testing.T) {
	f := newFakeLitestreamNode(t)
	n := newNodeReplicator(t, f)
	if err := n.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := n.Stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if body := f.last("/unregister"); body == nil || body["path"] != n.DBPath {
		t.Fatalf("/unregister body = %v, want this worker's database", body)
	}
	f.mu.Lock()
	left := len(f.registered)
	f.mu.Unlock()
	if left != 0 {
		t.Errorf("%d databases still registered after stop", left)
	}
}

// Abort has no no-sync route in v0.5.17, so it unregisters on the shortest
// budget the API accepts. The assertion is on that budget, because it is the
// only thing separating this path from an ordinary clean stop.
func TestAbortUnregistersOnTheShortestBudget(t *testing.T) {
	f := newFakeLitestreamNode(t)
	n := newNodeReplicator(t, f)
	if err := n.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := n.Abort(context.Background()); err != nil {
		t.Fatalf("abort: %v", err)
	}
	body := f.last("/unregister")
	if body == nil {
		t.Fatal("abort made no /unregister call")
	}
	if got, ok := body["timeout"].(float64); !ok || int(got) != AbortUnregisterTimeout {
		t.Errorf("abort timeout = %v, want %d", body["timeout"], AbortUnregisterTimeout)
	}
}

// The durable point's anchor has to be the position the OBJECT STORE holds,
// not the position the local database has reached. Those differ by exactly the
// upload that has not happened yet, and restoring to the local one points at
// transactions that never left the node.
func TestTxIDReportsTheReplicatedPositionNotTheLocalOne(t *testing.T) {
	f := newFakeLitestreamNode(t)
	f.replicated = 7
	n := newNodeReplicator(t, f)
	if err := n.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	got, err := n.TxID(context.Background())
	if err != nil {
		t.Fatalf("txid: %v", err)
	}
	// The fake answers txid 9 (local) and replicated_txid 7.
	if got != "0000000000000007" {
		t.Fatalf("TxID = %q, want the replicated position 0000000000000007", got)
	}
	if body := f.last("/sync"); body == nil || body["wait"] != true {
		t.Errorf("/sync body = %v, want wait:true — the position must be current", body)
	}
}

// A replica that has never received a transaction has no anchor, and reporting
// a zero as one would let a restore aim at transaction 0.
func TestTxIDIsEmptyWhenNothingHasBeenReplicated(t *testing.T) {
	f := newFakeLitestreamNode(t)
	f.replicated = 0
	n := newNodeReplicator(t, f)
	if err := n.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	got, err := n.TxID(context.Background())
	if err != nil {
		t.Fatalf("txid: %v", err)
	}
	if got != "" {
		t.Fatalf("TxID = %q, want empty", got)
	}
}

// The failure PLO-411 exists for, in the node topology: the daemon restarted,
// so it no longer knows this database, and the worker is still happily serving
// writes. The probe has to see it, and the repair is to register again.
func TestProbeFailsWhenTheDaemonForgotThisDatabaseAndRestartRegistersIt(t *testing.T) {
	f := newFakeLitestreamNode(t)
	n := newNodeReplicator(t, f)
	if err := n.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := n.Probe(context.Background()); err != nil {
		t.Fatalf("probe on a registered database: %v", err)
	}

	// The daemon comes back empty.
	f.mu.Lock()
	f.registered = map[string]string{}
	f.mu.Unlock()

	err := n.Probe(context.Background())
	if err == nil {
		t.Fatal("probe passed while the daemon did not know this database")
	}
	var status *controlStatusError
	if !errors.As(err, &status) || status.Status != http.StatusNotFound {
		t.Fatalf("probe error = %v, want a 404 the caller can tell apart", err)
	}

	if err := n.Restart(context.Background()); err != nil {
		t.Fatalf("restart: %v", err)
	}
	if err := n.Probe(context.Background()); err != nil {
		t.Fatalf("probe after restart: %v", err)
	}
}

// A daemon that is gone entirely is the other half of the same failure: no
// socket to dial. The probe must report it rather than block on a dial.
func TestProbeFailsWhenTheSocketIsGone(t *testing.T) {
	f := newFakeLitestreamNode(t)
	n := newNodeReplicator(t, f)
	if err := n.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	f.srv.Close()
	_ = os.Remove(f.socket)
	if err := n.Probe(context.Background()); err == nil {
		t.Fatal("probe passed with no replicator listening")
	}
}

// Every control call is scoped by this worker's database path. One daemon
// serves every mount on the node, so a call that forgot the path would act on
// somebody else's volume.
func TestEveryControlCallNamesThisWorkersDatabase(t *testing.T) {
	f := newFakeLitestreamNode(t)
	n := newNodeReplicator(t, f)
	ctx := context.Background()
	if err := n.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := n.SyncAndWait(ctx); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if _, err := n.TxID(ctx); err != nil {
		t.Fatalf("txid: %v", err)
	}
	if err := n.Probe(ctx); err != nil {
		t.Fatalf("probe: %v", err)
	}
	if err := n.Stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}
	f.mu.Lock()
	calls := append([]call(nil), f.calls...)
	f.mu.Unlock()
	if len(calls) == 0 {
		t.Fatal("no control calls were made")
	}
	for _, c := range calls {
		if c.Body["path"] != n.DBPath {
			t.Errorf("%s named path %v, want %s", c.Route, c.Body["path"], n.DBPath)
		}
	}
	if got := f.routes(); len(got) < 5 {
		t.Errorf("routes = %v, want one per operation", got)
	}
}

// Configure is what turns a spec into a destination, so a replicator used
// without it must refuse rather than register at an empty prefix — which would
// replicate every volume on the node into the bucket root.
func TestReplicaURLRefusesWithoutASpec(t *testing.T) {
	n := &NodeReplicator{SocketPath: "/nonexistent", DBPath: "/tmp/meta.db"}
	if _, err := n.ReplicaURL(); !errors.Is(err, ErrSpec) {
		t.Fatalf("ReplicaURL error = %v, want ErrSpec", err)
	}
}
