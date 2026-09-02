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

// Package creds holds the one object-store credential a plori-mount worker
// uses, and lets it be replaced while the process runs.
//
// Why this exists at all. Production's object store is Vultr Object Storage,
// which has no STS, no IAM and exactly one access/secret key pair per
// subscription (PLO-351, probed 2026-09-02: every STS and IAM action answers
// HTTP 405). So there is no per-Agent credential to expire, and ADR §5 C1
// takes option 1: the trusted per-mount worker holds the bucket-wide key and
// the isolation is the process boundary plus the writer lease, not the
// credential. What is left of "refreshable credentials" — and it is the part
// that matters operationally — is that the key must be replaceable in a
// running worker without a remount, because remounting the fleet to roll a key
// is an outage the size of the fleet.
//
// The package has no build tag: pkg/object (untagged) installs a Source as its
// S3 credential provider under the plori tag, and pkg/plori/mount consumes the
// same Source. A wire-shaped thing that two tagged packages must agree on is
// exactly what pkg/plori/mountspec learned to be untagged for (PLO-395).
package creds

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// ErrNoCredential means the source has never held a usable pair. It is fatal
// at startup: a worker that cannot authenticate to the object store must
// refuse to mount rather than serve EIO.
var ErrNoCredential = errors.New("no object credential")

// Pair is one access/secret key pair.
//
// The fields are strings rather than []byte on purpose, and the reason is
// written down here because PLO-394 F-8 asks for the opposite. Every consumer
// of this pair takes a string: aws.Credentials.AccessKeyID, meta.Format's
// AccessKey/SecretKey, and the child process environment Litestream reads. A
// []byte that is converted to a string at each of those three boundaries
// leaves three immutable copies on the heap that wiping the []byte does not
// touch, so the wipe would be ceremony rather than a control. The shred is the
// process exit, which is immediate on every terminal path (cmd/plori_mount.go
// exitTerminal), and the real reduction this package delivers instead is that
// the pair no longer has to be in the worker's own environment at all — see
// Source.Env.
type Pair struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
}

func (p Pair) valid() bool { return p.AccessKeyID != "" && p.SecretAccessKey != "" }

// Source is the process's single credential holder.
//
// Reads are lock-free: the current pair lives behind an atomic pointer, so a
// caller either sees the whole old pair or the whole new one and never a
// half-rotated mixture. Every consumer that needs the credential at
// request time goes through Provider, which hands out the same
// *aws.CredentialsCache to every S3 client in the process; a rotation
// invalidates that one cache and the next request signs with the new key,
// while a request that has already been signed completes with the key it
// started with.
type Source struct {
	// path is the file the credential is read from. Empty means the pair was
	// supplied once at construction and can never rotate.
	path string

	// readFile and now are seams for the tests. Nothing else replaces them.
	readFile func(string) ([]byte, error)
	now      func() time.Time

	cur   atomic.Pointer[Pair]
	gen   atomic.Int64
	stale atomic.Pointer[refreshError]

	// mu serialises Reload against itself. Retrieve never takes it.
	mu    sync.Mutex
	cache *aws.CredentialsCache
}

type refreshError struct {
	at  time.Time
	err error
}

// Static builds a source that can never rotate. It is the environment-variable
// path: the plugin passes AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY, which a
// running process cannot be handed a new value for, so rotation needs a
// remount. Health reports it so an operator can see that rotation is not armed
// on this mount.
func Static(accessKeyID, secretAccessKey string) (*Source, error) {
	p := Pair{AccessKeyID: accessKeyID, SecretAccessKey: secretAccessKey}
	if !p.valid() {
		return nil, ErrNoCredential
	}
	s := newSource("")
	s.cur.Store(&p)
	s.gen.Store(1)
	return s, nil
}

// FromFile builds a rotating source over a JSON document the CSI node plugin
// writes into the worker's private run directory:
//
//	{"access_key_id":"…","secret_access_key":"…"}
//
// One file rather than the node Secret's two projected files, and not the
// projected directory itself, for two independent reasons.
//
//  1. The worker may run as a non-root uid (fuse-csi-node worker.go
//     workerSysProcAttr), and the projected Secret is mode 0400 owned by root,
//     so the worker cannot read it. The plugin already re-stages the projected
//     ServiceAccount token into this same directory for the same reason; the
//     credential follows the token's pattern.
//  2. Two files cannot be read atomically. The kubelet publishes a projected
//     volume by renaming a `..data` symlink, and the two key files are symlinks
//     through it, so two ReadFile calls can straddle the rename and produce an
//     old id with a new secret. One document is one open, and the open resolves
//     the symlink once.
//
// The file must be present and well formed at startup; a worker with no
// credential cannot mount.
func FromFile(path string) (*Source, error) {
	s := newSource(path)
	if _, err := s.Reload(); err != nil {
		return nil, err
	}
	return s, nil
}

func newSource(path string) *Source {
	s := &Source{path: path, readFile: os.ReadFile, now: time.Now}
	s.cache = aws.NewCredentialsCache(providerFunc(s.retrieve))
	return s
}

// Rotates reports whether this source can pick up a new pair without a
// remount.
func (s *Source) Rotates() bool { return s.path != "" }

// Current is the pair in force right now.
func (s *Source) Current() Pair {
	if p := s.cur.Load(); p != nil {
		return *p
	}
	return Pair{}
}

// Generation counts accepted pairs. It starts at 1 and increases by one every
// time a genuinely different pair is loaded, so health.json can say "this
// worker is on the second key" without saying anything about the key.
func (s *Source) Generation() int64 { return s.gen.Load() }

// StaleSince reports when the last refresh failed, and with what, while the
// worker is still running on the previous good pair. Zero time means the last
// refresh succeeded.
func (s *Source) StaleSince() (time.Time, error) {
	if e := s.stale.Load(); e != nil {
		return e.at, e.err
	}
	return time.Time{}, nil
}

// Reload re-reads the file and swaps the pair in if it is both readable and
// different. It reports whether the pair changed.
//
// A read or parse failure is NOT fatal and does NOT clear the current pair:
// a projected volume is momentarily absent while the kubelet swaps it, and a
// half-written file is a file that is about to be complete. The worker keeps
// the last good pair and says so in health.json; only the object store gets to
// decide that a credential is dead, and that path is the caller's
// (mount.CredentialWatcher).
func (s *Source) Reload() (bool, error) {
	if s.path == "" {
		return false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := s.readFile(s.path)
	if err != nil {
		return false, s.refreshFailed(fmt.Errorf("read credential file: %w", err))
	}
	var next Pair
	if err := json.Unmarshal(data, &next); err != nil {
		// The error deliberately does not wrap the decoder's message: a JSON
		// decoder quotes the offending input, and the offending input is a
		// secret (threat-model F-11).
		return false, s.refreshFailed(errors.New("credential file is not a JSON credential document"))
	}
	if !next.valid() {
		return false, s.refreshFailed(errors.New("credential file names an empty access key id or secret"))
	}
	s.stale.Store(nil)

	prev := s.cur.Load()
	if prev != nil && *prev == next {
		return false, nil
	}
	// Publish the whole pair in one store, then drop the SDK's cached copy so
	// the next request signs with it. The order matters: invalidating first
	// would let a request in flight between the two steps retrieve the pair
	// that is on its way out.
	s.cur.Store(&next)
	s.gen.Add(1)
	s.cache.Invalidate()
	return prev != nil, nil
}

func (s *Source) refreshFailed(err error) error {
	s.stale.Store(&refreshError{at: s.now(), err: err})
	if s.cur.Load() == nil {
		return fmt.Errorf("%w: %s", ErrNoCredential, err)
	}
	return err
}

// Provider is the credential provider every S3 client in this process shares.
//
// It is already an *aws.CredentialsCache, which config.LoadDefaultConfig
// detects and does not wrap a second time (aws-sdk-go-v2 config
// resolve_credentials.go wrapWithCredentialsCache returns an existing cache
// unchanged). That is what makes rotation exact rather than eventual: the
// credentials this source hands out never expire on their own, so there is no
// polling interval to tune and no window in which the SDK keeps signing with a
// key that has been replaced — Reload invalidates the one cache and the very
// next request retrieves the new pair.
func (s *Source) Provider() aws.CredentialsProvider { return s.cache }

func (s *Source) retrieve(_ context.Context) (aws.Credentials, error) {
	p := s.cur.Load()
	if p == nil {
		return aws.Credentials{}, ErrNoCredential
	}
	return aws.Credentials{
		AccessKeyID:     p.AccessKeyID,
		SecretAccessKey: p.SecretAccessKey,
		Source:          ProviderName,
		// CanExpire stays false. Expiry is how a provider says "come back
		// later"; this one says "ask me again the moment I tell you to", which
		// is Reload's cache invalidation.
	}, nil
}

// ProviderName is what shows up in aws.Credentials.Source. It names the
// mechanism, never the key.
const ProviderName = "plori-mount/creds"

type providerFunc func(context.Context) (aws.Credentials, error)

func (f providerFunc) Retrieve(ctx context.Context) (aws.Credentials, error) { return f(ctx) }

// Env returns environ plus the AWS variables a child process needs, without
// putting either value in this process's own environment.
//
// Litestream is exec'd, reads its object credential from AWS_ACCESS_KEY_ID /
// AWS_SECRET_ACCESS_KEY, and cannot be handed a new one except by being
// restarted. Building the child's environment here is what lets the worker
// itself run with no credential in /proc/self/environ at all on the file path,
// which is one whole surface the hygiene audit no longer has to argue about.
func (s *Source) Env(environ []string) []string {
	p := s.Current()
	return append(environ,
		"AWS_ACCESS_KEY_ID="+p.AccessKeyID,
		"AWS_SECRET_ACCESS_KEY="+p.SecretAccessKey,
	)
}
