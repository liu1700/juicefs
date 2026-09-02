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
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/smithy-go"
	"github.com/juicedata/juicefs/pkg/plori/creds"
)

// CredentialPollInterval is how often the worker re-reads its credential file.
//
// The dominant delay in a rotation is not this: a Secret change reaches a node
// through the kubelet's projected-volume refresh, bounded by its sync period
// plus the secret manager's TTL cache — about two minutes at the defaults.
// This interval only decides how fast the worker reacts once the file it reads
// has actually changed, and the read is a hundred bytes off a tmpfs.
const CredentialPollInterval = 10 * time.Second

// CredentialRejectGrace is how long the worker keeps serving while the object
// store refuses its credential before it stops.
//
// The floor is a planned rotation: the old key dies the instant it is
// regenerated (PLO-351 — one key pair per subscription, regenerated wholesale,
// no overlap window is purchasable), so every live worker sees 403s from that
// instant until the new pair reaches its file — up to the ~2 minutes of
// kubelet propagation above, plus the plugin's re-stage and one poll. A grace
// shorter than that would turn every planned rotation into a fleet-wide
// unmount, which is the outage the rotation procedure exists to avoid.
//
// The ceiling is the Agent's experience: past the grace, cache-missing reads
// answer EIO and nothing the worker does can fix it, so stopping and letting
// the plugin retry the publish is strictly better than serving errors. Five
// minutes is twice the worst-case propagation and well inside the lease TTL
// arithmetic, which is unaffected either way — renewal talks to the
// control-plane, not to the store, so a credential outage never fences a mount
// by itself.
const CredentialRejectGrace = 5 * time.Minute

// CredentialVerdict is what the supervisor acts on.
type CredentialVerdict string

const (
	// CredentialOK — the credential is current and the store accepts it.
	CredentialOK CredentialVerdict = "ok"
	// CredentialStale — the credential file could not be read or parsed, so
	// the worker is still running on the last pair that was good. Reported,
	// not terminal: a projected volume is briefly absent while the kubelet
	// swaps it, and a half-written file is a file that is about to be whole.
	CredentialStale CredentialVerdict = "stale"
	// CredentialRejected — the store has refused this credential continuously
	// for longer than CredentialRejectGrace. Terminal.
	CredentialRejected CredentialVerdict = "rejected"
)

// ReasonCredentialRejected is the stop reason for a credential the store will
// not accept. It takes the same shape as an out-of-band fence — no barrier, no
// final sync, detach rather than unmount — for a reason that is not about
// authority at all: those two steps are object-store writes, and the store is
// exactly what is refusing this process. Running them would spend the whole
// remaining lease failing and report data loss (exit 69) for a condition that
// is retryable and usually transient.
const ReasonCredentialRejected = "credential_rejected"

// ReplicatorReloader is implemented by a Replicator whose object credential
// lives somewhere it cannot be changed in place — for Litestream, the
// environment of an exec'd child.
//
// It is a separate interface rather than a method on Replicator so that a
// replicator which needs nothing (a library, an in-memory fake) states that by
// not implementing it, and so the existing fakes in this package's tests are
// untouched.
type ReplicatorReloader interface {
	// ReloadCredentials makes the replicator use the credential the source
	// holds now. It is called only after a rotation, from the supervisor's own
	// goroutine, so it never runs concurrently with a barrier or a shutdown.
	ReloadCredentials(ctx context.Context) error
}

// CredentialWatcher owns the rotation half of the worker's credential.
//
// It is deliberately two inputs and one output: the file says what the
// credential IS, the object store says whether it WORKS, and the verdict is
// what the supervisor does about it. Neither input alone is enough — a
// readable file proves nothing about the store, and a 403 during a rotation is
// not a reason to stop if a new pair is seconds away.
type CredentialWatcher struct {
	source *creds.Source
	grace  time.Duration
	poll   time.Duration
	now    func() time.Time
	log    func(event string, kv ...any)

	// rejected is the fast path. Every object operation consults it; only a
	// transition takes the lock.
	rejected atomic.Bool

	mu         sync.Mutex
	rejectedAt time.Time
}

// NewCredentialWatcher wraps a source. A nil source is a programming error:
// the worker refuses to start without a credential, so by the time a watcher
// exists there is always one.
func NewCredentialWatcher(source *creds.Source, log func(string, ...any)) *CredentialWatcher {
	return &CredentialWatcher{
		source: source,
		grace:  CredentialRejectGrace,
		poll:   CredentialPollInterval,
		now:    time.Now,
		log:    log,
	}
}

// Interval is how often the supervisor should call Poll.
func (w *CredentialWatcher) Interval() time.Duration {
	if w == nil || w.poll <= 0 {
		return CredentialPollInterval
	}
	return w.poll
}

// Rotates reports whether this worker can pick up a new key without a remount.
func (w *CredentialWatcher) Rotates() bool { return w.source.Rotates() }

// Generation is the number of pairs this worker has run on, starting at 1.
func (w *CredentialWatcher) Generation() int64 { return w.source.Generation() }

// Stale reports whether the last attempt to re-read the credential failed.
func (w *CredentialWatcher) Stale() bool {
	at, _ := w.source.StaleSince()
	return !at.IsZero()
}

// Poll re-reads the credential and reports whether it changed.
//
// A rotation clears the rejection window: the store's refusal was of the key
// that has just been replaced, and the new one is entitled to its own grace.
func (w *CredentialWatcher) Poll() (rotated bool) {
	rotated, err := w.source.Reload()
	if err != nil {
		w.log("credential_refresh_failed", "error", err.Error())
		return false
	}
	if !rotated {
		return false
	}
	w.clear()
	w.log("credential_rotated", "generation", w.source.Generation())
	return true
}

// Observe records the outcome of one object-store operation. It is on the data
// path, so the common case — a successful operation while nothing is wrong —
// is a single atomic load.
func (w *CredentialWatcher) Observe(err error) {
	if err == nil {
		if w.rejected.Load() {
			w.clear()
		}
		return
	}
	if !IsCredentialRejected(err) {
		return
	}
	w.mark()
}

func (w *CredentialWatcher) mark() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.rejectedAt.IsZero() {
		w.rejectedAt = w.now()
		w.rejected.Store(true)
		w.log("credential_rejected_by_store", "grace", w.grace.String())
	}
}

func (w *CredentialWatcher) clear() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.rejectedAt.IsZero() {
		w.log("credential_accepted_again")
	}
	w.rejectedAt = time.Time{}
	w.rejected.Store(false)
}

// Verdict is the current state. The supervisor calls it on the credential
// tick.
func (w *CredentialWatcher) Verdict() CredentialVerdict {
	w.mu.Lock()
	rejectedAt := w.rejectedAt
	w.mu.Unlock()
	if !rejectedAt.IsZero() && w.now().Sub(rejectedAt) >= w.grace {
		return CredentialRejected
	}
	if w.Stale() {
		return CredentialStale
	}
	if !rejectedAt.IsZero() {
		// Refused, but inside the grace: the credential is not yet a reason to
		// stop, and reporting it as healthy would hide a rotation in progress.
		return CredentialStale
	}
	return CredentialOK
}

// IsCredentialRejected recognises the store refusing this process's identity,
// as opposed to refusing this particular request.
//
// On the production store the distinction collapses: Vultr Object Storage has
// one principal and no prefix policy that can bind to a second one (PLO-351),
// so a 403 is always about the key and never about which object was asked for.
// The API codes are still matched by name rather than by status alone, so the
// classification stays true if the store ever grows the scoping this project
// would like it to have.
func IsCredentialRejected(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, creds.ErrNoCredential) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "InvalidAccessKeyId", "SignatureDoesNotMatch", "AccessDenied",
			"ExpiredToken", "InvalidToken", "TokenRefreshRequired":
			return true
		}
	}
	var respErr interface{ HTTPStatusCode() int }
	if errors.As(err, &respErr) && respErr.HTTPStatusCode() == http.StatusForbidden {
		return true
	}
	return false
}
