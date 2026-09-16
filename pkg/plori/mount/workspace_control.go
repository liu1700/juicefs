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
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/plori/gatewaycontrol"
	"golang.org/x/sys/unix"
)

const (
	workspaceControlMaxBody           = 16 << 10
	workspaceControlReadHeaderTimeout = 2 * time.Second
	workspaceControlReadTimeout       = 5 * time.Second
	workspaceControlWriteTimeout      = 5 * time.Second
	workspaceControlIdleTimeout       = 5 * time.Second
)

type workspaceControlRequest struct {
	ctx     context.Context
	barrier *gatewaycontrol.BarrierRequest
	clone   *gatewaycontrol.CloneRequest
	reply   chan workspaceControlReply
}

type workspaceControlReply struct {
	barrier *gatewaycontrol.BarrierResponse
	clone   *gatewaycontrol.CloneResponse
	err     error
}

type workspaceControlPeerKey struct{}

// workspaceControlServer accepts only root-owned local connections. It never
// executes an operation itself: the Supervisor loop schedules its request on
// the existing serialized barrier worker.
type workspaceControlServer struct {
	ctx      context.Context
	cancel   context.CancelFunc
	identity gatewaycontrol.Identity
	requests chan workspaceControlRequest
	listener net.Listener
	server   *http.Server
	mux      *http.ServeMux
	socket   string
	done     chan struct{}
	active   func() bool

	mu       sync.Mutex
	busy     bool
	closing  bool
	handlers sync.WaitGroup
}

func newWorkspaceControlServer(parent context.Context, socket string, identity gatewaycontrol.Identity, active func() bool) (*workspaceControlServer, error) {
	if err := verifyWorkspaceControlStateDir(filepath.Dir(socket)); err != nil {
		return nil, err
	}
	if err := removeWorkspaceControlSocket(socket); err != nil {
		return nil, err
	}
	ln, err := net.Listen("unix", socket)
	if err != nil {
		return nil, err
	}
	if err = os.Chmod(socket, 0o600); err != nil {
		_ = ln.Close()
		_ = os.Remove(socket)
		return nil, err
	}
	if err = verifyWorkspaceControlSocket(socket); err != nil {
		_ = ln.Close()
		_ = os.Remove(socket)
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	s := &workspaceControlServer{
		ctx:      ctx,
		cancel:   cancel,
		identity: identity,
		requests: make(chan workspaceControlRequest, 1),
		listener: ln,
		socket:   socket,
		done:     make(chan struct{}),
		active:   active,
	}
	mux := http.NewServeMux()
	mux.HandleFunc(gatewaycontrol.BarrierRoute, s.handleBarrier)
	mux.HandleFunc(gatewaycontrol.CloneRoute, s.handleClone)
	s.mux = mux
	s.server = &http.Server{
		Handler:           http.HandlerFunc(s.serveHTTP),
		ReadHeaderTimeout: workspaceControlReadHeaderTimeout,
		ReadTimeout:       workspaceControlReadTimeout,
		IdleTimeout:       workspaceControlIdleTimeout,
		BaseContext: func(net.Listener) context.Context {
			return s.ctx
		},
		ConnContext: func(ctx context.Context, conn net.Conn) context.Context {
			uid, ok := workspaceControlPeerUID(conn)
			return context.WithValue(ctx, workspaceControlPeerKey{}, workspaceControlPeer{uid: uid, ok: ok})
		},
	}
	go func() {
		_ = s.server.Serve(ln)
		close(s.done)
	}()
	return s, nil
}

type workspaceControlPeer struct {
	uid uint32
	ok  bool
}

func workspaceControlPeerUID(conn net.Conn) (uint32, bool) {
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		return 0, false
	}
	raw, err := unixConn.SyscallConn()
	if err != nil {
		return 0, false
	}
	var cred *unix.Ucred
	var controlErr error
	err = raw.Control(func(fd uintptr) { cred, controlErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED) })
	if err != nil || controlErr != nil || cred == nil {
		return 0, false
	}
	return cred.Uid, true
}

func verifyWorkspaceControlStateDir(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return errors.New("workspace control state path is not a directory")
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || st.Uid != 0 {
		return errors.New("workspace control state directory is not root-owned")
	}
	if info.Mode().Perm() != 0o700 {
		return errors.New("workspace control state directory permissions are not 0700")
	}
	return nil
}

func removeWorkspaceControlSocket(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if err = verifyWorkspaceControlInfo(info); err != nil {
		return fmt.Errorf("refuse stale workspace control path: %w", err)
	}
	return os.Remove(path)
}

func verifyWorkspaceControlSocket(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if err = verifyWorkspaceControlInfo(info); err != nil {
		return err
	}
	if info.Mode().Perm() != 0o600 {
		return errors.New("socket permissions are not 0600")
	}
	return nil
}

func verifyWorkspaceControlInfo(info os.FileInfo) error {
	if info.Mode()&os.ModeSocket == 0 {
		return errors.New("not a socket")
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || st.Uid != 0 {
		return errors.New("socket is not root-owned")
	}
	return nil
}

func (s *workspaceControlServer) handleBarrier(w http.ResponseWriter, r *http.Request) {
	var request gatewaycontrol.BarrierRequest
	if !s.decodeRequest(w, r, gatewaycontrol.BarrierRoute, &request) {
		return
	}
	if request.Identity != s.identity {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	reply, ok := s.submit(r.Context(), workspaceControlRequest{barrier: &request})
	if !ok {
		http.Error(w, "workspace writer is not ready", http.StatusServiceUnavailable)
		return
	}
	if reply.err != nil || reply.barrier == nil || !s.writeSuccess(w, r.Context(), reply.barrier) {
		http.Error(w, "workspace writer is not ready", http.StatusServiceUnavailable)
		return
	}
}

func (s *workspaceControlServer) handleClone(w http.ResponseWriter, r *http.Request) {
	var request gatewaycontrol.CloneRequest
	if !s.decodeRequest(w, r, gatewaycontrol.CloneRoute, &request) {
		return
	}
	if request.Identity != s.identity {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	reply, ok := s.submit(r.Context(), workspaceControlRequest{clone: &request})
	if !ok {
		http.Error(w, "workspace writer is not ready", http.StatusServiceUnavailable)
		return
	}
	if reply.err != nil || reply.clone == nil || !s.writeSuccess(w, r.Context(), reply.clone) {
		http.Error(w, "workspace writer is not ready", http.StatusServiceUnavailable)
		return
	}
}

func (s *workspaceControlServer) decodeRequest(w http.ResponseWriter, r *http.Request, route string, request any) bool {
	peer, _ := r.Context().Value(workspaceControlPeerKey{}).(workspaceControlPeer)
	if !peer.ok || peer.uid != 0 {
		http.Error(w, "forbidden", http.StatusForbidden)
		return false
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return false
	}
	if r.URL.Path != route || r.URL.RawQuery != "" || r.ContentLength < 0 || r.ContentLength > workspaceControlMaxBody {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return false
	}
	defer r.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(r.Body, workspaceControlMaxBody+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(request); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return false
	}
	return true
}

func (s *workspaceControlServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if !s.enterHandler() {
		http.Error(w, "workspace writer is not ready", http.StatusServiceUnavailable)
		return
	}
	defer s.handlers.Done()
	s.mux.ServeHTTP(w, r)
}

func (s *workspaceControlServer) enterHandler() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing || s.ctx.Err() != nil {
		return false
	}
	s.handlers.Add(1)
	return true
}

func (s *workspaceControlServer) writeSuccess(w http.ResponseWriter, requestCtx context.Context, value any) bool {
	payload, err := json.Marshal(value)
	if err != nil {
		return false
	}
	s.mu.Lock()
	ready := !s.closing && s.ctx.Err() == nil && requestCtx.Err() == nil && s.active != nil && s.active()
	s.mu.Unlock()
	if !ready {
		return false
	}
	// Native work uses its lease-bound context. Start the network-write budget
	// only now; a Server.WriteTimeout would also count the time spent cloning.
	if err := http.NewResponseController(w).SetWriteDeadline(time.Now().Add(workspaceControlWriteTimeout)); err != nil {
		return false
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(append(payload, '\n'))
	return err == nil
}

func (s *workspaceControlServer) submit(ctx context.Context, request workspaceControlRequest) (workspaceControlReply, bool) {
	s.mu.Lock()
	if s.busy || s.closing || s.ctx.Err() != nil {
		s.mu.Unlock()
		return workspaceControlReply{}, false
	}
	s.busy = true
	s.mu.Unlock()
	request.ctx = ctx
	request.reply = make(chan workspaceControlReply, 1)
	select {
	case s.requests <- request:
	case <-ctx.Done():
		s.releaseBusy()
		return workspaceControlReply{}, false
	case <-s.ctx.Done():
		s.releaseBusy()
		return workspaceControlReply{}, false
	}
	select {
	case reply := <-request.reply:
		return reply, true
	case <-ctx.Done():
		return workspaceControlReply{}, false
	case <-s.ctx.Done():
		return workspaceControlReply{}, false
	}
}

func (s *workspaceControlServer) releaseBusy() {
	s.mu.Lock()
	s.busy = false
	s.mu.Unlock()
}

func (s *workspaceControlServer) finish(request workspaceControlRequest, reply workspaceControlReply) {
	s.releaseBusy()
	select {
	case request.reply <- reply:
	default:
	}
}

func (s *workspaceControlServer) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.beginClose()
	err := s.server.Close()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	select {
	case <-s.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	handlers := make(chan struct{})
	go func() {
		s.handlers.Wait()
		close(handlers)
	}()
	select {
	case <-handlers:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err = os.Remove(s.socket); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (s *workspaceControlServer) beginClose() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.closing = true
	s.mu.Unlock()
	s.cancel()
	_ = s.server.Close()
}

func (s *Supervisor) workspaceIdentity() gatewaycontrol.Identity {
	return gatewaycontrol.Identity{
		StorageVolumeID: s.Spec.StorageVolumeID,
		FormatUUID:      s.Spec.FormatUUID,
		Generation:      int64(s.Spec.Generation),
		FenceEpoch:      s.Spec.FenceEpoch,
	}
}

func (s *Supervisor) startWorkspaceControl(ctx context.Context) error {
	if !s.WorkspaceGateway {
		return nil
	}
	control, err := newWorkspaceControlServer(ctx, s.Paths.WorkspaceControlPath(), s.workspaceIdentity(), s.workspaceControlActive)
	if err != nil {
		return err
	}
	s.control = control
	return nil
}

func (s *Supervisor) stopWorkspaceControl(ctx context.Context) error {
	control := s.control
	s.control = nil
	return control.Close(ctx)
}

func (s *Supervisor) cancelWorkspaceControl() {
	if s.control != nil {
		s.control.beginClose()
	}
}

func (s *Supervisor) workspaceControlRequests() <-chan workspaceControlRequest {
	if s.control == nil {
		return nil
	}
	return s.control.requests
}

func (s *Supervisor) workspaceControlActive() bool {
	if s.deadline.RemainingLease(s.now()) <= 0 {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.fenced
}

func workspaceControlRequestContext(parent, request context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	if request.Err() != nil {
		cancel()
		return ctx, cancel
	}
	stop := context.AfterFunc(request, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}

func (s *Supervisor) runWorkspaceControlRequest(parent context.Context, request workspaceControlRequest) workspaceControlReply {
	ctx, cancel := workspaceControlRequestContext(parent, request.ctx)
	defer cancel()
	if !s.workspaceControlRequestMatches(request) {
		return workspaceControlReply{err: errors.New("workspace control identity does not match the mounted writer")}
	}
	if !s.workspaceControlActive() {
		return workspaceControlReply{err: errors.New("writer lease expired")}
	}
	if request.barrier != nil {
		result, err := s.runPayloadBarrier(ctx)
		if err != nil {
			return workspaceControlReply{err: err}
		}
		if result.LastSuccessfulFence < result.Fence || result.LastSuccessfulBarrierUnixMs <= 0 {
			return workspaceControlReply{err: errors.New("durability barrier returned incomplete evidence")}
		}
		if ctx.Err() != nil || !s.workspaceControlActive() {
			if ctx.Err() == nil {
				return workspaceControlReply{err: errors.New("writer lease expired")}
			}
			return workspaceControlReply{err: ctx.Err()}
		}
		return workspaceControlReply{barrier: &gatewaycontrol.BarrierResponse{
			Identity:                    s.workspaceIdentity(),
			Fence:                       ptrUint64(result.Fence),
			LastSuccessfulFence:         ptrUint64(result.LastSuccessfulFence),
			LastSuccessfulBarrierUnixMs: result.LastSuccessfulBarrierUnixMs,
		}}
	}
	if request.clone != nil {
		cloner, ok := s.vol.(WorkspaceCloner)
		if !ok {
			return workspaceControlReply{err: errors.New("workspace clone is unavailable")}
		}
		leaseCtx, leaseCancel, err := s.workspaceControlLeaseContext(ctx)
		if err != nil {
			return workspaceControlReply{err: err}
		}
		defer leaseCancel()
		if err = cloner.CloneTree(leaseCtx, *request.clone); err != nil || leaseCtx.Err() != nil || !s.workspaceControlActive() {
			if err == nil {
				if leaseCtx.Err() != nil {
					err = leaseCtx.Err()
				} else {
					err = errors.New("writer lease expired")
				}
			}
			return workspaceControlReply{err: err}
		}
		return workspaceControlReply{clone: &gatewaycontrol.CloneResponse{Identity: s.workspaceIdentity(), Cloned: true}}
	}
	return workspaceControlReply{err: errors.New("unknown workspace control request")}
}

func (s *Supervisor) workspaceControlRequestMatches(request workspaceControlRequest) bool {
	identity := s.workspaceIdentity()
	switch {
	case request.barrier != nil:
		return request.barrier.Identity == identity
	case request.clone != nil:
		return request.clone.Identity == identity
	default:
		return false
	}
}

func ptrUint64(value uint64) *uint64 { return &value }

func (s *Supervisor) runPayloadBarrier(ctx context.Context) (BarrierResult, error) {
	leaseCtx, cancel, err := s.workspaceControlLeaseContext(ctx)
	if err != nil {
		return BarrierResult{}, err
	}
	defer cancel()
	result, err := s.vol.Barrier(leaseCtx)
	if err != nil {
		return BarrierResult{}, err
	}
	if err = leaseCtx.Err(); err != nil {
		return BarrierResult{}, err
	}
	return result, nil
}

func (s *Supervisor) workspaceControlLeaseContext(ctx context.Context) (context.Context, context.CancelFunc, error) {
	budget := s.deadline.RemainingLease(s.now())
	if budget <= 0 {
		return nil, nil, errors.New("writer lease expired")
	}
	leaseCtx, cancel := context.WithTimeout(ctx, budget)
	if err := leaseCtx.Err(); err != nil {
		cancel()
		return nil, nil, err
	}
	return leaseCtx, cancel, nil
}
