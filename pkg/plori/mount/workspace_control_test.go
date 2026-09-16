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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/plori/gatewaycontrol"
)

func workspaceControlIdentity() gatewaycontrol.Identity {
	return gatewaycontrol.Identity{StorageVolumeID: "vol-1", FormatUUID: "12345678-1234-4234-8234-123456789abc", Generation: 3, FenceEpoch: 7}
}

func workspaceControlClient(socket string) *http.Client {
	return &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		},
		DisableKeepAlives: true,
	}}
}

func workspaceControlRootSocket(t *testing.T) string {
	t.Helper()
	state := t.TempDir()
	if err := os.Chmod(state, 0o700); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(state, "workspace-control.sock")
}

func TestWorkspaceControlRootRefusesSymlinkStateDir(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("requires root-owned temporary state directory; run the compiled test binary with sudo -n")
	}
	target := t.TempDir()
	if err := os.Chmod(target, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "state-link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if err := verifyWorkspaceControlStateDir(link); err == nil {
		t.Fatal("accepted a symlinked workspace control state directory")
	}
}

func TestWorkspaceControlRefusesStaleNonSocket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "workspace-control.sock")
	if err := os.WriteFile(path, []byte("not a socket"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := removeWorkspaceControlSocket(path); err == nil {
		t.Fatal("removed a non-socket stale control path")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("stale regular file was removed: %v", err)
	}
}

func TestOrdinaryMountDoesNotCreateWorkspaceControlSocket(t *testing.T) {
	state := t.TempDir()
	supervisor := &Supervisor{Paths: Paths{StateDir: state}}
	if err := supervisor.startWorkspaceControl(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(state, "workspace-control.sock")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ordinary mount created workspace control socket: %v", err)
	}
}

func TestWorkspaceControlRootPeerStrictBarrierWire(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("requires root peer credentials; run the compiled test binary with sudo -n")
	}
	socket := workspaceControlRootSocket(t)
	identity := workspaceControlIdentity()
	server, err := newWorkspaceControlServer(context.Background(), socket, identity, func() bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close(context.Background()) })
	if info, err := os.Lstat(socket); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("socket mode = %v, want 0600 (err=%v)", info.Mode(), err)
	}
	go func() {
		request := <-server.requests
		if request.barrier == nil {
			server.finish(request, workspaceControlReply{err: errors.New("not a barrier")})
			return
		}
		server.finish(request, workspaceControlReply{barrier: &gatewaycontrol.BarrierResponse{
			Identity:                    identity,
			Fence:                       ptrUint64(11),
			LastSuccessfulFence:         ptrUint64(11),
			LastSuccessfulBarrierUnixMs: time.Now().UnixMilli(),
		}})
	}()
	body, err := json.Marshal(gatewaycontrol.BarrierRequest{Identity: identity})
	if err != nil {
		t.Fatal(err)
	}
	response, err := workspaceControlClient(socket).Post("http://workspace"+gatewaycontrol.BarrierRoute, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", response.StatusCode)
	}
	var got gatewaycontrol.BarrierResponse
	if err = json.NewDecoder(response.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.Identity != identity || got.Fence == nil || got.LastSuccessfulFence == nil || *got.Fence != 11 || *got.LastSuccessfulFence != 11 || got.LastSuccessfulBarrierUnixMs == 0 {
		t.Fatalf("response = %+v", got)
	}

	response, err = workspaceControlClient(socket).Post("http://workspace"+gatewaycontrol.BarrierRoute, "application/json", bytes.NewBufferString(`{"storage_volume_id":"vol-1","format_uuid":"12345678-1234-4234-8234-123456789abc","generation":3,"fence_epoch":7,"extra":true}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("unknown field status = %d", response.StatusCode)
	}
}

func TestWorkspaceControlRootCloseCancelsPendingRequest(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("requires root peer credentials; run the compiled test binary with sudo -n")
	}
	socket := workspaceControlRootSocket(t)
	identity := workspaceControlIdentity()
	server, err := newWorkspaceControlServer(context.Background(), socket, identity, func() bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(gatewaycontrol.BarrierRequest{Identity: identity})
	if err != nil {
		t.Fatal(err)
	}
	type clientResult struct {
		status int
		err    error
	}
	result := make(chan clientResult, 1)
	go func() {
		response, err := workspaceControlClient(socket).Post("http://workspace"+gatewaycontrol.BarrierRoute, "application/json", bytes.NewReader(body))
		status := 0
		if response != nil {
			status = response.StatusCode
			_ = response.Body.Close()
		}
		result <- clientResult{status: status, err: err}
	}()
	select {
	case <-server.requests:
	case <-time.After(time.Second):
		t.Fatal("request did not reach the scheduler")
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-result:
		if got.err == nil && got.status == http.StatusOK {
			t.Fatal("pending request received a successful response after close")
		}
	case <-time.After(time.Second):
		t.Fatal("pending request did not leave after control shutdown")
	}
}

func TestWorkspaceControlRootSlowWorkKeepsReplyWritable(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("requires root peer credentials; run the compiled test binary with sudo -n")
	}
	socket := workspaceControlRootSocket(t)
	identity := workspaceControlIdentity()
	server, err := newWorkspaceControlServer(context.Background(), socket, identity, func() bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close(context.Background()) })
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		select {
		case request := <-server.requests:
			timer := time.NewTimer(workspaceControlWriteTimeout + 100*time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
				server.finish(request, workspaceControlReply{barrier: &gatewaycontrol.BarrierResponse{
					Identity: identity, Fence: ptrUint64(0), LastSuccessfulFence: ptrUint64(0),
					LastSuccessfulBarrierUnixMs: time.Now().UnixMilli(),
				}})
			case <-server.ctx.Done():
			}
		case <-server.ctx.Done():
		}
	}()
	defer func() { _ = server.Close(context.Background()); <-finished }()
	body, err := json.Marshal(gatewaycontrol.BarrierRequest{Identity: identity})
	if err != nil {
		t.Fatal(err)
	}
	client := workspaceControlClient(socket)
	client.Timeout = 2 * workspaceControlWriteTimeout
	response, err := client.Post("http://workspace"+gatewaycontrol.BarrierRoute, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("native work consumed the reply write deadline: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("slow operation status = %d", response.StatusCode)
	}
	var reply gatewaycontrol.BarrierResponse
	if err := json.NewDecoder(response.Body).Decode(&reply); err != nil || reply.Identity != identity {
		t.Fatalf("slow operation reply = %+v, err=%v", reply, err)
	}
}

type workspaceControlVolume struct {
	*fakeVolume
	clone func(context.Context, gatewaycontrol.CloneRequest) error
}

func (v *workspaceControlVolume) CloneTree(ctx context.Context, request gatewaycontrol.CloneRequest) error {
	v.record("clone")
	if v.clone != nil {
		return v.clone(ctx, request)
	}
	return nil
}

func newWorkspaceControlSupervisor(t *testing.T, volume Volume) *Supervisor {
	t.Helper()
	spec := testSpec()
	spec.LeaseExpiresAt = time.Now().UTC().Add(time.Minute)
	sup := newSup(t, spec, &fakeFS{vol: healthyVolume()}, &fakeCP{}, &fakeReplicator{}, &fakeFencer{})
	sup.vol = volume
	sup.deadline = NewDeadline(spec.LeaseExpiresAt, spec.WriteStopMargin.D(), time.Now())
	return sup
}

func TestWorkspaceControlRejectsMismatchedOrExpiredAuthority(t *testing.T) {
	volume := healthyVolume()
	sup := newWorkspaceControlSupervisor(t, volume)
	request := workspaceControlRequest{ctx: context.Background(), barrier: &gatewaycontrol.BarrierRequest{Identity: sup.workspaceIdentity()}}
	request.barrier.FenceEpoch++
	if reply := sup.runWorkspaceControlRequest(context.Background(), request); reply.err == nil {
		t.Fatal("accepted a mismatched writer identity")
	}
	if got := countCalls(volume.order(), "barrier"); got != 0 {
		t.Fatalf("mismatched identity ran %d barriers", got)
	}
	sup.deadline = NewDeadline(time.Now().UTC().Add(-time.Second), sup.Spec.WriteStopMargin.D(), time.Now())
	request.barrier.Identity = sup.workspaceIdentity()
	if reply := sup.runWorkspaceControlRequest(context.Background(), request); reply.err == nil {
		t.Fatal("accepted an expired writer lease")
	}
	if got := countCalls(volume.order(), "barrier"); got != 0 {
		t.Fatalf("expired authority ran %d barriers", got)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	sup.deadline = NewDeadline(time.Now().UTC().Add(time.Minute), sup.Spec.WriteStopMargin.D(), time.Now())
	request.ctx = cancelled
	if reply := sup.runWorkspaceControlRequest(context.Background(), request); reply.err == nil {
		t.Fatal("accepted a pre-cancelled request")
	}
	if got := countCalls(volume.order(), "barrier"); got != 0 {
		t.Fatalf("pre-cancelled request ran %d barriers", got)
	}
}

func TestWorkspaceControlAcceptsZeroFenceForEmptyVolume(t *testing.T) {
	volume := healthyVolume()
	volume.barrier = func(context.Context) (BarrierResult, error) {
		return BarrierResult{LastSuccessfulBarrierUnixMs: time.Now().UnixMilli()}, nil
	}
	sup := newWorkspaceControlSupervisor(t, volume)
	reply := sup.runWorkspaceControlRequest(context.Background(), workspaceControlRequest{
		ctx: context.Background(), barrier: &gatewaycontrol.BarrierRequest{Identity: sup.workspaceIdentity()},
	})
	if reply.err != nil || reply.barrier == nil || reply.barrier.Fence == nil || reply.barrier.LastSuccessfulFence == nil || *reply.barrier.Fence != 0 || *reply.barrier.LastSuccessfulFence != 0 {
		t.Fatalf("empty-volume barrier reply = %+v, err = %v", reply.barrier, reply.err)
	}
}

func TestWorkspaceControlRefusesLateBarrierSuccess(t *testing.T) {
	started := make(chan struct{})
	volume := healthyVolume()
	var sup *Supervisor
	volume.barrier = func(ctx context.Context) (BarrierResult, error) {
		close(started)
		<-ctx.Done()
		// The mount can have renewed while this request kept its original
		// deadline. A fresh lease must not make the expired call successful.
		now := time.Now()
		sup.deadline.Update(now.Add(time.Minute), 0, now)
		return BarrierResult{LastSuccessfulBarrierUnixMs: time.Now().UnixMilli()}, nil
	}
	sup = newWorkspaceControlSupervisor(t, volume)
	sup.Spec.LeaseExpiresAt = time.Now().UTC().Add(20 * time.Millisecond)
	sup.deadline = NewDeadline(sup.Spec.LeaseExpiresAt, 0, time.Now())
	result := make(chan workspaceControlReply, 1)
	go func() {
		result <- sup.runWorkspaceControlRequest(context.Background(), workspaceControlRequest{
			ctx: context.Background(), barrier: &gatewaycontrol.BarrierRequest{Identity: sup.workspaceIdentity()},
		})
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("barrier did not start")
	}
	select {
	case reply := <-result:
		if reply.err == nil || reply.barrier != nil {
			t.Fatalf("late barrier acknowledged success: %+v", reply)
		}
	case <-time.After(time.Second):
		t.Fatal("late barrier did not stop at its lease bound")
	}
}

func TestWorkspaceControlSerializesPeriodicBarrierCloneAndBarrier(t *testing.T) {
	firstBarrierStarted := make(chan struct{})
	releaseFirstBarrier := make(chan struct{})
	barrierCalls := 0
	volume := &workspaceControlVolume{fakeVolume: healthyVolume()}
	volume.barrier = func(context.Context) (BarrierResult, error) {
		barrierCalls++
		if barrierCalls == 1 {
			close(firstBarrierStarted)
			<-releaseFirstBarrier
		}
		return BarrierResult{LastSuccessfulBarrierUnixMs: time.Now().UnixMilli()}, nil
	}
	sup := newWorkspaceControlSupervisor(t, volume)
	w := sup.startWorkers(context.Background())
	defer sup.stopWorkers(context.Background(), false)
	w.wantBarrier = true
	w.pump()
	select {
	case <-firstBarrierStarted:
	case <-time.After(time.Second):
		t.Fatal("periodic barrier did not start")
	}
	cloneRequest := workspaceControlRequest{ctx: context.Background(), clone: &gatewaycontrol.CloneRequest{Identity: sup.workspaceIdentity()}}
	w.workspaceControl = &cloneRequest
	w.pump()
	if got := countCalls(volume.order(), "clone"); got != 0 {
		t.Fatalf("clone ran beside periodic barrier: %v", volume.order())
	}
	close(releaseFirstBarrier)
	<-w.barrierDone
	w.barriering = false
	w.pump()
	<-w.barrierDone
	w.barriering = false
	barrierRequest := workspaceControlRequest{ctx: context.Background(), barrier: &gatewaycontrol.BarrierRequest{Identity: sup.workspaceIdentity()}}
	w.workspaceControl = &barrierRequest
	w.pump()
	<-w.barrierDone
	order := volume.order()
	if len(order) != 3 || order[0] != "barrier" || order[1] != "clone" || order[2] != "barrier" {
		t.Fatalf("serialized order = %v", order)
	}
}

func TestWorkspaceControlUncooperativeJobBlocksTeardownButReturnsBoundedly(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	volume := &workspaceControlVolume{fakeVolume: healthyVolume()}
	volume.clone = func(context.Context, gatewaycontrol.CloneRequest) error {
		close(started)
		<-release
		return nil
	}
	sup := newWorkspaceControlSupervisor(t, volume)
	sup.Spec.LeaseExpiresAt = time.Now().UTC().Add(10 * time.Millisecond)
	sup.deadline = NewDeadline(sup.Spec.LeaseExpiresAt, 0, time.Now())
	w := sup.startWorkers(context.Background())
	request := workspaceControlRequest{ctx: context.Background(), clone: &gatewaycontrol.CloneRequest{Identity: sup.workspaceIdentity()}}
	w.workspaceControl = &request
	w.pump()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("clone did not start")
	}
	startedAt := time.Now()
	fatal := sup.shutdown(context.Background(), ReasonFencedOutOfBand)
	if elapsed := time.Since(startedAt); elapsed > 2*time.Second {
		t.Fatalf("shutdown exceeded its bounded join: %s", elapsed)
	}
	if fatal.Exit != CodeBarrierIncomplete || fatal.ErrCode != ErrCodeBarrierIncomplete {
		t.Fatalf("shutdown = %d/%s, want barrier incomplete", fatal.Exit, fatal.ErrCode)
	}
	for _, call := range volume.order() {
		if call == "detach" || call == "close" {
			t.Fatalf("unsafe teardown after uncooperative job: %v", volume.order())
		}
	}
	if countCalls(sup.Deps.CP.(*fakeCP).order(), "release") != 0 {
		t.Fatal("released lease after uncooperative job")
	}
	close(release)
	w.wg.Wait()
}
