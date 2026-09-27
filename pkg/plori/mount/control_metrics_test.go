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
	"fmt"
	"io"
	"net"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/plori/gatewaycontrol"
	"github.com/prometheus/client_golang/prometheus"
)

var controlOutcomeLabels = []string{"ok", "deadline", "canceled", "refused", "other"}

func syncCallsKey(outcome string) string {
	return ControlCallsMetric + "{outcome=" + outcome + ",route=sync}"
}

func syncSecondsKey() string { return ControlDurationMetric + "{route=sync}" }

func operationKey(op, outcome string) string {
	return WorkspaceOperationsMetric + "{operation=" + op + ",outcome=" + outcome + "}"
}

func operationSecondsKey(op string) string {
	return WorkspaceOperationDurationMetric + "{operation=" + op + "}"
}

// gatherControlSeries reads a registry the way a scrape does and keys each
// control telemetry series by its name and sorted labels. A duplicated series
// fails the test: the consumer rejects one.
func gatherControlSeries(t *testing.T, g prometheus.Gatherer) map[string]float64 {
	t.Helper()
	families, err := g.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	series := map[string]float64{}
	for _, mf := range families {
		name := mf.GetName()
		if !strings.HasPrefix(name, "juicefs_plori_control_") && !strings.HasPrefix(name, "juicefs_plori_workspace_") {
			continue
		}
		for _, metric := range mf.GetMetric() {
			key := name
			if labels := metric.GetLabel(); len(labels) > 0 {
				parts := make([]string, 0, len(labels))
				for _, l := range labels {
					parts = append(parts, l.GetName()+"="+l.GetValue())
				}
				key += "{" + strings.Join(parts, ",") + "}"
			}
			var value float64
			switch {
			case metric.GetCounter() != nil:
				value = metric.GetCounter().GetValue()
			case metric.GetGauge() != nil:
				value = metric.GetGauge().GetValue()
			default:
				t.Fatalf("%s is neither a counter nor a gauge", key)
			}
			if _, dup := series[key]; dup {
				t.Fatalf("duplicate series %s", key)
			}
			series[key] = value
		}
	}
	return series
}

// controlSeries registers m on a fresh registry of its own and reads it.
func controlSeries(t *testing.T, m *ControlMetrics) map[string]float64 {
	t.Helper()
	registry := prometheus.NewRegistry()
	if err := m.Register(registry); err != nil {
		t.Fatalf("register: %v", err)
	}
	return gatherControlSeries(t, registry)
}

func wantSyncCalls(t *testing.T, series map[string]float64, want map[string]float64) {
	t.Helper()
	for _, outcome := range controlOutcomeLabels {
		if got := series[syncCallsKey(outcome)]; got != want[outcome] {
			t.Errorf("sync %s = %v, want %v", outcome, got, want[outcome])
		}
	}
}

func wantOperations(t *testing.T, series map[string]float64, op string, want map[string]float64) {
	t.Helper()
	for _, outcome := range controlOutcomeLabels {
		if got := series[operationKey(op, outcome)]; got != want[outcome] {
			t.Errorf("%s %s = %v, want %v", op, outcome, got, want[outcome])
		}
	}
}

// The Runtime collector binds 18 exact keys and a label-free sentinel. A writer
// built before this change emits none of them, and the collector must read that
// as unavailable; a writer built after emits every finite series at zero before
// its sentinel, so a zero it reads is a zero the writer counted.
func TestControlMetricsAppearWholeWithTheReadySentinel(t *testing.T) {
	old := prometheus.NewRegistry()
	old.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: LitestreamMetricsChildGauge}, func() float64 { return 1 }))
	if series := gatherControlSeries(t, old); len(series) != 0 {
		t.Fatalf("a writer without control telemetry exposed %v", series)
	}

	series := controlSeries(t, NewControlMetrics())
	want := map[string]float64{ControlMetricsReadyMetric: 1, syncSecondsKey(): 0}
	for _, outcome := range controlOutcomeLabels {
		want[syncCallsKey(outcome)] = 0
		for _, op := range []string{"barrier", "clone"} {
			want[operationKey(op, outcome)] = 0
		}
	}
	for _, op := range []string{"barrier", "clone"} {
		want[operationSecondsKey(op)] = 0
	}
	if len(want) != 19 {
		t.Fatalf("fixture has %d series, want 18 keys and the sentinel", len(want))
	}
	var extra []string
	for key := range series {
		if _, ok := want[key]; !ok {
			extra = append(extra, key)
		}
	}
	sort.Strings(extra)
	if len(extra) != 0 {
		t.Errorf("unexpected series %v", extra)
	}
	for key, value := range want {
		got, ok := series[key]
		if !ok || got != value {
			t.Errorf("%s = %v (present %t), want %v", key, got, ok, value)
		}
	}
}

// A registration that fails leaves the sentinel out, so the collector reads no
// telemetry rather than a family it cannot trust.
func TestControlMetricsWithoutEveryFamilyHasNoSentinel(t *testing.T) {
	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: ControlCallsMetric}, func() float64 { return 0 }))
	if err := NewControlMetrics().Register(registry); err == nil {
		t.Fatal("registered over a conflicting family")
	}
	if _, ok := gatherControlSeries(t, registry)[ControlMetricsReadyMetric]; ok {
		t.Fatal("ready sentinel exposed without every control family")
	}
}

// A nil collector is the node replicator's and every non-gateway mount's: it
// must record nothing and never fail the call it would have observed.
func TestNilControlMetricsRecordsNothing(t *testing.T) {
	var m *ControlMetrics
	m.observeSync(outcomeOK, time.Second)
	m.observeWorkspace(workspaceBarrier, outcomeOK, time.Second)
}

const controlSecretPayload = "plori-private-payload-7f3a"

// Every shape a per-mount `/sync` can end in is counted exactly once, under the
// outcome its typed error names, and its time is the whole call. The caller's
// error and the outcome agree: ok exactly when the caller saw success. No
// payload of the answer reaches the series.
func TestLitestreamSyncOutcomesAreTypedAndCountedOnce(t *testing.T) {
	const bound = 150 * time.Millisecond
	for _, tc := range []struct {
		name    string
		answer  func(net.Conn)
		wait    bool
		cancel  bool
		outcome string
		minTime time.Duration
	}{
		{
			name: "waiting sync answered whole",
			answer: func(conn net.Conn) {
				_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"status\":\"synced\",\"txid\":9,\"replicated_txid\":9}")
			},
			wait:    true,
			outcome: "ok",
		},
		{
			name: "probe answered whole",
			answer: func(conn net.Conn) {
				_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"status\":\"ok\"}")
			},
			outcome: "ok",
		},
		{
			name:    "no status line before the deadline",
			answer:  func(conn net.Conn) { _, _ = io.Copy(io.Discard, conn) },
			wait:    true,
			outcome: "deadline",
			minTime: bound / 2,
		},
		{
			name: "a body that stops arriving before the deadline",
			answer: func(conn net.Conn) {
				_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 96\r\n\r\n{\"status\":")
				_, _ = io.Copy(io.Discard, conn)
			},
			wait:    true,
			outcome: "deadline",
			minTime: bound / 2,
		},
		{
			name:    "caller cancels a stalled call",
			answer:  func(conn net.Conn) { _, _ = io.Copy(io.Discard, conn) },
			wait:    true,
			cancel:  true,
			outcome: "canceled",
		},
		{
			name: "a non-200 answer",
			answer: func(conn net.Conn) {
				_, _ = io.WriteString(conn, "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"error\":\""+controlSecretPayload+"\"}")
			},
			wait:    true,
			outcome: "refused",
		},
		{
			name: "a 200 that does not decode",
			answer: func(conn net.Conn) {
				_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"status\":\""+controlSecretPayload+"\",\"txid\":")
			},
			wait:    true,
			outcome: "other",
		},
		{
			name: "a 200 shorter than its content-length",
			answer: func(conn net.Conn) {
				_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 96\r\n\r\n{\"status\":\""+controlSecretPayload)
			},
			outcome: "other",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reached := make(chan struct{}, 1)
			sock := newRawControlSocket(t, func(conn net.Conn) {
				reached <- struct{}{}
				tc.answer(conn)
			})
			m := NewControlMetrics()
			ls := &Litestream{SocketPath: sock.path, DBPath: filepath.Join(t.TempDir(), "meta.db"), ControlMetrics: m}
			ctx, cancel := context.WithTimeout(context.Background(), bound)
			defer cancel()
			if tc.cancel {
				ctx, cancel = context.WithCancel(context.Background())
				defer cancel()
				go func() {
					<-reached
					cancel()
				}()
			}
			start := time.Now()
			_, err := ls.controlSync(ctx, tc.wait)
			elapsed := time.Since(start)
			if (err == nil) != (tc.outcome == "ok") {
				t.Fatalf("call error = %v, want success %t", err, tc.outcome == "ok")
			}
			switch tc.outcome {
			case "deadline":
				if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(ctx.Err(), context.DeadlineExceeded) {
					t.Errorf("error = %v, want the call's deadline", err)
				}
			case "canceled":
				if !errors.Is(err, context.Canceled) {
					t.Errorf("error = %v, want the caller's cancellation", err)
				}
			}
			registry := prometheus.NewRegistry()
			if err := m.Register(registry); err != nil {
				t.Fatal(err)
			}
			series := gatherControlSeries(t, registry)
			wantSyncCalls(t, series, map[string]float64{tc.outcome: 1})
			seconds := series[syncSecondsKey()]
			if seconds < tc.minTime.Seconds() || seconds > elapsed.Seconds() {
				t.Errorf("elapsed total = %vs, want within [%v, %v]", seconds, tc.minTime, elapsed)
			}
			families, err := registry.Gather()
			if err != nil {
				t.Fatal(err)
			}
			for _, mf := range families {
				if strings.Contains(mf.String(), controlSecretPayload) {
					t.Fatalf("the answer's payload reached %s", mf.GetName())
				}
			}
		})
	}
}

// The control call's own bound, not only the caller's deadline, is a deadline:
// a caller with no deadline still gets one typed outcome.
func TestTheControlCallBoundIsADeadlineOutcome(t *testing.T) {
	sock := newRawControlSocket(t, func(conn net.Conn) {
		_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 96\r\n\r\n{\"status\":")
		_, _ = io.Copy(io.Discard, conn)
	})
	_, outcome, err := litestreamControlCall(context.Background(), 150*time.Millisecond, sock.path, "/sync", map[string]any{"wait": true})
	if err == nil || outcome != outcomeDeadline {
		t.Fatalf("outcome = %s (err %v), want deadline", outcome, err)
	}
}

// Two writers never add into each other's series, and the node-level
// replicator, which has no collector, is attributed to neither.
func TestTwoWritersControlMetricsAreIsolated(t *testing.T) {
	okSock := newRawControlSocket(t, func(conn net.Conn) {
		_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"txid\":9,\"replicated_txid\":9}")
	})
	refusedSock := newRawControlSocket(t, func(conn net.Conn) {
		_, _ = io.WriteString(conn, "HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"error\":\"database not found\"}")
	})
	dir := t.TempDir()
	first := &Litestream{SocketPath: okSock.path, DBPath: filepath.Join(dir, "a.db"), ControlMetrics: NewControlMetrics()}
	second := &Litestream{SocketPath: refusedSock.path, DBPath: filepath.Join(dir, "b.db"), ControlMetrics: NewControlMetrics()}
	if txid, err := first.TxID(context.Background()); err != nil || txid != "0000000000000009" {
		t.Fatalf("first TxID = %q, %v", txid, err)
	}
	if err := second.SyncAndWait(context.Background()); err == nil {
		t.Fatal("second writer's refused sync succeeded")
	}
	node := &NodeReplicator{SocketPath: okSock.path, DBPath: filepath.Join(dir, "a.db")}
	if err := node.SyncAndWait(context.Background()); err != nil {
		t.Fatalf("node SyncAndWait: %v", err)
	}
	wantSyncCalls(t, controlSeries(t, first.ControlMetrics), map[string]float64{"ok": 1})
	wantSyncCalls(t, controlSeries(t, second.ControlMetrics), map[string]float64{"refused": 1})

	barrierVolume := healthyVolume()
	barrierVolume.barrier = func(context.Context) (BarrierResult, error) {
		return BarrierResult{LastSuccessfulBarrierUnixMs: time.Now().UnixMilli()}, nil
	}
	one := newWorkspaceControlSupervisor(t, barrierVolume)
	one.Deps.ControlMetrics = NewControlMetrics()
	other := newWorkspaceControlSupervisor(t, healthyVolume())
	other.Deps.ControlMetrics = NewControlMetrics()
	if reply := one.runWorkspaceControlRequest(context.Background(), workspaceControlRequest{
		ctx: context.Background(), barrier: &gatewaycontrol.BarrierRequest{Identity: one.workspaceIdentity()},
	}); reply.err != nil {
		t.Fatalf("barrier: %v", reply.err)
	}
	wantOperations(t, controlSeries(t, one.Deps.ControlMetrics), "barrier", map[string]float64{"ok": 1})
	wantOperations(t, controlSeries(t, other.Deps.ControlMetrics), "barrier", nil)
}

// Each executed private operation is counted once under its typed outcome: an
// authority refusal is refused, a lease that ran out under the work is
// deadline, a request its caller abandoned is canceled, and a failure of the
// filesystem's own state (quota included) is other. None is a false ok.
func TestWorkspaceOperationOutcomesAreTyped(t *testing.T) {
	succeed := func(context.Context) (BarrierResult, error) {
		return BarrierResult{LastSuccessfulBarrierUnixMs: time.Now().UnixMilli()}, nil
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range []struct {
		name    string
		op      string
		setup   func(*Supervisor, *workspaceControlVolume)
		request func(*Supervisor) workspaceControlRequest
		outcome string
	}{
		{
			name:    "barrier succeeds",
			op:      "barrier",
			setup:   func(_ *Supervisor, v *workspaceControlVolume) { v.barrier = succeed },
			outcome: "ok",
		},
		{
			name:  "barrier for another writer",
			op:    "barrier",
			setup: func(_ *Supervisor, v *workspaceControlVolume) { v.barrier = succeed },
			request: func(s *Supervisor) workspaceControlRequest {
				identity := s.workspaceIdentity()
				identity.FenceEpoch++
				return workspaceControlRequest{ctx: context.Background(), barrier: &gatewaycontrol.BarrierRequest{Identity: identity}}
			},
			outcome: "refused",
		},
		{
			name: "barrier after the lease",
			op:   "barrier",
			setup: func(s *Supervisor, v *workspaceControlVolume) {
				v.barrier = succeed
				s.deadline = NewDeadline(time.Now().UTC().Add(-time.Second), s.Spec.WriteStopMargin.D(), time.Now())
			},
			outcome: "refused",
		},
		{
			name: "barrier that outlives the lease",
			op:   "barrier",
			setup: func(s *Supervisor, v *workspaceControlVolume) {
				v.barrier = func(ctx context.Context) (BarrierResult, error) {
					<-ctx.Done()
					return BarrierResult{LastSuccessfulBarrierUnixMs: time.Now().UnixMilli()}, nil
				}
				s.deadline = NewDeadline(time.Now().UTC().Add(20*time.Millisecond), 0, time.Now())
			},
			outcome: "deadline",
		},
		{
			name:  "barrier its caller abandoned",
			op:    "barrier",
			setup: func(_ *Supervisor, v *workspaceControlVolume) { v.barrier = succeed },
			request: func(s *Supervisor) workspaceControlRequest {
				return workspaceControlRequest{ctx: cancelled, barrier: &gatewaycontrol.BarrierRequest{Identity: s.workspaceIdentity()}}
			},
			outcome: "canceled",
		},
		{
			name:    "barrier with incomplete evidence",
			op:      "barrier",
			setup:   func(*Supervisor, *workspaceControlVolume) {},
			outcome: "other",
		},
		{
			name:    "clone succeeds",
			op:      "clone",
			setup:   func(*Supervisor, *workspaceControlVolume) {},
			outcome: "ok",
		},
		{
			name: "clone outside the private trees",
			op:   "clone",
			setup: func(_ *Supervisor, v *workspaceControlVolume) {
				v.clone = func(context.Context, gatewaycontrol.CloneRequest) error {
					return WorkspaceControlRefusal("workspace clone: source is outside the allowed trees")
				}
			},
			outcome: "refused",
		},
		{
			name: "clone over quota",
			op:   "clone",
			setup: func(_ *Supervisor, v *workspaceControlVolume) {
				v.clone = func(context.Context, gatewaycontrol.CloneRequest) error {
					return fmt.Errorf("workspace clone: %w", syscall.ENOSPC)
				}
			},
			outcome: "other",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			volume := &workspaceControlVolume{fakeVolume: healthyVolume()}
			sup := newWorkspaceControlSupervisor(t, volume)
			sup.Deps.ControlMetrics = NewControlMetrics()
			tc.setup(sup, volume)
			request := workspaceControlRequest{ctx: context.Background(), barrier: &gatewaycontrol.BarrierRequest{Identity: sup.workspaceIdentity()}}
			if tc.op == "clone" {
				request = workspaceControlRequest{ctx: context.Background(), clone: &gatewaycontrol.CloneRequest{Identity: sup.workspaceIdentity()}}
			}
			if tc.request != nil {
				request = tc.request(sup)
			}
			reply := sup.runWorkspaceControlRequest(context.Background(), request)
			if (reply.err == nil) != (tc.outcome == "ok") {
				t.Fatalf("reply error = %v, want success %t", reply.err, tc.outcome == "ok")
			}
			series := controlSeries(t, sup.Deps.ControlMetrics)
			wantOperations(t, series, tc.op, map[string]float64{tc.outcome: 1})
			idle := "clone"
			if tc.op == "clone" {
				idle = "barrier"
			}
			wantOperations(t, series, idle, nil)
			wantSyncCalls(t, series, nil)
		})
	}
}

// The timed interval is the execution on the serialized lane, not the queue
// wait behind a periodic barrier, and the periodic barrier itself is not a
// private operation.
func TestWorkspaceOperationTimeExcludesQueueWait(t *testing.T) {
	const hold = 200 * time.Millisecond
	started := make(chan struct{})
	calls := 0
	volume := &workspaceControlVolume{fakeVolume: healthyVolume()}
	volume.barrier = func(context.Context) (BarrierResult, error) {
		calls++
		if calls == 1 {
			close(started)
			time.Sleep(hold)
		}
		return BarrierResult{LastSuccessfulBarrierUnixMs: time.Now().UnixMilli()}, nil
	}
	sup := newWorkspaceControlSupervisor(t, volume)
	sup.Deps.ControlMetrics = NewControlMetrics()
	w := sup.startWorkers(context.Background())
	defer sup.stopWorkers(context.Background(), false)
	w.wantBarrier = true
	w.pump()
	<-started
	queued := time.Now()
	request := workspaceControlRequest{ctx: context.Background(), clone: &gatewaycontrol.CloneRequest{Identity: sup.workspaceIdentity()}}
	w.workspaceControl = &request
	<-w.barrierDone
	w.barriering = false
	w.pump()
	observed := <-w.barrierDone
	w.barriering = false
	if observed.reply.err != nil {
		t.Fatalf("clone: %v", observed.reply.err)
	}
	waited := time.Since(queued)
	series := controlSeries(t, sup.Deps.ControlMetrics)
	wantOperations(t, series, "clone", map[string]float64{"ok": 1})
	wantOperations(t, series, "barrier", nil)
	if seconds := series[operationSecondsKey("clone")]; seconds >= waited.Seconds() || seconds >= (hold/2).Seconds() {
		t.Fatalf("clone time = %vs includes the %s it waited in the queue", seconds, waited)
	}
}

// Work that never reaches the serialized lane executes nothing and is not
// counted: a request behind a busy lane, one refused at the socket, and one
// that names no operation. The writer keeps no reply cache, so a response the
// Runtime replays from its own cache never reaches it; a request that does
// arrive again is new work and is counted again.
func TestWorkspaceControlCountsOnlyExecutedWork(t *testing.T) {
	volume := &workspaceControlVolume{fakeVolume: healthyVolume()}
	sup := newWorkspaceControlSupervisor(t, volume)
	sup.Deps.ControlMetrics = NewControlMetrics()
	request := workspaceControlRequest{ctx: context.Background(), clone: &gatewaycontrol.CloneRequest{Identity: sup.workspaceIdentity()}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	busy := &workspaceControlServer{ctx: ctx, cancel: cancel, requests: make(chan workspaceControlRequest, 1), busy: true}
	if _, ok := busy.submit(context.Background(), request); ok {
		t.Fatal("a busy lane accepted a second request")
	}
	if reply := sup.runWorkspaceControlRequest(context.Background(), workspaceControlRequest{ctx: context.Background()}); reply.err == nil {
		t.Fatal("a request naming no operation succeeded")
	}
	series := controlSeries(t, sup.Deps.ControlMetrics)
	wantOperations(t, series, "clone", nil)
	wantOperations(t, series, "barrier", nil)
	if got := countCalls(volume.order(), "clone"); got != 0 {
		t.Fatalf("work that never reached the lane cloned %d times", got)
	}

	for i := 0; i < 2; i++ {
		if reply := sup.runWorkspaceControlRequest(context.Background(), request); reply.err != nil {
			t.Fatalf("clone %d: %v", i, reply.err)
		}
	}
	wantOperations(t, controlSeries(t, sup.Deps.ControlMetrics), "clone", map[string]float64{"ok": 2})
	if got := countCalls(volume.order(), "clone"); got != 2 {
		t.Fatalf("executed %d clones, counted 2", got)
	}
}
