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
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// The writer's control telemetry (PLO-805). The families are served only on
// the writer's private metrics socket, on the bare registry, so the names are
// exact and the only labels are the finite ones below. No volume, path, PID,
// socket or error text ever becomes a label.
const (
	ControlCallsMetric               = "juicefs_plori_control_calls_total"
	ControlDurationMetric            = "juicefs_plori_control_duration_seconds_total"
	WorkspaceOperationsMetric        = "juicefs_plori_workspace_operations_total"
	WorkspaceOperationDurationMetric = "juicefs_plori_workspace_operation_duration_seconds_total"
	// ControlMetricsReadyMetric is registered only after every series above is,
	// so a scrape without it has no control telemetry at all, never zeros.
	ControlMetricsReadyMetric = "juicefs_plori_control_metrics_ready"
)

// controlRouteSync is the one control route the writer calls on its own
// Litestream child: `/sync`, waiting (barrier position, final sync, credential
// reload) or not (probe).
const controlRouteSync = "sync"

// controlOutcome is how one control call or workspace operation ended. It is
// derived from typed errors and the call's own context, never from text.
type controlOutcome int

const (
	outcomeOK controlOutcome = iota
	outcomeDeadline
	outcomeCanceled
	outcomeRefused
	outcomeOther
	controlOutcomes
)

func (o controlOutcome) String() string {
	switch o {
	case outcomeOK:
		return "ok"
	case outcomeDeadline:
		return "deadline"
	case outcomeCanceled:
		return "canceled"
	case outcomeRefused:
		return "refused"
	default:
		return "other"
	}
}

// workspaceOperation is a private gateway operation the writer executes on its
// serialized barrier lane.
type workspaceOperation int

const (
	workspaceBarrier workspaceOperation = iota
	workspaceClone
	workspaceOperations
)

func (o workspaceOperation) String() string {
	if o == workspaceClone {
		return "clone"
	}
	return "barrier"
}

// ControlMetrics counts one writer's Litestream control calls and executed
// private workspace operations. Every writer owns its own and registers it on
// its own registry: nothing here is process-wide, so two Supervisors can never
// add into each other's series. A nil *ControlMetrics records nothing, which is
// what the node-level replicator and every non-gateway mount use.
//
// A count and its elapsed total move under one lock and are read under it, so
// a scrape never sees a call counted without its time.
type ControlMetrics struct {
	calls, duration, operations, operationDuration *prometheus.Desc

	mu               sync.Mutex
	syncCalls        [controlOutcomes]uint64
	syncSeconds      float64
	operationCalls   [workspaceOperations][controlOutcomes]uint64
	operationSeconds [workspaceOperations]float64
}

func NewControlMetrics() *ControlMetrics {
	return &ControlMetrics{
		calls: prometheus.NewDesc(ControlCallsMetric,
			"Completed Litestream control calls this writer made, by route and outcome.",
			[]string{"route", "outcome"}, nil),
		duration: prometheus.NewDesc(ControlDurationMetric,
			"Total elapsed seconds of this writer's Litestream control calls, dial to last body byte.",
			[]string{"route"}, nil),
		operations: prometheus.NewDesc(WorkspaceOperationsMetric,
			"Private workspace operations this writer executed on its serialized lane, by operation and outcome.",
			[]string{"operation", "outcome"}, nil),
		operationDuration: prometheus.NewDesc(WorkspaceOperationDurationMetric,
			"Total elapsed seconds of this writer's executed private workspace operations, excluding queue wait.",
			[]string{"operation"}, nil),
	}
}

// Register exposes every finite series on registry, each at its current value,
// and only then the ready sentinel. On error the sentinel is absent.
func (m *ControlMetrics) Register(registry prometheus.Registerer) error {
	if err := registry.Register(m); err != nil {
		return err
	}
	ready := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: ControlMetricsReadyMetric,
		Help: "1 once every control telemetry series of this writer is registered.",
	}, func() float64 { return 1 })
	if err := registry.Register(ready); err != nil {
		registry.Unregister(m)
		return err
	}
	return nil
}

func (m *ControlMetrics) Describe(ch chan<- *prometheus.Desc) {
	ch <- m.calls
	ch <- m.duration
	ch <- m.operations
	ch <- m.operationDuration
}

func (m *ControlMetrics) Collect(ch chan<- prometheus.Metric) {
	m.mu.Lock()
	syncCalls, syncSeconds := m.syncCalls, m.syncSeconds
	operationCalls, operationSeconds := m.operationCalls, m.operationSeconds
	m.mu.Unlock()
	for o := outcomeOK; o < controlOutcomes; o++ {
		ch <- prometheus.MustNewConstMetric(m.calls, prometheus.CounterValue, float64(syncCalls[o]), controlRouteSync, o.String())
	}
	ch <- prometheus.MustNewConstMetric(m.duration, prometheus.CounterValue, syncSeconds, controlRouteSync)
	for op := workspaceBarrier; op < workspaceOperations; op++ {
		for o := outcomeOK; o < controlOutcomes; o++ {
			ch <- prometheus.MustNewConstMetric(m.operations, prometheus.CounterValue, float64(operationCalls[op][o]), op.String(), o.String())
		}
		ch <- prometheus.MustNewConstMetric(m.operationDuration, prometheus.CounterValue, operationSeconds[op], op.String())
	}
}

func (m *ControlMetrics) observeSync(outcome controlOutcome, elapsed time.Duration) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.syncCalls[boundedOutcome(outcome)]++
	m.syncSeconds += elapsedSeconds(elapsed)
}

func (m *ControlMetrics) observeWorkspace(op workspaceOperation, outcome controlOutcome, elapsed time.Duration) {
	if m == nil || op < 0 || op >= workspaceOperations {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.operationCalls[op][boundedOutcome(outcome)]++
	m.operationSeconds[op] += elapsedSeconds(elapsed)
}

func boundedOutcome(o controlOutcome) controlOutcome {
	if o < outcomeOK || o >= controlOutcomes {
		return outcomeOther
	}
	return o
}

// elapsedSeconds is a monotonic interval, so it cannot be negative; the guard
// keeps a counter from ever being handed one.
func elapsedSeconds(d time.Duration) float64 {
	if d < 0 {
		return 0
	}
	return d.Seconds()
}

// controlCallOutcome classifies a control call that failed without a status.
// A deadline and a cancellation are told apart by the typed error the call
// returned or, when the transport reports the connection it tore down instead,
// by the state of the call's own bounded context, which is what tore it down.
func controlCallOutcome(bound context.Context, err error) controlOutcome {
	if o, ok := contextOutcome(err); ok {
		return o
	}
	if o, ok := contextOutcome(bound.Err()); ok {
		return o
	}
	return outcomeOther
}

func contextOutcome(err error) (controlOutcome, bool) {
	switch {
	case err == nil:
		return outcomeOK, false
	case errors.Is(err, context.DeadlineExceeded):
		return outcomeDeadline, true
	case errors.Is(err, context.Canceled):
		return outcomeCanceled, true
	}
	return outcomeOther, false
}

// workspaceOperationOutcome classifies an executed workspace operation's
// reply error. A refusal is only a typed WorkspaceControlRefusal: failures of
// the filesystem's own state, quota included, are other.
func workspaceOperationOutcome(err error) controlOutcome {
	if err == nil {
		return outcomeOK
	}
	if o, ok := contextOutcome(err); ok {
		return o
	}
	var refusal WorkspaceControlRefusal
	if errors.As(err, &refusal) {
		return outcomeRefused
	}
	return outcomeOther
}
