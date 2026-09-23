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

package cmd

import (
	"errors"
	"net"
	"net/http"
	"os"

	"github.com/juicedata/juicefs/pkg/meta"
	pmount "github.com/juicedata/juicefs/pkg/plori/mount"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// registerLitestreamMetricsChild exposes pmount.LitestreamMetricsChildGauge on
// the bare registry, so the name is exact and carries no labels. It is an
// observation for the Runtime collector only; nothing in the writer's health or
// authority reads it.
func registerLitestreamMetricsChild(registry *prometheus.Registry, child func() uint64) {
	registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: pmount.LitestreamMetricsChildGauge,
		Help: "Start sequence of the supervised Litestream child while it alone holds the loopback metrics listener, else 0.",
	}, func() float64 { return float64(child()) }))
}

// DataSpaceDriftGauge is the slice_data recount drift of this mount's last
// open: the recount of the slice data minus the persisted counter row, in
// bytes (meta.PloriDataSpaceDrift). It is non-zero only when a reference path
// changed slice references without the counter, or after a restore.
const DataSpaceDriftGauge = "juicefs_plori_data_space_recount_drift_bytes"

// registerDataSpaceDrift exposes DataSpaceDriftGauge on the bare registry. It
// is registered only on slice_data mounts.
func registerDataSpaceDrift(registry *prometheus.Registry) {
	if registry == nil {
		return
	}
	err := registry.Register(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: DataSpaceDriftGauge,
		Help: "Slice data recount minus the persisted ploriDataSpace counter at the last slice_data open, in bytes.",
	}, func() float64 { return float64(meta.PloriDataSpaceDrift()) }))
	var already prometheus.AlreadyRegisteredError
	if err != nil && !errors.As(err, &already) {
		ploriLog("data_space_drift_unregistered", "error", err.Error())
	}
}

// registerControlMetrics exposes the writer's control telemetry on the bare
// registry before the private socket serves it. A failure leaves the ready
// sentinel absent, which the Runtime collector reads as no telemetry rather
// than zeros, and does not stop the mount.
func registerControlMetrics(registry *prometheus.Registry, m *pmount.ControlMetrics) {
	if err := m.Register(registry); err != nil {
		ploriLog("control_metrics_unregistered", "error", err.Error())
	}
}

// privateMetricsServer exposes this mount's registry through a state-dir
// socket. The root supervisor owns the 0700 state directory; mode 0600 keeps
// the Agent container out even though it shares the network namespace.
type privateMetricsServer struct {
	listener net.Listener
	server   *http.Server
	socket   string
	done     chan struct{}
}

func startPrivateMetrics(socket string, registry *prometheus.Registry) (*privateMetricsServer, error) {
	if err := os.Remove(socket); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	ln, err := net.Listen("unix", socket)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(socket, 0o600); err != nil {
		_ = ln.Close()
		_ = os.Remove(socket)
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{EnableOpenMetrics: true}))
	s := &privateMetricsServer{listener: ln, server: &http.Server{Handler: mux}, socket: socket, done: make(chan struct{})}
	go func() {
		_ = s.server.Serve(ln)
		close(s.done)
	}()
	return s, nil
}

func (s *privateMetricsServer) Close() error {
	if s == nil {
		return nil
	}
	err := s.server.Close()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	<-s.done
	if err := os.Remove(s.socket); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
