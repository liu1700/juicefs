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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

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
