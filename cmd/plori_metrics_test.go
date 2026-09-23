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
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestPrivateMetricsServesOnlyThroughStateSocket(t *testing.T) {
	dir := t.TempDir()
	socket := filepath.Join(dir, "metrics.sock")
	registry := prometheus.NewRegistry()
	metric := prometheus.NewGauge(prometheus.GaugeOpts{Name: "juicefs_private_test_metric"})
	metric.Set(7)
	registry.MustRegister(metric)
	s, err := startPrivateMetrics(socket, registry)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(socket)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("socket mode = %o, want 0600", info.Mode().Perm())
	}
	client := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}}
	resp, err := client.Get("http://metrics/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "juicefs_private_test_metric 7") {
		t.Fatalf("metrics response status=%d body=%s", resp.StatusCode, body)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(socket); !os.IsNotExist(err) {
		t.Fatalf("metrics socket remains after close: %v", err)
	}
}

// The Runtime collector matches this exact, unlabeled family name; a JuiceFS
// prefix or the mount's common labels would make it look missing.
func TestLitestreamMetricsChildGaugeIsExactAndUnlabeled(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "metrics.sock")
	registry := prometheus.NewRegistry()
	var child atomic.Uint64
	child.Store(3)
	registerLitestreamMetricsChild(registry, child.Load)
	s, err := startPrivateMetrics(socket, registry)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	client := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}}
	scrape := func() string {
		resp, err := client.Get("http://metrics/metrics")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return string(body)
	}
	if body := scrape(); !strings.Contains(body, "\njuicefs_plori_litestream_metrics_child 3\n") {
		t.Fatalf("gauge missing or labeled:\n%s", body)
	}
	child.Store(0)
	if body := scrape(); !strings.Contains(body, "\njuicefs_plori_litestream_metrics_child 0\n") {
		t.Fatalf("gauge is not read per scrape:\n%s", body)
	}
}
