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

	pmount "github.com/juicedata/juicefs/pkg/plori/mount"
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

// The Runtime collector binds these exact lines on the writer's own socket: no
// prefix, no mount labels, every finite series at zero, then the sentinel. A
// writer built before them is the old fixture and exposes none, which the
// collector reads as unavailable rather than as zero.
func TestControlMetricsAreExactOnThePrivateSocket(t *testing.T) {
	dir := t.TempDir()
	scrape := func(name string, registry *prometheus.Registry) string {
		t.Helper()
		socket := filepath.Join(dir, name)
		s, err := startPrivateMetrics(socket, registry)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = s.Close() }()
		client := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		}}}
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
	controlLines := func(body string) []string {
		var lines []string
		for _, line := range strings.Split(body, "\n") {
			if strings.HasPrefix(line, "juicefs_plori_control_") || strings.HasPrefix(line, "juicefs_plori_workspace_") {
				lines = append(lines, line)
			}
		}
		return lines
	}

	old := prometheus.NewRegistry()
	registerLitestreamMetricsChild(old, func() uint64 { return 1 })
	if lines := controlLines(scrape("old.sock", old)); len(lines) != 0 {
		t.Fatalf("a writer without control telemetry exposed %v", lines)
	}

	current := prometheus.NewRegistry()
	registerLitestreamMetricsChild(current, func() uint64 { return 1 })
	registerControlMetrics(current, pmount.NewControlMetrics())
	var want []string
	for _, outcome := range []string{"ok", "deadline", "canceled", "refused", "other"} {
		want = append(want, `juicefs_plori_control_calls_total{outcome="`+outcome+`",route="sync"} 0`)
		for _, op := range []string{"barrier", "clone"} {
			want = append(want, `juicefs_plori_workspace_operations_total{operation="`+op+`",outcome="`+outcome+`"} 0`)
		}
	}
	want = append(want,
		`juicefs_plori_control_duration_seconds_total{route="sync"} 0`,
		`juicefs_plori_workspace_operation_duration_seconds_total{operation="barrier"} 0`,
		`juicefs_plori_workspace_operation_duration_seconds_total{operation="clone"} 0`,
		`juicefs_plori_control_metrics_ready 1`,
	)
	got := controlLines(scrape("current.sock", current))
	if len(got) != len(want) {
		t.Fatalf("control series = %v, want exactly %d", got, len(want))
	}
	present := map[string]bool{}
	for _, line := range got {
		present[line] = true
	}
	for _, line := range want {
		if !present[line] {
			t.Errorf("missing %s in %v", line, got)
		}
	}
}
