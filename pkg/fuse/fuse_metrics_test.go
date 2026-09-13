package fuse

import (
	"testing"

	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/prometheus/client_golang/prometheus"
)

func TestForgetRecordsPrivateMetricsWithoutVFSState(t *testing.T) {
	registry := prometheus.NewRegistry()
	registerer := prometheus.WrapRegistererWithPrefix("juicefs_", registry)
	vfs.InitMetrics(registerer)

	fs := newFileSystem(nil, &vfs.VFS{})
	beforeForget := counterValue(t, registry, "juicefs_fuse_forget_total")
	beforeLookups := counterValue(t, registry, "juicefs_fuse_forget_nlookup_total")
	fs.Forget(42, 3)

	if got := counterValue(t, registry, "juicefs_fuse_forget_total"); got != beforeForget+1 {
		t.Fatalf("forget total = %v, want %v", got, beforeForget+1)
	}
	if got := counterValue(t, registry, "juicefs_fuse_forget_nlookup_total"); got != beforeLookups+3 {
		t.Fatalf("forget nlookup total = %v, want %v", got, beforeLookups+3)
	}
}

func counterValue(t *testing.T, registry *prometheus.Registry, name string) float64 {
	t.Helper()
	metrics, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, metric := range metrics {
		if metric.GetName() == name && len(metric.Metric) == 1 {
			return metric.Metric[0].GetCounter().GetValue()
		}
	}
	t.Fatalf("counter %q not found", name)
	return 0
}
