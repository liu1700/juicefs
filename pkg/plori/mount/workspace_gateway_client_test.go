//go:build plori

package mount

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestGatewayWriterUsesOnlyItsAuthenticatedRoute(t *testing.T) {
	var got string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.URL.Path
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Error("missing authentication")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("test-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	client := NewClient(server.URL, token, time.Second)
	for _, route := range ClientRoutes() {
		client.WorkspaceGateway = false
		if err := client.post(context.Background(), route, struct{}{}, nil); err != nil {
			t.Fatal(err)
		}
		if got != route {
			t.Fatalf("legacy route changed: %s", got)
		}
		client.WorkspaceGateway = true
		if err := client.post(context.Background(), route, struct{}{}, nil); err != nil {
			t.Fatal(err)
		}
		want := "/v1/internal/workspace-storage/" + route[len("/v1/internal/storage/"):]
		if got != want {
			t.Fatalf("gateway used %s want %s", got, want)
		}
	}
}
