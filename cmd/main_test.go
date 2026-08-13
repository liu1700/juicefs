/*
 * JuiceFS, Copyright 2020 Juicedata, Inc.
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
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/urfave/cli/v2"
)

func TestDebugAgentDisabledByDefault(t *testing.T) {
	app := &cli.App{Flags: globalFlags()}
	var address string
	var disabled bool
	app.Action = func(c *cli.Context) error {
		address = c.String("debug-agent")
		disabled = c.Bool("no-agent")
		return nil
	}
	if err := app.Run([]string{"juicefs"}); err != nil {
		t.Fatal(err)
	}
	if address != "" || disabled {
		t.Fatalf("default debug agent flags: address=%q no-agent=%v", address, disabled)
	}
	if err := app.Run([]string{"juicefs", "--debug-agent=127.0.0.1:0"}); err != nil {
		t.Fatal(err)
	}
	if address != "127.0.0.1:0" {
		t.Fatalf("explicit debug agent address = %q", address)
	}
}

func TestDebugAgentServer(t *testing.T) {
	server, listener, err := newDebugAgentServer("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
		<-done
	})
	baseURL := "http://" + listener.Addr().String()
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(baseURL + "/debug/pprof/cmdline")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cmdline status = %d", resp.StatusCode)
	}
	resp, err = client.Get(baseURL + "/")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unexpected route status = %d", resp.StatusCode)
	}
	resp, err = client.Get(baseURL + "/debug/pprof/profile?seconds=121")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("oversized profile status = %d", resp.StatusCode)
	}
	if server.ReadHeaderTimeout == 0 || server.ReadTimeout == 0 || server.WriteTimeout == 0 || server.IdleTimeout == 0 {
		t.Fatal("debug server timeouts are not configured")
	}
}

func TestDebugAgentRejectsNonLoopback(t *testing.T) {
	if server, listener, err := newDebugAgentServer("0.0.0.0:0"); err == nil {
		_ = listener.Close()
		_ = server.Close()
		t.Fatal("debug agent accepted a non-loopback address")
	}
}

func TestDebugProfileConcurrencyLimit(t *testing.T) {
	sem := make(chan struct{}, 1)
	entered := make(chan struct{})
	release := make(chan struct{})
	handler := limitDebugProfile(sem, func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(http.StatusNoContent)
	})
	server := httptest.NewServer(handler)
	defer server.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		resp, err := http.Get(server.URL)
		if err == nil {
			_ = resp.Body.Close()
		}
	}()
	<-entered
	resp, err := http.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("concurrent profile status = %d", resp.StatusCode)
	}
	close(release)
	wg.Wait()
}

func TestDebugAgentMode(t *testing.T) {
	cases := []struct {
		command            string
		disabled, explicit bool
	}{
		{"juicefs mount redis:// /jfs", false, false},
		{"juicefs --debug-agent=127.0.0.1:6060 mount redis:// /jfs", false, true},
		{"juicefs --debug-agent 127.0.0.1:6060 mount redis:// /jfs", false, true},
		{"juicefs --no-agent mount redis:// /jfs", true, false},
	}
	for _, tc := range cases {
		disabled, explicit := debugAgentMode(tc.command)
		if disabled != tc.disabled || explicit != tc.explicit {
			t.Errorf("debugAgentMode(%q) = (%v, %v), want (%v, %v)", tc.command, disabled, explicit, tc.disabled, tc.explicit)
		}
	}
}

func TestArgsOrder(t *testing.T) {
	var app = &cli.App{
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:    "verbose",
				Aliases: []string{"v"},
			},
			&cli.Int64Flag{
				Name:    "key",
				Aliases: []string{"k"},
			},
		},
		Commands: []*cli.Command{
			{
				Name: "cmd",
				Flags: []cli.Flag{
					&cli.Int64Flag{
						Name: "k2",
					},
				},
			},
		},
	}

	var cases = [][]string{
		{"test", "cmd", "a", "-k2", "v2", "b", "--v"},
		{"test", "--v", "cmd", "-k2", "v2", "a", "b"},
		{"test", "cmd", "a", "-k2=v", "--h"},
		{"test", "cmd", "-k2=v", "--h", "a"},
	}
	for i := 0; i < len(cases); i += 2 {
		oreded := reorderOptions(app, cases[i])
		if !reflect.DeepEqual(cases[i+1], oreded) {
			t.Fatalf("expecte %v, but got %v", cases[i+1], oreded)
		}
	}
}

func TestHandleSysMountArgs(t *testing.T) {
	var cases = []struct {
		args    []string
		newArgs string
		fail    bool
	}{
		{
			[]string{"/mount.juicefs", "memkv://", "/jfs", "-o", "no-usage-report"},
			"juicefs mount -d --no-usage-report memkv:// /jfs",
			false,
		},
		{
			[]string{"/mount.juicefs", "memkv://", "/jfs", "-o", "no-usage-report=true"},
			"juicefs mount -d --no-usage-report=true memkv:// /jfs",
			false,
		},
		{
			[]string{"/mount.juicefs", "memkv://", "/jfs", "-o", "cache-size=204800"},
			"juicefs mount -d --cache-size=204800 memkv:// /jfs",
			false,
		},
		{
			[]string{"/mount.juicefs", "memkv://", "/jfs", "-o", "verbose"},
			"juicefs mount -d --verbose memkv:// /jfs",
			false,
		},
		{
			[]string{"/mount.juicefs", "memkv://", "/jfs", "-o", "debug"},
			"juicefs mount -d --debug -o debug memkv:// /jfs",
			false,
		},
		{
			[]string{"/mount.juicefs", "memkv://", "/jfs", "-o", "cache-size=204800,no-usage-report=false,free-space-ratio=0.5,cache-dir=/data/juicfs,metrics=0.0.0.0:9567"},
			"juicefs mount -d --cache-size=204800 --no-usage-report=false --free-space-ratio=0.5 --cache-dir=/data/juicfs --metrics=0.0.0.0:9567 memkv:// /jfs",
			false,
		},
		{
			[]string{"/mount.juicefs", "memkv://", "/jfs", "-o", "cache-size"},
			"",
			true,
		},
	}
	for _, c := range cases {
		rawNewArgs, err := handleSysMountArgs(c.args)
		if c.fail && err == nil {
			t.Fatalf("expect error, but got nil")
		}
		if !c.fail && err != nil {
			t.Fatalf("expect nil, but got %v", err)
		}
		newArgs := strings.Join(rawNewArgs, " ")
		if c.newArgs != newArgs {
			t.Fatalf("expect `%v`, but got `%v`", c.newArgs, newArgs)
		}
	}
}
