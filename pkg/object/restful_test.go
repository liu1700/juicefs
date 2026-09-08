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

package object

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

// startTCPListener starts a TCP listener on the given address and returns it.
// The listener accepts connections in background and immediately closes them.
func startTCPListener(t *testing.T, addr string) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("failed to listen on %s: %v", addr, err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
	return ln
}

func getPort(t *testing.T, ln net.Listener) string {
	t.Helper()
	_, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	return port
}

func TestDialParallel_OnlyPrimaries(t *testing.T) {
	ln := startTCPListener(t, "127.0.0.1:0")
	defer ln.Close()
	port := getPort(t, ln)

	dialer := &net.Dialer{Timeout: 2 * time.Second}
	conn, err := dialParallel(context.Background(), dialer, "tcp",
		[]net.IP{net.ParseIP("127.0.0.1")}, nil, port)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	conn.Close()
}

func TestDialParallel_OnlyFallbacks(t *testing.T) {
	// Bug reproduced: empty primaries should not panic
	ln := startTCPListener(t, "127.0.0.1:0")
	defer ln.Close()
	port := getPort(t, ln)

	dialer := &net.Dialer{Timeout: 2 * time.Second}
	conn, err := dialParallel(context.Background(), dialer, "tcp",
		nil, []net.IP{net.ParseIP("127.0.0.1")}, port)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	conn.Close()
}

func TestDialParallel_PrimaryFailsFast_FallbackSucceeds(t *testing.T) {
	// Primary (IPv6 ::1) has no listener → fails fast (connection refused)
	// Fallback (127.0.0.1) has a listener → succeeds
	ln := startTCPListener(t, "127.0.0.1:0")
	defer ln.Close()
	port := getPort(t, ln)

	dialer := &net.Dialer{Timeout: 2 * time.Second}
	conn, err := dialParallel(context.Background(), dialer, "tcp",
		[]net.IP{net.ParseIP("::1")},       // primary - will fail (no listener on ::1:port)
		[]net.IP{net.ParseIP("127.0.0.1")}, // fallback - has listener
		port)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	conn.Close()
}

func TestDialParallel_BothFail(t *testing.T) {
	dialer := &net.Dialer{Timeout: 500 * time.Millisecond}
	_, err := dialParallel(context.Background(), dialer, "tcp",
		[]net.IP{net.ParseIP("::1")},
		[]net.IP{net.ParseIP("127.0.0.1")},
		"0")
	if err == nil {
		t.Fatal("expected error when both groups fail, got nil")
	}
}

func TestSplitIPsByVersion(t *testing.T) {
	ips := []net.IP{
		net.ParseIP("127.0.0.1"),
		net.ParseIP("::1"),
		net.ParseIP("10.0.0.1"),
		net.ParseIP("fe80::1"),
	}
	v6, v4 := splitIPsByVersion(ips)
	if len(v6) != 2 {
		t.Errorf("expected 2 IPv6, got %d", len(v6))
	}
	if len(v4) != 2 {
		t.Errorf("expected 2 IPv4, got %d", len(v4))
	}
}

func TestDialFamilies_MixedAnswer_IPv6Disabled(t *testing.T) {
	ips := []net.IP{
		net.ParseIP("10.0.0.1"),
		net.ParseIP("2001:db8::1"),
		net.ParseIP("10.0.0.2"),
	}
	primaries, fallbacks := dialFamilies(ips, false)
	if len(primaries) != 2 {
		t.Errorf("expected 2 IPv4 primaries, got %d (%v)", len(primaries), primaries)
	}
	for _, ip := range primaries {
		if ip.To4() == nil {
			t.Errorf("expected only IPv4 primaries, got %v", ip)
		}
	}
	if len(fallbacks) != 0 {
		t.Errorf("expected no fallbacks when IPv6 is disabled, got %v", fallbacks)
	}
}

func TestDialFamilies_MixedAnswer_IPv6Enabled(t *testing.T) {
	ips := []net.IP{
		net.ParseIP("10.0.0.1"),
		net.ParseIP("2001:db8::1"),
		net.ParseIP("10.0.0.2"),
	}
	primaries, fallbacks := dialFamilies(ips, true)
	if len(primaries) != 1 {
		t.Errorf("expected 1 IPv6 primary, got %d (%v)", len(primaries), primaries)
	}
	for _, ip := range primaries {
		if ip.To4() != nil {
			t.Errorf("expected only IPv6 primaries, got %v", ip)
		}
	}
	if len(fallbacks) != 2 {
		t.Errorf("expected 2 IPv4 fallbacks, got %d (%v)", len(fallbacks), fallbacks)
	}
	for _, ip := range fallbacks {
		if ip.To4() == nil {
			t.Errorf("expected only IPv4 fallbacks, got %v", ip)
		}
	}
}

func TestDialFamilies_IPv6OnlyAnswer_Disabled(t *testing.T) {
	ips := []net.IP{
		net.ParseIP("2001:db8::1"),
		net.ParseIP("2001:db8::2"),
	}
	primaries, fallbacks := dialFamilies(ips, false)
	if len(primaries) != 0 {
		t.Errorf("expected no primaries for an IPv6-only answer with IPv6 disabled, got %v", primaries)
	}
	if len(fallbacks) != 0 {
		t.Errorf("expected no fallbacks for an IPv6-only answer with IPv6 disabled, got %v", fallbacks)
	}
}

func TestDialResolved_IPv6OnlyAnswer_Disabled_FailsFast(t *testing.T) {
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	start := time.Now()
	_, err := dialResolved(context.Background(), dialer, "tcp", "ipv6-only.test",
		[]net.IP{net.ParseIP("2001:db8::1")}, "443", false)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected an error for an IPv6-only answer with IPv6 disabled, got nil")
	}
	var dnsErr *net.DNSError
	if !errors.As(err, &dnsErr) {
		t.Fatalf("expected a *net.DNSError, got %T: %v", err, err)
	}
	if !dnsErr.IsNotFound {
		t.Errorf("expected IsNotFound to be true, got false (%v)", dnsErr)
	}
	if !strings.Contains(dnsErr.Err, "IPv4") {
		t.Errorf("expected the error to mention IPv4, got %q", dnsErr.Err)
	}
	if elapsed >= time.Second {
		t.Errorf("expected a fast failure without dialing, took %v", elapsed)
	}
}

func TestDialResolved_MixedAnswer_Disabled_DialsIPv4(t *testing.T) {
	ln := startTCPListener(t, "127.0.0.1:0")
	defer ln.Close()
	port := getPort(t, ln)

	dialer := &net.Dialer{Timeout: 2 * time.Second}
	conn, err := dialResolved(context.Background(), dialer, "tcp", "mixed.test",
		[]net.IP{net.ParseIP("2001:db8::1"), net.ParseIP("127.0.0.1")}, port, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	conn.Close()
}

func TestIsIPv6Enabled(t *testing.T) {
	cases := []struct {
		val  string
		want bool
	}{
		{"1", true},
		{"true", true},
		{"True", true},
		{"TRUE", true},
		{"  1  ", true},
		{"", false},
		{"0", false},
		{"false", false},
		{"yes", false},
		{"2", false},
	}
	for _, c := range cases {
		if got := isIPv6Enabled(c.val); got != c.want {
			t.Errorf("isIPv6Enabled(%q) = %v, want %v", c.val, got, c.want)
		}
	}
}

func TestObjectResolverPartialAnswerRecoversBeforeDial(t *testing.T) {
	ln := startTCPListener(t, "127.0.0.1:0")
	defer ln.Close()
	var queries []string
	r := &objectResolver{lookup: func(ctx context.Context, network, host string) ([]net.IP, error) {
		queries = append(queries, network+":"+host)
		if host == "storage.test" {
			return []net.IP{net.ParseIP("2001:db8::1")}, nil
		}
		if host != "storage.test." || network != "ip4" {
			t.Fatalf("unexpected lookup %s %s", network, host)
		}
		return []net.IP{net.ParseIP("127.0.0.1")}, nil
	}}
	for i := 0; i < 3; i++ {
		ips, err := r.fetch(context.Background(), "storage.test", false)
		if err != nil {
			t.Fatal(err)
		}
		conn, err := dialResolved(context.Background(), &net.Dialer{Timeout: time.Second}, "tcp", "storage.test", ips, getPort(t, ln), false)
		if err != nil {
			t.Fatal(err)
		}
		conn.Close()
	}
	if got := strings.Join(queries, ","); got != "ip:storage.test,ip4:storage.test." {
		t.Fatalf("lookups = %s", got)
	}
}

func TestObjectResolverDoesNotCacheUnusableAnswers(t *testing.T) {
	for _, kind := range []string{"AAAA", "empty", "error", "absolute"} {
		t.Run(kind, func(t *testing.T) {
			calls, healthy := 0, false
			host := "storage.test"
			if kind == "absolute" {
				host += "."
			}
			r := &objectResolver{lookup: func(context.Context, string, string) ([]net.IP, error) {
				calls++
				if healthy {
					return []net.IP{net.ParseIP("127.0.0.1")}, nil
				}
				switch kind {
				case "error":
					return nil, &net.DNSError{Err: "temporary DNS failure", IsTemporary: true}
				case "empty":
					return nil, nil
				default:
					return []net.IP{net.ParseIP("2001:db8::1")}, nil
				}
			}}
			if _, err := r.fetch(context.Background(), host, false); err == nil {
				t.Fatal("unusable answer succeeded")
			}
			wantCalls := 2
			if kind == "error" || kind == "absolute" {
				wantCalls = 1
			}
			if calls != wantCalls {
				t.Fatalf("lookup count = %d, want %d", calls, wantCalls)
			}
			healthy = true
			if _, err := r.fetch(context.Background(), host, false); err != nil {
				t.Fatalf("retry failed: %v", err)
			}
			if calls != wantCalls+1 {
				t.Fatal("retry did not resolve again")
			}
		})
	}
}

func TestObjectResolverCachePolicyAndExpiry(t *testing.T) {
	for _, ipv6 := range []bool{false, true} {
		calls := 0
		r := &objectResolver{lookup: func(context.Context, string, string) ([]net.IP, error) {
			calls++
			if ipv6 {
				return []net.IP{net.ParseIP("2001:db8::1")}, nil
			}
			return []net.IP{net.ParseIP("127.0.0.1")}, nil
		}}
		for i := 0; i < 2; i++ {
			if _, err := r.fetch(context.Background(), "storage.test", ipv6); err != nil {
				t.Fatal(err)
			}
		}
		if calls != 1 {
			t.Fatalf("usable answer was not cached: %d", calls)
		}
		r.cache.Range(func(key, value any) bool {
			answer := value.(objectDNSAnswer)
			answer.expires = time.Now().Add(-time.Second)
			r.cache.Store(key, answer)
			return true
		})
		if _, err := r.fetch(context.Background(), "storage.test", ipv6); err != nil {
			t.Fatal(err)
		}
		if calls != 2 {
			t.Fatalf("expired answer was reused: %d", calls)
		}
	}
}
