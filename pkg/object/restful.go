/*
 * JuiceFS, Copyright 2018 Juicedata, Inc.
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
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

var resolver = &objectResolver{lookup: net.DefaultResolver.LookupIP}

// Cache only answers usable by this client's address-family policy. A partial
// search-path answer must not poison every storage retry in this process.
type objectResolver struct {
	lookup func(context.Context, string, string) ([]net.IP, error)
	cache  sync.Map
}

type objectDNSAnswer struct {
	ips     []net.IP
	expires time.Time
}

func (r *objectResolver) fetch(ctx context.Context, host string, ipv6 bool) ([]net.IP, error) {
	network := "ip4"
	if ipv6 {
		network = "ip"
	}
	key := network + ":" + host
	if v, ok := r.cache.Load(key); ok {
		answer := v.(objectDNSAnswer)
		if time.Now().Before(answer.expires) {
			return answer.ips, nil
		}
		r.cache.Delete(key)
	}
	// Resolve both families to preserve search-name behavior. In IPv4-only mode
	// an AAAA-only answer gets one fresh absolute-name lookup, bypassing ndots
	// and cached search-qualified responses without changing the HTTP/TLS host.
	ips, err := r.lookup(ctx, "ip", host)
	if err != nil {
		return nil, err
	}
	primaries, fallbacks := dialFamilies(ips, ipv6)
	if len(primaries) == 0 && len(fallbacks) == 0 && !ipv6 && !strings.HasSuffix(host, ".") {
		ips, err = r.lookup(ctx, "ip4", host+".")
		if err != nil {
			return nil, err
		}
		primaries, fallbacks = dialFamilies(ips, ipv6)
	}
	if len(primaries) == 0 && len(fallbacks) == 0 {
		return nil, &net.DNSError{Err: "no IPv4 address found", Name: host, IsNotFound: true}
	}
	r.cache.Store(key, objectDNSAnswer{ips: ips, expires: time.Now().Add(time.Minute)})
	return ips, nil
}

var httpClient *http.Client

// Dialing IPv6 as the primary address family and IPv4 as a fallback costs 300 milliseconds on
// every cold connection in an environment where a pod has an IPv6 address and default route but
// IPv6 egress does not reach the destination: the IPv6 attempt gets no answer and only gives up
// when the fallback timer fires. When a DNS answer carries only an IPv6 (AAAA) address, there is
// no IPv4 fallback to race against, so the dial waits out the full dialer timeout instead of
// failing fast. No object store this fork talks to requires IPv6. Set JFS_ENABLE_IPV6=1 to
// restore the IPv6-primary, IPv4-fallback dual-stack race.
var enableIPv6 = isIPv6Enabled(os.Getenv("JFS_ENABLE_IPV6"))

func isIPv6Enabled(val string) bool {
	v := strings.ToLower(strings.TrimSpace(val))
	return v == "1" || v == "true"
}

func splitIPsByVersion(ips []net.IP) ([]net.IP, []net.IP) {
	ipv6 := make([]net.IP, 0, len(ips))
	ipv4 := make([]net.IP, 0, len(ips))
	for _, ip := range ips {
		if ip.To4() == nil {
			ipv6 = append(ipv6, ip)
		} else {
			ipv4 = append(ipv4, ip)
		}
	}
	return ipv6, ipv4
}

// dialFamilies splits a DNS answer into the primary and fallback address groups that
// dialParallel races. When IPv6 is disabled (the default), IPv6 addresses are dropped and the
// IPv4 addresses become the only primaries, with no fallback group, so a cold connection dials
// IPv4 directly instead of waiting on an IPv6 attempt that cannot succeed. When IPv6 is enabled,
// the historical behavior is unchanged: IPv6 addresses are primary and IPv4 addresses are the
// fallback.
func dialFamilies(ips []net.IP, enableIPv6 bool) (primaries, fallbacks []net.IP) {
	ipv6, ipv4 := splitIPsByVersion(ips)
	if !enableIPv6 {
		return ipv4, nil
	}
	return ipv6, ipv4
}

// dialResolved dials an already-resolved DNS answer. It is split out from the DialContext
// closure so the no-IPv4-address fast failure is testable without a real DNS lookup.
func dialResolved(ctx context.Context, dialer *net.Dialer, network, host string, ips []net.IP, port string, enableIPv6 bool) (net.Conn, error) {
	primaries, fallbacks := dialFamilies(ips, enableIPv6)
	if len(primaries) == 0 && len(fallbacks) == 0 {
		// IPv6 is disabled and the DNS answer had only IPv6 addresses: fail fast instead of
		// handing dialParallel two empty address lists to wait out.
		return nil, &net.DNSError{Err: "no IPv4 address found", Name: host, IsNotFound: true}
	}
	return dialParallel(ctx, dialer, network, primaries, fallbacks, port)
}

// dialParallel is adapted from the Go standard library.
// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
func dialParallel(ctx context.Context, dialer *net.Dialer, network string, primaries, fallbacks []net.IP, port string) (net.Conn, error) {
	if len(fallbacks) == 0 {
		return dialRandom(ctx, dialer, network, primaries, port)
	}

	returned := make(chan struct{})
	defer close(returned)

	type dialResult struct {
		net.Conn
		error
		primary bool
		done    bool
	}
	results := make(chan dialResult) // unbuffered

	startRacer := func(ctx context.Context, primary bool) {
		ras := primaries
		if !primary {
			ras = fallbacks
		}
		c, err := dialRandom(ctx, dialer, network, ras, port)
		select {
		case results <- dialResult{Conn: c, error: err, primary: primary, done: true}:
		case <-returned:
			if c != nil {
				c.Close()
			}
		}
	}

	var primary, fallback dialResult

	// Start the main racer.
	primaryCtx, primaryCancel := context.WithCancel(ctx)
	defer primaryCancel()
	go startRacer(primaryCtx, true)

	// Start the timer for the fallback racer.
	fallbackTimer := time.NewTimer(300 * time.Millisecond)
	defer fallbackTimer.Stop()

	for {
		select {
		case <-fallbackTimer.C:
			fallbackCtx, fallbackCancel := context.WithCancel(ctx)
			defer fallbackCancel()
			go startRacer(fallbackCtx, false)

		case res := <-results:
			if res.error == nil {
				return res.Conn, nil
			}
			if res.primary {
				primary = res
			} else {
				fallback = res
			}
			if primary.done && fallback.done {
				return nil, errors.Join(primary.error, fallback.error)
			}
			if res.primary && fallbackTimer.Stop() {
				// If we were able to stop the timer, that means it
				// was running (hadn't yet started the fallback), but
				// we just got an error on the primary path, so start
				// the fallback immediately (in 0 nanoseconds).
				fallbackTimer.Reset(0)
			}
		}
	}
}

func dialRandom(ctx context.Context, dialer *net.Dialer, network string, ips []net.IP, port string) (net.Conn, error) {
	var lastErr error
	n := len(ips)
	if n == 0 {
		return nil, fmt.Errorf("no addresses to dial")
	}
	first := rand.Intn(n)
	for i := 0; i < n; i++ {
		ip := ips[(first+i)%n]
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
		if err == nil {
			return conn, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

func init() {
	dialer := &net.Dialer{Timeout: time.Second * 10}
	httpClient = &http.Client{
		Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			TLSHandshakeTimeout:   time.Second * 20,
			ResponseHeaderTimeout: time.Second * 30,
			IdleConnTimeout:       time.Second * 300,
			MaxIdleConnsPerHost:   500,
			ReadBufferSize:        32 << 10,
			WriteBufferSize:       32 << 10,
			DialContext: func(ctx context.Context, network string, address string) (net.Conn, error) {
				host, port, err := net.SplitHostPort(address)
				if err != nil {
					return nil, err
				}
				if ip := net.ParseIP(host); ip != nil {
					return dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
				}
				ips, err := resolver.fetch(ctx, host, enableIPv6)
				if err != nil {
					return nil, err
				}
				if len(ips) == 0 {
					return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
				}
				return dialResolved(ctx, dialer, network, host, ips, port, enableIPv6)
			},
			DisableCompression: true,
			TLSClientConfig:    &tls.Config{},
		},
		Timeout: time.Hour,
	}
}

func GetHttpClient() *http.Client {
	return httpClient
}

func cleanup(response *http.Response) {
	if response != nil && response.Body != nil {
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
	}
}

type RestfulStorage struct {
	DefaultObjectStorage
	endpoint  string
	accessKey string
	secretKey string
	signName  string
	signer    func(*http.Request, string, string, string)
}

func (s *RestfulStorage) String() string {
	return s.endpoint
}

var HEADER_NAMES = []string{"Content-MD5", "Content-Type", "Date"}

func (s *RestfulStorage) request(ctx context.Context, method, key string, body io.Reader, headers map[string]string) (*http.Response, error) {
	uri := s.endpoint + "/" + key
	req, err := http.NewRequestWithContext(ctx, method, uri, body)
	if err != nil {
		return nil, err
	}
	if f, ok := body.(*os.File); ok {
		st, err := f.Stat()
		if err == nil {
			req.ContentLength = st.Size()
		}
	}
	setUserAgent(req)
	now := time.Now().UTC().Format(http.TimeFormat)
	req.Header.Add("Date", now)
	for key := range headers {
		req.Header.Add(key, headers[key])
	}
	s.signer(req, s.accessKey, s.secretKey, s.signName)
	return httpClient.Do(req)
}

func parseError(resp *http.Response) error {
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("request failed: %s", err)
	}
	// The status is the only classifier this backend has, and the message stays
	// as it was: classify only adds the failure class (PLO-458).
	return classify(classFromStatus(resp.StatusCode),
		fmt.Errorf("status: %v, message: %s", resp.StatusCode, string(data)))
}

func (s *RestfulStorage) Head(ctx context.Context, key string) (Object, error) {
	resp, err := s.request(ctx, "HEAD", key, nil, nil)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, os.ErrNotExist
	}
	defer cleanup(resp)
	if resp.StatusCode != 200 {
		return nil, parseError(resp)
	}

	lastModified := resp.Header.Get("Last-Modified")
	if lastModified == "" {
		return nil, fmt.Errorf("cannot get last modified time")
	}
	mtime, _ := time.Parse(time.RFC1123, lastModified)
	return &obj{
		key,
		resp.ContentLength,
		mtime,
		strings.HasSuffix(key, "/"),
		"",
		"",
	}, nil
}

func getRange(off, limit int64) string {
	if off > 0 || limit > 0 {
		if limit > 0 {
			return fmt.Sprintf("bytes=%d-%d", off, off+limit-1)
		} else {
			return fmt.Sprintf("bytes=%d-", off)
		}
	}
	return ""
}

func checkGetStatus(statusCode int, partial bool) error {
	var expected = http.StatusOK
	if partial {
		expected = http.StatusPartialContent
	}
	if statusCode != expected {
		return fmt.Errorf("expected status code %d, but got %d", expected, statusCode)
	}
	return nil
}

func (s *RestfulStorage) Get(ctx context.Context, key string, off, limit int64, getters ...AttrGetter) (io.ReadCloser, error) {
	headers := make(map[string]string)
	if off > 0 || limit > 0 {
		headers["Range"] = getRange(off, limit)
	}
	resp, err := s.request(ctx, "GET", key, nil, headers)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 && resp.StatusCode != 206 {
		return nil, parseError(resp)
	}
	if err = checkGetStatus(resp.StatusCode, len(headers) > 0); err != nil {
		_ = resp.Body.Close()
		return nil, err
	}
	return resp.Body, nil
}

func (u *RestfulStorage) Put(ctx context.Context, key string, body io.Reader, getters ...AttrGetter) error {
	resp, err := u.request(ctx, "PUT", key, body, nil)
	if err != nil {
		return err
	}
	defer cleanup(resp)
	if resp.StatusCode != 201 && resp.StatusCode != 200 {
		return parseError(resp)
	}
	return nil
}

func (s *RestfulStorage) Copy(ctx context.Context, dst, src string) error {
	in, err := s.Get(ctx, src, 0, -1)
	if err != nil {
		return err
	}
	defer in.Close()
	d, err := io.ReadAll(in)
	if err != nil {
		return err
	}
	return s.Put(ctx, dst, bytes.NewReader(d))
}

func (s *RestfulStorage) Delete(ctx context.Context, key string, getters ...AttrGetter) error {
	resp, err := s.request(ctx, "DELETE", key, nil, nil)
	if err != nil {
		return err
	}
	defer cleanup(resp)
	if resp.StatusCode != 204 && resp.StatusCode != 404 {
		return parseError(resp)
	}
	return nil
}

func (s *RestfulStorage) List(ctx context.Context, prefix, marker, token, delimiter string, limit int64, followLink bool) ([]Object, bool, string, error) {
	return nil, false, "", notSupported
}

var _ ObjectStorage = (*RestfulStorage)(nil)
