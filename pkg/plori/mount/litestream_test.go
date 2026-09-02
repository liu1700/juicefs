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
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The rendered config is asserted verbatim. It is the file a pinned external
// binary parses, so a silent shape change here is a mount that replicates
// nowhere; CI additionally parses it with the real litestream.
func TestLitestreamConfigRendersTheWave2Defaults(t *testing.T) {
	dir := t.TempDir()
	ls := &Litestream{
		Bin:        "litestream",
		ConfigPath: filepath.Join(dir, "litestream.yml"),
		SocketPath: filepath.Join(dir, "litestream.sock"),
		DBPath:     filepath.Join(dir, "meta.db"),
	}
	spec := &MountSpec{
		MetaPrefix: "agents-meta/v1/g3/",
		ObjectStore: ObjectStore{
			Endpoint: "https://plorifs.lax1.vultrobjects.com",
			Bucket:   "plorifs",
			Region:   "lax1",
		},
	}
	if err := ls.WriteConfig(spec, ParseMountOptions(nil)); err != nil {
		t.Fatalf("WriteConfig: %v", err)
	}
	data, err := os.ReadFile(ls.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	got := string(data)

	// PLO-316 wave 2 measured these; raising sync-interval does not reduce
	// PUTs and multiplies replica lag, so the value is pinned here.
	for _, want := range []string{
		`sync-interval: "1s"`,
		`- interval: "10m0s"`,
		`- interval: "1h0m0s"`,
		`- interval: "6h0m0s"`,
		`interval: "24h0m0s"`,
		`l0-retention: "30m0s"`,
		`path: "agents-meta/v1/g3"`,
		`bucket: "plorifs"`,
		`force-path-style: true`,
		`enabled: true`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("rendered config is missing %s\n%s", want, got)
		}
	}
	// The one bucket-wide credential is inherited from the environment and
	// must never reach a file (mountspec.md §5, threat-model F-11).
	for _, forbidden := range []string{"access-key-id", "secret-access-key", "AKIA"} {
		if strings.Contains(got, forbidden) {
			t.Errorf("rendered config leaks %q:\n%s", forbidden, got)
		}
	}
	info, err := os.Stat(ls.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("config mode = %o, want 600", info.Mode().Perm())
	}
}

func TestYamlQuoteEscapesStructuralCharacters(t *testing.T) {
	cases := map[string]string{
		"plain":       `"plain"`,
		`with"quote`:  `"with\"quote"`,
		"a: b":        `"a: b"`,
		"*anchor":     `"*anchor"`,
		"line\nbreak": `"line\nbreak"`,
		"tab\there":   `"tab\there"`,
		"back\\slash": `"back\\slash"`,
		"\x01control": `"\x01control"`,
	}
	for in, want := range cases {
		if got := yamlQuote(in); got != want {
			t.Errorf("yamlQuote(%q) = %s, want %s", in, got, want)
		}
	}
}
