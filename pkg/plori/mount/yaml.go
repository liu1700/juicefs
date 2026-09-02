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
	"errors"
	"fmt"
	"strings"
)

// marshalLitestreamYAML renders the fixed Litestream config shape by hand.
//
// A YAML library would be the obvious choice, but every yaml module in this
// repo's graph is an indirect dependency today; importing one would link it
// into the release binary and put a parser the mount worker never needs to
// read anything with into the SBOM. The shape here is closed and small, so
// the honest trade is a renderer plus a round-trip test (yaml_test.go parses
// the output with a real YAML parser, which stays a test-only import).
//
// Every scalar goes through yamlQuote: the values include object prefixes and
// endpoints that come from the MountSpec, and an unquoted scalar containing a
// `:` or a leading `*` would change the document's structure.
func marshalLitestreamYAML(cfg *litestreamConfig) ([]byte, error) {
	if len(cfg.DBs) != 1 {
		return nil, errors.New("litestream config must describe exactly one database")
	}
	db := cfg.DBs[0]
	var b strings.Builder
	fmt.Fprintf(&b, "addr: %s\n", yamlQuote(cfg.Addr))
	b.WriteString("socket:\n")
	fmt.Fprintf(&b, "  enabled: %t\n", cfg.Socket.Enabled)
	fmt.Fprintf(&b, "  path: %s\n", yamlQuote(cfg.Socket.Path))
	fmt.Fprintf(&b, "  permissions: %d\n", cfg.Socket.Permissions)
	b.WriteString("levels:\n")
	for _, level := range cfg.Levels {
		fmt.Fprintf(&b, "  - interval: %s\n", yamlQuote(level.Interval))
	}
	b.WriteString("snapshot:\n")
	fmt.Fprintf(&b, "  interval: %s\n", yamlQuote(cfg.Snapshot.Interval))
	fmt.Fprintf(&b, "l0-retention: %s\n", yamlQuote(cfg.L0Retention))
	b.WriteString("logging:\n")
	fmt.Fprintf(&b, "  level: %s\n", yamlQuote(cfg.Logging.Level))
	fmt.Fprintf(&b, "  type: %s\n", yamlQuote(cfg.Logging.Type))
	fmt.Fprintf(&b, "  stderr: %t\n", cfg.Logging.Stderr)
	b.WriteString("dbs:\n")
	fmt.Fprintf(&b, "  - path: %s\n", yamlQuote(db.Path))
	b.WriteString("    replica:\n")
	fmt.Fprintf(&b, "      type: %s\n", yamlQuote(db.Replica.Type))
	fmt.Fprintf(&b, "      bucket: %s\n", yamlQuote(db.Replica.Bucket))
	fmt.Fprintf(&b, "      path: %s\n", yamlQuote(db.Replica.Path))
	fmt.Fprintf(&b, "      endpoint: %s\n", yamlQuote(db.Replica.Endpoint))
	fmt.Fprintf(&b, "      region: %s\n", yamlQuote(db.Replica.Region))
	fmt.Fprintf(&b, "      force-path-style: %t\n", db.Replica.ForcePathStyle)
	fmt.Fprintf(&b, "      sync-interval: %s\n", yamlQuote(db.Replica.SyncInterval))
	return []byte(b.String()), nil
}

// yamlQuote renders a Go string as a YAML double-quoted scalar. Double quotes
// are the only YAML style that accepts every byte through escapes, so the
// renderer never has to reason about which characters are safe bare.
func yamlQuote(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			if r < 0x20 {
				fmt.Fprintf(&b, `\x%02x`, r)
				continue
			}
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String()
}
