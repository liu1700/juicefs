//go:build !nosqlite && !plori
// +build !nosqlite,!plori

/*
 * JuiceFS, Copyright 2022 Juicedata, Inc.
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

// The plori profile supports SQLite as a *metadata* engine (PLO-319) but keeps
// remote object storage S3-only, so it must not also register SQLite as an
// object store: that is a separate backend with its own surface, and the
// audited support policy lists `s3` alone. The `!plori` tag removes only this
// registration. `database/sql` still has the sqlite3 driver, because
// pkg/meta/sql_sqlite.go imports it for the metadata engine.
package object

import (
	_ "github.com/mattn/go-sqlite3"
)

func init() {
	Register("sqlite3", func(addr, user, pass, token string) (ObjectStorage, error) {
		return newSQLStore("sqlite3", removeScheme(addr), user, pass)
	})
}
