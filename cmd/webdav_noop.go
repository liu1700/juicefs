//go:build nowebdav || plori
// +build nowebdav plori

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

package cmd

import (
	"errors"

	"github.com/urfave/cli/v2"
)

func cmdWebDav() *cli.Command {
	return &cli.Command{
		Name:     "webdav",
		Category: "SERVICE",
		Usage:    "Start a WebDAV server (not included)",
		// The `plori` tag also selects this stub: pkg/fs/http.go, which holds
		// StartHTTPServer and WebdavConfig, is `//go:build !plori`, so the
		// real command cannot compile in that profile (PLO-445). The Plori
		// release build sets both tags.
		Description: `This feature is not included. If you want it, recompile juicefs without the "nowebdav" or "plori" flag`,
		Action: func(*cli.Context) error {
			return errors.New("not supported")
		},
	}
}
