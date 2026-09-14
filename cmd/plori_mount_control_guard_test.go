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
	"testing"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	pmount "github.com/juicedata/juicefs/pkg/plori/mount"
)

// ploriFS.Open calls ploriMountContext for each delivery mode, then passes the
// resulting context to ploriVFSConfig. Keep the assertion on that path so a
// mode-specific Open change cannot leave one mounted VFS with .control enabled.
func TestPloriVFSConfigDisablesInternalCommandsForBothMountModes(t *testing.T) {
	for _, tc := range []struct {
		name  string
		inPod bool
	}{
		{name: "node broker", inPod: false},
		{name: "in pod", inPod: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, err := ploriMountContext(
				pmount.MountOptions{}, pmount.Paths{CacheDir: "/private/cache"}, tc.inPod,
			)
			if err != nil {
				t.Fatal(err)
			}
			conf := ploriVFSConfig(ctx, &meta.Config{}, &meta.Format{}, &chunk.Config{})
			if !conf.DisableInternalCommands {
				t.Fatal(".control commands remain enabled")
			}
		})
	}
}

// Generic mount, gateway, and mdtest keep using getVfsConf. The Plori setting
// must not become a process-wide default merely because the Plori build is in
// use.
func TestGenericVFSConfigKeepsInternalCommandsEnabled(t *testing.T) {
	ctx, err := ploriMountContext(
		pmount.MountOptions{}, pmount.Paths{CacheDir: "/private/cache"}, false,
	)
	if err != nil {
		t.Fatal(err)
	}
	conf := getVfsConf(ctx, &meta.Config{}, &meta.Format{}, &chunk.Config{})
	if conf.DisableInternalCommands {
		t.Fatal("generic VFS configuration disables .control commands")
	}
}
