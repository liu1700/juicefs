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
	"errors"
	"testing"

	pmount "github.com/juicedata/juicefs/pkg/plori/mount"
	"github.com/juicedata/juicefs/pkg/vfs"
)

func TestMountModeBindsCredentialDelivery(t *testing.T) {
	for _, tc := range []struct {
		name           string
		mode           mountMode
		source         string
		credentialFile string
		replicator     string
		wantOK         bool
	}{
		{"node broker preserves its source", mountModeNodeBroker, pmount.CredentialSourceNodeSecret, "", "node.sock", true},
		{"in pod stages a private credential file", mountModeInPod, pmount.CredentialSourceClaimInline, "/private/credential.json", "", true},
		{"node broker rejects inline source", mountModeNodeBroker, pmount.CredentialSourceClaimInline, "/private/credential.json", "", false},
		{"in pod rejects node secret", mountModeInPod, pmount.CredentialSourceNodeSecret, "/private/credential.json", "", false},
		{"in pod cannot fall back to AWS environment", mountModeInPod, pmount.CredentialSourceClaimInline, "", "", false},
		{"in pod owns a per-mount replicator", mountModeInPod, pmount.CredentialSourceClaimInline, "/private/credential.json", "node.sock", false},
	} {
		err := validateMountRuntime(tc.mode, tc.source, tc.credentialFile, tc.replicator)
		if (err == nil) != tc.wantOK {
			t.Errorf("%s: validateMountRuntime() = %v, want ok=%t", tc.name, err, tc.wantOK)
		}
		if err != nil && !errors.Is(err, pmount.ErrSpec) {
			t.Errorf("refusal = %v, want ErrSpec", err)
		}
	}
}

func TestInPodMountContextUsesTrustedRootSquash(t *testing.T) {
	ctx, err := ploriMountContext(pmount.MountOptions{}, pmount.Paths{CacheDir: "/private/cache"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if got := ctx.Bool("plori-trusted-all-squash-root"); !got {
		t.Error("trusted in-pod root squash is false")
	}
	if got := ctx.String("visible-owner"); got != "65532:65532" {
		t.Errorf("visible-owner = %q, want 65532:65532", got)
	}
	ctx, err = ploriMountContext(pmount.MountOptions{}, pmount.Paths{CacheDir: "/private/cache"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if got := ctx.Bool("plori-trusted-all-squash-root"); got {
		t.Error("node-broker trusted in-pod root squash is true")
	}
	if got := ctx.String("visible-owner"); got != "" {
		t.Errorf("node-broker visible-owner = %q, want empty", got)
	}
}

func TestInPodContextBuildsRootOwnershipConfig(t *testing.T) {
	ctx, err := ploriMountContext(pmount.MountOptions{}, pmount.Paths{CacheDir: "/private/cache"}, true)
	if err != nil {
		t.Fatal(err)
	}
	conf := &vfs.Config{}
	applyOwnershipMappings(conf, ctx)
	if !conf.NonDefaultPermission {
		t.Error("in-pod config left default_permissions enabled")
	}
	if conf.AllSquash == nil || conf.AllSquash.Uid != 0 || conf.AllSquash.Gid != 0 {
		t.Fatalf("in-pod all-squash = %+v, want 0:0", conf.AllSquash)
	}
	if conf.VisibleOwner == nil || conf.VisibleOwner.Uid != 65532 || conf.VisibleOwner.Gid != 65532 {
		t.Fatalf("in-pod visible owner = %+v, want 65532:65532", conf.VisibleOwner)
	}

	plain, err := ploriMountContext(pmount.MountOptions{}, pmount.Paths{CacheDir: "/private/cache"}, false)
	if err != nil {
		t.Fatal(err)
	}
	plainConf := &vfs.Config{}
	applyOwnershipMappings(plainConf, plain)
	if plainConf.AllSquash != nil || plainConf.VisibleOwner != nil {
		t.Fatalf("node-broker ownership config = all=%+v visible=%+v, want nil", plainConf.AllSquash, plainConf.VisibleOwner)
	}
}

func TestPublicSquashStillRefusesRoot(t *testing.T) {
	uid, gid := parseUIDGID("0:0", 65534, 65534)
	if uid != 65534 || gid != 65534 {
		t.Fatalf("public 0:0 squash = %d:%d, want 65534:65534", uid, gid)
	}
}

func TestMountContextAppliesOptionalCacheSize(t *testing.T) {
	ctx, err := ploriMountContext(pmount.MountOptions{CacheSizeMB: 512}, pmount.Paths{CacheDir: "/private/cache"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if got := ctx.String("cache-size"); got != "512" {
		t.Errorf("cache-size = %q, want 512", got)
	}
	ctx, err = ploriMountContext(pmount.MountOptions{}, pmount.Paths{CacheDir: "/private/cache"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if got := ctx.String("cache-size"); got != "100G" {
		t.Errorf("absent cache-size = %q, want generic default 100G", got)
	}
}
