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

package object

import (
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

// installed is the process-wide S3 credential provider, or nil.
//
// It is process-wide because the credential is: a plori-mount worker runs one
// volume against one bucket with the one key pair the subscription has
// (PLO-351 — Vultr issues no second principal), and every S3 client this
// process builds — the data blob, the blob it rebuilds on a Format reload, and
// the conditional-PUT fencer — must sign with the same current pair or a
// rotation would leave half the process on a dead key.
var installed atomic.Pointer[aws.CredentialsProvider]

// SetS3CredentialsProvider installs the provider every S3 client built after
// it will sign with. plori-mount calls it once, before it opens anything.
//
// Passing nil restores the static behaviour, which is what the tests of the
// static path do; production never does.
func SetS3CredentialsProvider(p aws.CredentialsProvider) {
	if p == nil {
		installed.Store(nil)
		return
	}
	installed.Store(&p)
}

// S3CredentialsProviderInstalled reports whether one is installed. The worker
// asserts it after setup: with a provider installed, the access key in the
// in-memory Format is a placeholder (cmd/plori_mount.go credentialPatch), so a
// path that silently fell back to the static provider would sign every request
// with a string that is not a credential.
func S3CredentialsProviderInstalled() bool { return installed.Load() != nil }

// s3Credentials prefers the installed rotating provider over the key the
// Format carries. The Format's key is not a credential in this build — see
// SetS3CredentialsProvider — so this is not a precedence choice between two
// credentials, it is the only credential.
func s3Credentials(accessKey, secretKey, token string) aws.CredentialsProvider {
	if p := installed.Load(); p != nil {
		return *p
	}
	if accessKey == "" {
		return nil
	}
	return credentials.NewStaticCredentialsProvider(accessKey, secretKey, token)
}
