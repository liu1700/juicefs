//go:build !plori
// +build !plori

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
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

// s3Credentials is the credential provider the S3 backend signs with.
//
// The default build returns exactly what the call sites used to construct
// inline: a static provider when a key was configured, and nil when one was
// not, so the SDK falls through to its own credential chain. The Plori build
// replaces this one function (s3_credentials_plori.go) with a provider whose
// key can be replaced while the process runs.
func s3Credentials(accessKey, secretKey, token string) aws.CredentialsProvider {
	if accessKey == "" {
		return nil
	}
	return credentials.NewStaticCredentialsProvider(accessKey, secretKey, token)
}
