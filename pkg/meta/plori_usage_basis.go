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

package meta

// Stub for merging. The slice_data branch (feat/quota-slice-data) defines these
// names: the constants in volume_reservation.go and the two functions in
// plori_quota.go. This file lets the trash and usage-report changes compile on
// their own and is deleted when this branch is merged onto that one.

const (
	QuotaBasisLogical   = "logical"
	QuotaBasisSliceData = "slice_data"
)

// PloriQuotaBasis is the basis the volume byte ceiling of m is compared
// against. Without the slice_data branch every volume is logical.
func PloriQuotaBasis(m Meta) string {
	return QuotaBasisLogical
}

// PloriLogicalBytes is the logical used space. Without the slice_data branch
// it is the used space StatFS reports.
func PloriLogicalBytes(m Meta) int64 {
	var total, avail, iused, iavail uint64
	if st := m.StatFS(Background(), RootInode, &total, &avail, &iused, &iavail); st != 0 {
		return 0
	}
	return int64(total - avail)
}
