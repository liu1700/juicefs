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

import "github.com/prometheus/client_golang/prometheus"

var quotaTripsDesc = prometheus.NewDesc("plori_quota_trips_total",
	"Volume quota admissions by terminal outcome.", []string{"outcome"}, nil)

func (s *Supervisor) Describe(ch chan<- *prometheus.Desc) { ch <- quotaTripsDesc }

func (s *Supervisor) Collect(ch chan<- prometheus.Metric) {
	for i, outcome := range []string{"admitted", "refused", "interrupted"} {
		ch <- prometheus.MustNewConstMetric(quotaTripsDesc, prometheus.CounterValue,
			float64(s.admissionOutcomes[i].Load()), outcome)
	}
}
