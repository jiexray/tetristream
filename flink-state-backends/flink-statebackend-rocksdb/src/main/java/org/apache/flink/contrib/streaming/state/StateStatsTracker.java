/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.metrics.MetricGroup;

import java.util.concurrent.atomic.AtomicLong;

public class StateStatsTracker {
    public StateStatsTracker(MetricGroup metricGroup) {
        registerMetrics(metricGroup);
    }

    private final AtomicLong syncDuration = new AtomicLong(0L);
    private final AtomicLong asyncDuration = new AtomicLong(0L);

    static final String SNAPSHOT_SYNC_DURATION = "snapshotSyncDuration";

    static final String SNAPSHOT_ASYNC_DURATION = "snapshotAsyncDuration";

    private void registerMetrics(MetricGroup metricGroup) {
        metricGroup.gauge(SNAPSHOT_SYNC_DURATION, () -> syncDuration.getAndSet(0L));
        metricGroup.gauge(SNAPSHOT_ASYNC_DURATION, () -> asyncDuration.getAndSet(0L));
    }

    public void updateSnapshotSync(long duration) {
        syncDuration.addAndGet(duration);
    }

    public void updateSnapshotAsync(long duration) {
        asyncDuration.addAndGet(duration);
    }
}
