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

package org.apache.flink.runtime.state.rocksdb;

public class CompactionSchedulerManagerConfig {
    private final double compactionCpuRatio;
    private final long idleInterval;
    private final long collectInterval;
    private final boolean printCompactionDebugMsg;

    public static final int MAX_PENDING_COMPACTIONS = 5;

    public CompactionSchedulerManagerConfig(
            double compactionCpuRatio, long idleInterval, long collectInterval, boolean printCompactionDebugMsg) {
        this.compactionCpuRatio = compactionCpuRatio;
        this.idleInterval = idleInterval;
        this.collectInterval = collectInterval;
        this.printCompactionDebugMsg = printCompactionDebugMsg;
    }

    public double getCompactionCpuRatio() {
        return compactionCpuRatio;
    }

    public long getIdleInterval() {
        return idleInterval;
    }

    public long getCollectInterval() {
        return collectInterval;
    }

    public long waitForNextCompaction(CompactionJobMetrics metrics) {
        if (metrics == null) {
            return idleInterval;
        }

        if (metrics.isCompactionForKeyElimination()) {
            return 0L;
        }

        return (long) (metrics.getCompactionDuration() / (1 - compactionCpuRatio));
    }

    public boolean isPrintCompactionDebugMsg() {
        return printCompactionDebugMsg;
    }
}
