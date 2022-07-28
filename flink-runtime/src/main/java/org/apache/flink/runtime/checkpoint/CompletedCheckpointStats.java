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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.jobgraph.JobVertexID;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class CompletedCheckpointStats extends AbstractCheckpointStats {

    private static final long serialVersionUID = 138833868551861344L;

    private final long stateSize;

    private final long totalStateSize;

    private final SubtaskStateStats latestAcknowledgedSubtask;

    private final String externalPointer;

    private volatile boolean discarded;

    CompletedCheckpointStats(
            long checkpointId,
            long triggerTimestamp,
            CheckpointProperties props,
            int totalSubtaskCount,
            Map<JobVertexID, TaskStateStats> taskStats,
            int numAcknowledgedSubtasks,
            long stateSize,
            SubtaskStateStats latestAcknowledgedSubtask,
            String externalPointer) {

        this(
                checkpointId,
                triggerTimestamp,
                props,
                totalSubtaskCount,
                taskStats,
                numAcknowledgedSubtasks,
                stateSize,
                0L,
                latestAcknowledgedSubtask,
                externalPointer);
    }

    CompletedCheckpointStats(
            long checkpointId,
            long triggerTimestamp,
            CheckpointProperties props,
            int totalSubtaskCount,
            Map<JobVertexID, TaskStateStats> taskStats,
            int numAcknowledgedSubtasks,
            long stateSize,
            long totalStateSize,
            SubtaskStateStats latestAcknowledgedSubtask,
            String externalPointer) {

        super(checkpointId, triggerTimestamp, props, totalSubtaskCount, taskStats);
        checkArgument(
                numAcknowledgedSubtasks == totalSubtaskCount, "Did not acknowledge all subtasks.");
        checkArgument(stateSize >= 0, "Negative state size");
        this.stateSize = stateSize;
        this.totalStateSize = totalStateSize;
        this.latestAcknowledgedSubtask = checkNotNull(latestAcknowledgedSubtask);
        this.externalPointer = externalPointer;
    }

    @Override
    public CheckpointStatsStatus getStatus() {
        return CheckpointStatsStatus.COMPLETED;
    }

    @Override
    public int getNumberOfAcknowledgedSubtasks() {
        return numberOfSubtasks;
    }

    @Override
    public long getStateSize() {
        return stateSize;
    }

    public long getTotalStateSize() {
        return totalStateSize;
    }

    @Override
    @Nullable
    public SubtaskStateStats getLatestAcknowledgedSubtaskStats() {
        return latestAcknowledgedSubtask;
    }

    public String getExternalPath() {
        return externalPointer;
    }

    public boolean isDiscarded() {
        return discarded;
    }

    DiscardCallback getDiscardCallback() {
        return new DiscardCallback();
    }

    class DiscardCallback {

        void notifyDiscardedCheckpoint() {
            discarded = true;
        }
    }

    @Override
    public String toString() {
        return "CompletedCheckpoint(id=" + getCheckpointId() + ")";
    }
}
