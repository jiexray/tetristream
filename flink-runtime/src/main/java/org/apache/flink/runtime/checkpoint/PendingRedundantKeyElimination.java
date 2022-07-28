/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PendingRedundantKeyElimination {
    public enum TaskAcknowledgeResult {
        SUCCESS,
        DUPLICATE,
        UNKNOWN,
    }

    private static final Logger LOG = LoggerFactory.getLogger(CheckpointCoordinator.class);

    private final Object lock = new Object();

    private final JobID jobId;

    private final Map<ExecutionVertexID, ExecutionVertex> notYetAcknowledgedTasks;

    private final Set<ExecutionVertexID> acknowledgedTasks;

    private int numAcknowledgedTasks;

    public PendingRedundantKeyElimination(
            JobID jobId, Map<ExecutionVertexID, ExecutionVertex> verticesToConfirm) {
        this.jobId = jobId;
        this.notYetAcknowledgedTasks = verticesToConfirm;
        acknowledgedTasks = new HashSet<>(verticesToConfirm.size());
    }

    public boolean areTasksFullyAcknowledged() {
        return notYetAcknowledgedTasks.isEmpty();
    }

    public TaskAcknowledgeResult acknowledgeTask(ExecutionVertexID executionVertexID) {
        synchronized (lock) {
            final ExecutionVertex vertex = notYetAcknowledgedTasks.remove(executionVertexID);

            if (vertex == null) {
                if (acknowledgedTasks.contains(executionVertexID)) {
                    return TaskAcknowledgeResult.DUPLICATE;
                } else {
                    return TaskAcknowledgeResult.UNKNOWN;
                }
            } else {
                acknowledgedTasks.add(executionVertexID);
            }

            numAcknowledgedTasks++;
            return TaskAcknowledgeResult.SUCCESS;
        }
    }
}
