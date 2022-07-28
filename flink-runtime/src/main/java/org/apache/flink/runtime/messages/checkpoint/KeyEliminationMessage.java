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

package org.apache.flink.runtime.messages.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.Objects;

import scala.Serializable;

public class KeyEliminationMessage implements Serializable {

    private final JobID job;

    private final ExecutionVertexID taskVertexId;

    public KeyEliminationMessage(JobID job, ExecutionVertexID taskVertexId) {
        if (job == null || taskVertexId == null) {
            throw new NullPointerException();
        }
        this.job = job;
        this.taskVertexId = taskVertexId;
    }

    public JobID getJob() {
        return job;
    }

    public ExecutionVertexID getTaskVertexId() {
        return taskVertexId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KeyEliminationMessage that = (KeyEliminationMessage) o;
        return Objects.equals(job, that.job) && Objects.equals(taskVertexId, that.taskVertexId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(job, taskVertexId);
    }

    @Override
    public String toString() {
        return "KeyEliminationMessage{" + "job=" + job + ", taskVertexId=" + taskVertexId + '}';
    }
}
