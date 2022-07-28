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

import org.apache.flink.api.common.TaskInfo;

import javax.annotation.concurrent.GuardedBy;

import java.util.LinkedList;
import java.util.Objects;

public class CompactionElement implements Comparable<CompactionElement> {
    private final TaskInfo taskInfo;
    private final String subTaskName;

    @GuardedBy("lock")
    private final LinkedList<CompactionRunner> compactions;

    private volatile double score;

    public CompactionElement(TaskInfo taskInfo) {
        this.taskInfo = taskInfo;
        this.subTaskName = taskInfo.getTaskNameWithSubtasks();
        this.compactions = new LinkedList<>();
        this.score = 0;
    }

    public void addLast(CompactionRunner compaction) {
        compactions.addLast(compaction);
        score += compaction.getCompactionScore();
    }

    public CompactionRunner getFirst() {
        if (compactions.isEmpty()) {
            return null;
        }

        CompactionRunner firstCompaction = compactions.removeFirst();
        score -= firstCompaction.getCompactionScore();
        return firstCompaction;
    }

    public LinkedList<CompactionRunner> getCompactions() {
        return compactions;
    }

    @Override
    public int compareTo(CompactionElement other) {
        if (other == null) {
            return 1;
        }

        if (Double.compare(this.score, other.score) != 0) {
            return Double.compare(this.score, other.score);
        }

        return Integer.compare(this.compactions.size(), other.compactions.size());
    }

    @Override
    public String toString() {
        return "CompactionElement{"
                + "subTaskName='"
                + subTaskName
                + '\''
                + ", compactions="
                + compactions
                + ", score="
                + score
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CompactionElement element = (CompactionElement) o;

        return Objects.equals(subTaskName, element.subTaskName);
    }

    @Override
    public int hashCode() {
        return subTaskName != null ? subTaskName.hashCode() : 0;
    }

    public String getSubTaskName() {
        return subTaskName;
    }
}
