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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class CompactionPriorityQueue {
    private static final Logger LOG = LoggerFactory.getLogger(CompactionPriorityQueue.class);

    private final Object lock;
    private final boolean printCompactionDebugMsg;

    @GuardedBy("lock")
    private final List<CompactionElement> queue;

    private final Map<String, CompactionElement> index;

    public CompactionPriorityQueue(boolean printCompactionDebugMsg) {
        this.lock = new Object();
        this.printCompactionDebugMsg = printCompactionDebugMsg;

        queue = new LinkedList<>();
        index = new HashMap<>();
    }

    public CompactionRunner poll() {
        synchronized (lock) {
            if (printCompactionDebugMsg) {
                LOG.info("On polling compaction job, compaction elements in pq: {}", queue);
            }

            if (queue.isEmpty()) {
                return null;
            }

            CompactionElement firstElement = null;
            for (CompactionElement element : queue) {
                if (firstElement == null) {
                    firstElement = element;
                } else if (firstElement.compareTo(element) < 0) {
                    firstElement = element;
                }
            }

            CompactionRunner firstCompaction = firstElement.getFirst();

            return firstCompaction;
        }
    }

    public void push(TaskInfo taskInfo, CompactionRunner compaction) {
        if (compaction == null) {
            return;
        }
        synchronized (lock) {
            CompactionElement element = index.get(taskInfo.getTaskNameWithSubtasks());
            if (element == null) {
                element = new CompactionElement(taskInfo);
                index.put(taskInfo.getTaskNameWithSubtasks(), element);
                queue.add(element);
            }
            element.addLast(compaction);
        }
    }

    public void remove(TaskInfo taskInfo) {
        synchronized (lock) {
            index.remove(taskInfo);
            queue.removeIf(
                    element -> element.getSubTaskName().equals(taskInfo.getTaskNameWithSubtasks()));
        }
    }

    public int numOfCompactions(TaskInfo taskInfo) {
        synchronized (lock) {
            CompactionElement element = index.get(taskInfo.getTaskNameWithSubtasks());
            return element == null ? 0 : element.getCompactions().size();
        }
    }
}
