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
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class CompactionSchedulerManager {
    private static final Logger LOG = LoggerFactory.getLogger(CompactionSchedulerManager.class);

    Map<TaskInfo, CompactionCollector> registerCompactionSchedulers = new ConcurrentHashMap<>();

    private final ScheduledExecutorService compactionRunnerExecutor;

    private final ScheduledExecutorService compactionCollectorExecutor;

    private final CompactionPriorityQueue compactionQueue;

    private final CompactionSchedulerManagerConfig config;

    private volatile boolean closed;

    public CompactionSchedulerManager(CompactionSchedulerManagerConfig config) {
        this.compactionRunnerExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        new DispatcherThreadFactory(
                                Thread.currentThread().getThreadGroup(), "Compaction Runner"));

        this.compactionCollectorExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        new DispatcherThreadFactory(
                                Thread.currentThread().getThreadGroup(), "Compaction Collector"));

        this.config = config;

        compactionQueue = new CompactionPriorityQueue(config.isPrintCompactionDebugMsg());
    }

    public void registerCompactionScheduler(
            TaskInfo taskInfo, CompactionCollector compactionScheduler) {
        if (closed) {
            return;
        }
        registerCompactionSchedulers.putIfAbsent(taskInfo, compactionScheduler);
    }

    public void unregisterCompactionScheduler(TaskInfo taskInfo) {
        if (closed) {
            return;
        }
        compactionQueue.remove(taskInfo);
        registerCompactionSchedulers.remove(taskInfo);
    }

    private void scheduleCollection() {
        if (closed) {
            return;
        }

        for (Map.Entry<TaskInfo, CompactionCollector> entry :
                registerCompactionSchedulers.entrySet()) {
            TaskInfo task = entry.getKey();
            CompactionCollector compactionCollector = entry.getValue();

            if (compactionCollector == null) {
                continue;
            }

            if (compactionQueue.numOfCompactions(task)
                    > CompactionSchedulerManagerConfig.MAX_PENDING_COMPACTIONS) {
                continue;
            }

            try {
                compactionQueue.push(task, compactionCollector.pickCompactionFromTask());
            } catch (Exception e) {}
        }
    }

    private void runCompaction(Consumer<CompactionJobMetrics> compactionScheduleCallback) {
        try (CompactionRunner runner = compactionQueue.poll()) {
            if (runner != null) {
                compactionScheduleCallback.accept(runner.runCompaction());
                return;
            } else {
            }
        } catch (Exception e) {
        }
        compactionScheduleCallback.accept(null);
    }

    private void scheduleCompaction() {
        if (closed) {
            return;
        }

        runCompaction(
                metrics ->
                        compactionRunnerExecutor.schedule(
                                this::scheduleCompaction,
                                config.waitForNextCompaction(metrics),
                                TimeUnit.MILLISECONDS));
    }

    public void start() {
        compactionCollectorExecutor.scheduleAtFixedRate(
                this::scheduleCollection, 0, config.getCollectInterval(), TimeUnit.MILLISECONDS);

        compactionRunnerExecutor.schedule(
                this::scheduleCompaction, config.getIdleInterval(), TimeUnit.MILLISECONDS);
    }

    public void close() {
        if (closed) {
            return;
        }
        closed = true;

        if (!registerCompactionSchedulers.isEmpty()) {
            for (Map.Entry<TaskInfo, CompactionCollector> entry :
                    registerCompactionSchedulers.entrySet()) {
                CompactionCollector collector = entry.getValue();
                String taskName = entry.getKey().getTaskNameWithSubtasks();
                try {
                    collector.close();
                } catch (Exception e) {}
            }
            registerCompactionSchedulers.clear();
        }

        compactionRunnerExecutor.shutdown();
        compactionCollectorExecutor.shutdown();
    }
}
