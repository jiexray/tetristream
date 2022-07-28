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

package org.apache.flink.contrib.streaming.state.compaction;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.contrib.streaming.state.RocksDBOptions;
import org.apache.flink.contrib.streaming.state.compaction.util.ColumnFamilyInfo;
import org.apache.flink.runtime.state.rocksdb.CompactionCollector;
import org.apache.flink.runtime.state.rocksdb.CompactionRunner;
import org.apache.flink.runtime.util.FatalExitExceptionHandler;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class CompactionScheduler implements CompactionCollector {
    private static final Logger LOG = LoggerFactory.getLogger(CompactionScheduler.class);

    private AtomicReference<SchedulerState> schedulerState =
            new AtomicReference<>(SchedulerState.Uninitialized);

    private CompactionStrategy compactionStrategy;
    private List<ColumnFamilyInfo> restoreStates;

    private boolean isRescaling = false;
    private boolean enableTrivialMoveAfterFileIngestion = false;
    private Supplier<Boolean> keyEliminationCallback;

    private final ScheduledExecutorService versionSynchronizerTimer =
            Executors.newSingleThreadScheduledExecutor(
                    runnable -> {
                        Thread t =
                                new Thread(
                                        Thread.currentThread().getThreadGroup(),
                                        runnable,
                                        "Version Synchronizer");
                        t.setUncaughtExceptionHandler(FatalExitExceptionHandler.INSTANCE);
                        return t;
                    });

    private final long syncRocksDBInterval;

    public CompactionScheduler(ReadableConfig config) {
        String schedulerStrategyType =
                config.get(RocksDBOptions.ROCKSDB_COMPACTION_SCHEDULE_STRATEGY_TYPE);
        Preconditions.checkArgument(schedulerStrategyType != null);

        compactionStrategy = new LevelBaseCompactionStrategy();
        restoreStates = new ArrayList<>();

        syncRocksDBInterval = config.get(RocksDBOptions.ROCKSDB_COMPACTION_SYNC_ROCKSDB_INTERVAL);
        enableTrivialMoveAfterFileIngestion = config.get(RocksDBOptions.ENABLE_TRIVIAL_MOVE_AFTER_FILE_INGESTION);
    }

    private CompactionScheduler() {
        schedulerState.set(SchedulerState.Dummy);
        syncRocksDBInterval = -1;
    }

    public void registerState(ColumnFamilyInfo columnFamilyInfo) throws Exception {
        synchronized (this) {
            switch (schedulerState.get()) {
                case Closed:
                    throw new Exception("Compaction scheduler has closed!");
                case Uninitialized:
                    restoreStates.add(columnFamilyInfo);
                    break;
                case Running:
                    compactionStrategy.appendState(columnFamilyInfo);
                    break;
                case Restored:
                    compactionStrategy.appendState(columnFamilyInfo);
                    transferState(SchedulerState.Restored, SchedulerState.Running);
                    versionSynchronizerTimer.scheduleAtFixedRate(
                            () -> compactionStrategy.updateVersion(),
                            syncRocksDBInterval,
                            syncRocksDBInterval,
                            TimeUnit.MILLISECONDS);
                    break;
                case OnKeyElimination:
                    break;
                case Dummy:
                    break;
            }
        }
    }

    public void finishRestore(Supplier<Boolean> keyEliminationCallback) {
        transferState(SchedulerState.Uninitialized, SchedulerState.Restored);
        if (isRescaling) {
            Preconditions.checkArgument(
                    !restoreStates.isEmpty(), "There must some states when rescaling");
            if (keyEliminationCallback != null) {
                this.keyEliminationCallback = keyEliminationCallback;
                compactionStrategy.initialRestoreStates(restoreStates);
                if (enableTrivialMoveAfterFileIngestion) {
                    compactionStrategy.initializeTrivialMoveAfterRestoreByFileIngestion();
                }
                compactionStrategy.prepareKeyElimination();
                transferState(SchedulerState.Restored, SchedulerState.OnKeyElimination);
            } else {
                compactionStrategy.initialRestoreStates(restoreStates);
                transferState(SchedulerState.Restored, SchedulerState.Running);
            }
        } else {
            if (!restoreStates.isEmpty()) {
                compactionStrategy.initialRestoreStates(restoreStates);
                transferState(SchedulerState.Restored, SchedulerState.Running);
                versionSynchronizerTimer.scheduleAtFixedRate(
                        () -> compactionStrategy.updateVersion(),
                        syncRocksDBInterval,
                        syncRocksDBInterval,
                        TimeUnit.MILLISECONDS);
            } else {
            }

            if (keyEliminationCallback != null) {
                keyEliminationCallback.get();
            }
        }
    }

    @Override
    @Nullable
    public CompactionRunner pickCompactionFromTask() {
        switch (schedulerState.get()) {
            case Restored:
                return null;
            case Running:
                if (isRescaling && enableTrivialMoveAfterFileIngestion) {
                    // enable trivial move after file ingestion, first pick trivial move compaction
                    CompactionJob trivialMoveCompactionJob = compactionStrategy.createTrivialMoveCompaction();
                    if (trivialMoveCompactionJob != null) {
                        trivialMoveCompactionJob.setTrivialMove(true);
                        return trivialMoveCompactionJob;
                    } else {
                        enableTrivialMoveAfterFileIngestion = false;
                        return null;
                    }
                }

                return compactionStrategy.createCompactionJob();
            case OnKeyElimination:
                CompactionJob compactionJob = compactionStrategy.nextKeyElimination();
                if (!compactionStrategy.hasNextKeyElimination()) {
                    completeKeyElimination();
                }
                return compactionJob;
            default:
                return null;
        }
    }

    @Override
    public void close() throws IOException {
        transferState(schedulerState.get(), SchedulerState.Closed);
        versionSynchronizerTimer.shutdown();
        compactionStrategy = null;
    }

    private void completeKeyElimination() {
        transferState(SchedulerState.OnKeyElimination, SchedulerState.Running);
        versionSynchronizerTimer.scheduleAtFixedRate(
                () -> compactionStrategy.updateVersion(),
                syncRocksDBInterval,
                syncRocksDBInterval,
                TimeUnit.MILLISECONDS);
        keyEliminationCallback.get();
    }

    public void setRescaling(boolean rescaling) {
        isRescaling = rescaling;
    }

    public enum SchedulerState {
        Uninitialized,
        Restored,
        OnKeyElimination,
        Running,
        Closed,
        Dummy
    }

    private boolean transferState(SchedulerState from, SchedulerState to) {
        if (!schedulerState.get().equals(from)) {
            return false;
        } else {
            boolean transferResult = schedulerState.compareAndSet(from, to);
            return transferResult;
        }
    }

    public SchedulerState getSchedulerState() {
        return schedulerState.get();
    }

    public static CompactionScheduler getDummyCompactionScheduler() {
        return new CompactionScheduler();
    }

    public boolean sanityCheckAllFilesAfterKeyElimination() {
        return compactionStrategy.sanityCheckAllFilesAfterKeyElimination();
    }

    public void dumpDB() {
        compactionStrategy.dumpDB();
    }
}
