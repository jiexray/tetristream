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

import org.apache.flink.contrib.streaming.state.compaction.mvcc.PullBasedVersionSynchronizer;
import org.apache.flink.contrib.streaming.state.compaction.mvcc.VersionStorageInfo;
import org.apache.flink.contrib.streaming.state.compaction.mvcc.VersionSynchronizer;
import org.apache.flink.contrib.streaming.state.compaction.util.ColumnFamilyInfo;
import org.apache.flink.contrib.streaming.state.compaction.util.FlinkSstFileMetaData;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.util.Preconditions;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyMetaData;
import org.rocksdb.CompactionJobInfo;
import org.rocksdb.RocksDB;
import org.rocksdb.SstFileMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractCompactionStrategy implements CompactionStrategy {
    protected static final Logger LOG = LoggerFactory.getLogger(AbstractCompactionStrategy.class);

    public AbstractCompactionStrategy() {

        this.versionSynchronizer = new PullBasedVersionSynchronizer();

        this.versionLock = new Object();
    }

    protected VersionSynchronizer versionSynchronizer;

    protected final Object versionLock;


    @Override
    public void dumpDB() {
        synchronized (versionLock) {
            Map<ColumnFamilyInfo, VersionStorageInfo> currentVersion =
                    versionSynchronizer.getCurrentVersion();
            for (Map.Entry<ColumnFamilyInfo, VersionStorageInfo> entry :
                    currentVersion.entrySet()) {
                VersionStorageInfo vStorage = entry.getValue();
                ColumnFamilyInfo columnFamilyInfo = entry.getKey();

                System.out.printf("=============== %s ===============\n", columnFamilyInfo.getStateName());
                vStorage.dumpCurrentDB();
                System.out.println("===========================================");
            }
        }
    }

    @Override
    public void initialRestoreStates(List<ColumnFamilyInfo> restoreStates) {
        versionSynchronizer.initializeRestoredStates(restoreStates);
    }

    @Override
    public void appendState(ColumnFamilyInfo newState) {
        versionSynchronizer.appendState(newState);
    }

    @Override
    public CompactionJob createCompactionJob() {
        synchronized (versionLock) {
            Map<ColumnFamilyInfo, Compaction> candidateCompactions = new HashMap<>();
            Map<ColumnFamilyInfo, VersionStorageInfo> currentVersion =
                    versionSynchronizer.getCurrentVersion();
            for (Map.Entry<ColumnFamilyInfo, VersionStorageInfo> entry :
                    currentVersion.entrySet()) {
                Compaction compaction = generateCompaction(entry.getKey(), entry.getValue());

                if (compaction != null) {
                    candidateCompactions.put(entry.getKey(), compaction);
                }
            }

            return pickCompaction(candidateCompactions);
        }
    }

    protected abstract Compaction generateCompaction(
            ColumnFamilyInfo columnFamily, VersionStorageInfo vStorage);

    @Nullable
    protected abstract CompactionJob pickCompaction(
            Map<ColumnFamilyInfo, Compaction> candidateCompactions);

    @Override
    public void updateVersion() {
        versionSynchronizer.syncRocksDBAndUpdateVersion(this::updateVersionStorageAfterSyncRocksDB);
    }

    protected void updateVersionStorageAfterCompaction(
            CompactionJobInfo compactionJobInfo, ColumnFamilyInfo columnFamilyInfo) {
        synchronized (versionLock) {
            Preconditions.checkArgument(compactionJobInfo != null);
            RocksDB db = columnFamilyInfo.getDb();
            ColumnFamilyHandle stateHandle = columnFamilyInfo.getStateHandle();
            ColumnFamilyMetaData cfmd = db.getColumnFamilyMetaData(stateHandle);

            int outputLevel = compactionJobInfo.outputLevel();
            int inputLevel = compactionJobInfo.baseInputLevel();

            List<SstFileMetaData> inputLevelFiles = cfmd.levels().get(inputLevel).files();
            List<SstFileMetaData> outputLevelFiles = cfmd.levels().get(outputLevel).files();

            List<FlinkSstFileMetaData> inputLevelFilesMeta =
                    new ArrayList<>(inputLevelFiles.size());
            for (SstFileMetaData file : inputLevelFiles) {
                inputLevelFilesMeta.add(new FlinkSstFileMetaData(file));
            }
            versionSynchronizer.updateColumnFamily(
                    columnFamilyInfo, inputLevel, inputLevelFilesMeta);

            List<FlinkSstFileMetaData> outputLevelFilesMeta =
                    new ArrayList<>(outputLevelFiles.size());
            for (SstFileMetaData file : outputLevelFiles) {
                outputLevelFilesMeta.add(new FlinkSstFileMetaData(file));
            }
            versionSynchronizer.updateColumnFamily(
                    columnFamilyInfo, outputLevel, outputLevelFilesMeta);
        }
    }

    protected void updateVersionStorageAfterSyncRocksDB(
            Map<Integer, List<SstFileMetaData>> files, ColumnFamilyInfo columnFamilyInfo) {
        synchronized (versionLock) {
            for (Map.Entry<Integer, List<SstFileMetaData>> entry : files.entrySet()) {
                int level = entry.getKey();
                List<SstFileMetaData> levelFiles = entry.getValue();
                List<FlinkSstFileMetaData> levelFilesMeta = new ArrayList<>(levelFiles.size());

                for (SstFileMetaData file : levelFiles) {
                    levelFilesMeta.add(new FlinkSstFileMetaData(file));
                }
                versionSynchronizer.updateColumnFamily(columnFamilyInfo, level, levelFilesMeta);
            }
        }
    }

    @Override
    public void initializeTrivialMoveAfterRestoreByFileIngestion() {
        Map<ColumnFamilyInfo, VersionStorageInfo> currentVersion =
                versionSynchronizer.getCurrentVersion();

        for (Map.Entry<ColumnFamilyInfo, VersionStorageInfo> entry :
                currentVersion.entrySet()) {
            ColumnFamilyInfo columnFamilyInfo = entry.getKey();
            VersionStorageInfo vStorage = entry.getValue();

            for (int level = vStorage.getNumLevels() - 1; level >= 0; level--) {
                if (!vStorage.levelFiles(level).isEmpty()) {
                    columnFamilyInfo.setMaxNonEmptyLevelAfterFileIngestion(level);
                    break;
                }
            }
        }
    }

    @Override
    public CompactionJob createTrivialMoveCompaction() {
        synchronized (versionLock) {
            Map<ColumnFamilyInfo, VersionStorageInfo> currentVersion =
                    versionSynchronizer.getCurrentVersion();
            for (Map.Entry<ColumnFamilyInfo, VersionStorageInfo> entry :
                    currentVersion.entrySet()) {
                ColumnFamilyInfo columnFamilyInfo = entry.getKey();
                VersionStorageInfo vStorage = entry.getValue();

                Preconditions.checkArgument(columnFamilyInfo.getMaxNonEmptyLevelAfterFileIngestion() != -1);

                for (int level = columnFamilyInfo.getMaxNonEmptyLevelAfterFileIngestion(); level >= 1; level--) {
                    Compaction trivialMoveCompaction = generateTrivialMoveAtLevel(columnFamilyInfo, vStorage, level);
                    if (trivialMoveCompaction != null) {
                        return pickCompaction(Collections.singletonMap(columnFamilyInfo, trivialMoveCompaction));
                    }
                    columnFamilyInfo.setMaxNonEmptyLevelAfterFileIngestion(level - 1);
                }
            }
            return null;
        }
    }

    @Override
    public void prepareKeyElimination() {
        synchronized (versionLock) {
            Map<ColumnFamilyInfo, VersionStorageInfo> currentVersion =
                    versionSynchronizer.getCurrentVersion();
            for (Map.Entry<ColumnFamilyInfo, VersionStorageInfo> entry :
                    currentVersion.entrySet()) {
                ColumnFamilyInfo columnFamilyInfo = entry.getKey();
                VersionStorageInfo vStorage = entry.getValue();

                for (int level = 0; level < vStorage.getNumLevels(); level++) {
                    List<FlinkSstFileMetaData> levelFiles = vStorage.levelFiles(level);
                    for (FlinkSstFileMetaData file : levelFiles) {
                        int keyGroupPrefixBytes = columnFamilyInfo.getKeyGroupPrefixBytes();
                        KeyGroupRange keyGroupRange = columnFamilyInfo.getKeyGroupRange();

                        int smallestKeyGroup = getKeyGroup(file.smallestKey(), keyGroupPrefixBytes);
                        int largestKeyGroup = getKeyGroup(file.largestKey(), keyGroupPrefixBytes);

                        if (keyGroupRange.contains(smallestKeyGroup)
                                && keyGroupRange.contains(largestKeyGroup)) {

                        } else {
                            file.setToCompactByKeyElimination(true);
                        }
                    }
                }
            }
        }
    }

    @Override
    public boolean sanityCheckAllFilesAfterKeyElimination() {
        synchronized (versionLock) {
            Map<ColumnFamilyInfo, VersionStorageInfo> currentVersion =
                    versionSynchronizer.getCurrentVersion();
            for (Map.Entry<ColumnFamilyInfo, VersionStorageInfo> entry :
                    currentVersion.entrySet()) {
                ColumnFamilyInfo columnFamilyInfo = entry.getKey();
                VersionStorageInfo vStorage = entry.getValue();

                for (int level = 0; level < vStorage.getNumLevels(); level++) {
                    List<FlinkSstFileMetaData> levelFiles = vStorage.levelFiles(level);
                    for (FlinkSstFileMetaData file : levelFiles) {
                        int keyGroupPrefixBytes = columnFamilyInfo.getKeyGroupPrefixBytes();
                        KeyGroupRange keyGroupRange = columnFamilyInfo.getKeyGroupRange();

                        int smallestKeyGroup = getKeyGroup(file.smallestKey(), keyGroupPrefixBytes);
                        int largestKeyGroup = getKeyGroup(file.largestKey(), keyGroupPrefixBytes);

                        if (keyGroupRange.contains(smallestKeyGroup)
                                && keyGroupRange.contains(largestKeyGroup)) {
                        } else {
                            return false;
                        }
                    }
                }
            }
        }
        return true;
    }

    public static int getKeyGroup(byte[] key, int keyGroupPrefixBytes) {
        int keyGroup = 0;
        for (int i = 0; i < keyGroupPrefixBytes; ++i) {
            keyGroup <<= 8;
            keyGroup |= (key[i] & 0xFF);
        }
        return keyGroup;
    }

    @Override
    public boolean hasNextKeyElimination() {
        synchronized (versionLock) {
            Map<ColumnFamilyInfo, VersionStorageInfo> currentVersion =
                    versionSynchronizer.getCurrentVersion();
            for (Map.Entry<ColumnFamilyInfo, VersionStorageInfo> entry :
                    currentVersion.entrySet()) {
                if (entry.getValue().hasFileLeftToCompactionForKeyElimination()) {
                    return true;
                }
            }
            return false;
        }
    }

    @Override
    public CompactionJob nextKeyElimination() {
        synchronized (versionLock) {
            Map<ColumnFamilyInfo, Compaction> candidateCompactions = new HashMap<>();
            Map<ColumnFamilyInfo, VersionStorageInfo> currentVersion =
                    versionSynchronizer.getCurrentVersion();
            for (Map.Entry<ColumnFamilyInfo, VersionStorageInfo> entry :
                    currentVersion.entrySet()) {
                Compaction compaction =
                        generateCompactionForKeyElimination(entry.getKey(), entry.getValue());

                if (compaction != null) {
                    candidateCompactions.put(entry.getKey(), compaction);
                }
            }

            CompactionJob compactionJob = pickCompaction(candidateCompactions);
            if (compactionJob != null) {
                compactionJob.setCompactionForKeyElimination(true);
            }
            return compactionJob;
        }
    }

    protected abstract Compaction generateCompactionForKeyElimination(
            ColumnFamilyInfo columnFamily, VersionStorageInfo vStorage);

    protected abstract Compaction generateTrivialMoveAtLevel(
            ColumnFamilyInfo columnFamily, VersionStorageInfo vStorage, int level);
}
