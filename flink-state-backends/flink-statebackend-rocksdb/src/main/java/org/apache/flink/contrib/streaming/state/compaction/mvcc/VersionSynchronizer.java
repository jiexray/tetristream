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

package org.apache.flink.contrib.streaming.state.compaction.mvcc;

import org.apache.flink.contrib.streaming.state.compaction.util.ColumnFamilyInfo;
import org.apache.flink.contrib.streaming.state.compaction.util.FlinkSstFileMetaData;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.util.Preconditions;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyMetaData;
import org.rocksdb.RocksDB;
import org.rocksdb.SstFileMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;

import static org.apache.flink.contrib.streaming.state.compaction.AbstractCompactionStrategy.getKeyGroup;

public abstract class VersionSynchronizer {
    protected static final Logger LOG = LoggerFactory.getLogger(VersionSynchronizer.class);

    protected final List<ColumnFamilyInfo> columnFamilyInfos;

    protected Map<ColumnFamilyInfo, VersionStorageInfo> currentVersion;

    public VersionSynchronizer() {
        this.columnFamilyInfos = new CopyOnWriteArrayList<>();
        this.currentVersion = new ConcurrentHashMap<>();
    }

    public void initializeRestoredStates(List<ColumnFamilyInfo> columnFamilyInfos) {
        for (ColumnFamilyInfo columnFamilyInfo : columnFamilyInfos) {
            currentVersion.put(columnFamilyInfo, initVStorage(columnFamilyInfo));
        }

        this.columnFamilyInfos.addAll(columnFamilyInfos);
    }

    public void appendState(ColumnFamilyInfo columnFamilyInfo) {
        currentVersion.put(columnFamilyInfo, initVStorage(columnFamilyInfo));

        this.columnFamilyInfos.add(columnFamilyInfo);
    }

    private VersionStorageInfo initVStorage(ColumnFamilyInfo columnFamilyInfo) {
        RocksDB db = columnFamilyInfo.getDb();
        ColumnFamilyHandle stateHandle = columnFamilyInfo.getStateHandle();
        ColumnFamilyMetaData cfmd = db.getColumnFamilyMetaData(stateHandle);

        VersionStorageInfo initialVersionStorage =
                new VersionStorageInfo(cfmd, columnFamilyInfo.getColumnFamilyOptions());
        initialVersionStorage.computeCompactionScore(columnFamilyInfo.getColumnFamilyOptions());
        return initialVersionStorage;
    }

    public void updateColumnFamily(
            ColumnFamilyInfo columnFamilyInfo, int level, List<FlinkSstFileMetaData> newFiles) {
        VersionStorageInfo vStorage = currentVersion.get(columnFamilyInfo);
        Preconditions.checkArgument(vStorage != null);
        Preconditions.checkArgument(level <= vStorage.getNumLevels());

        List<FlinkSstFileMetaData> oldFiles = vStorage.levelFiles(level);
        Map<String, FlinkSstFileMetaData> oldFilesIndex = new HashMap<>(oldFiles.size());

        for (FlinkSstFileMetaData file : oldFiles) {
            oldFilesIndex.put(file.fileName(), file);
        }

        for (FlinkSstFileMetaData file : newFiles) {
            FlinkSstFileMetaData oldFile = oldFilesIndex.get(file.fileName());
            if (oldFile != null) {
                file.setBeingCompacted(oldFile.beingCompacted());
                file.setToCompactByKeyElimination(oldFile.isToCompactByKeyElimination());
            } else {
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

        vStorage.installLevelFiles(level, newFiles);

        vStorage.computeCompactionScore(columnFamilyInfo.getColumnFamilyOptions());
    }

    public Map<ColumnFamilyInfo, VersionStorageInfo> getCurrentVersion() {
        return currentVersion;
    }

    public abstract void syncRocksDBAndUpdateVersion(
            BiConsumer<Map<Integer, List<SstFileMetaData>>, ColumnFamilyInfo> syncCallback);
}
