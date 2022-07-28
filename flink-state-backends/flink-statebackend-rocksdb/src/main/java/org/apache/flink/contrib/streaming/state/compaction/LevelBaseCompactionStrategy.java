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

import org.apache.flink.contrib.streaming.state.compaction.mvcc.VersionStorageInfo;
import org.apache.flink.contrib.streaming.state.compaction.picker.LevelCompactionPicker;
import org.apache.flink.contrib.streaming.state.compaction.util.ColumnFamilyInfo;
import org.apache.flink.contrib.streaming.state.compaction.util.DBFormat;
import org.apache.flink.contrib.streaming.state.compaction.util.FlinkSstFileMetaData;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public class LevelBaseCompactionStrategy extends AbstractCompactionStrategy {

    public LevelBaseCompactionStrategy() {
        super();
    }

    @Override
    protected Compaction generateCompaction(
            ColumnFamilyInfo columnFamily, VersionStorageInfo vStorage) {
        CompactionPicker compactionPickerForColumnFamily = columnFamily.getCompactionPicker();
        if (!compactionPickerForColumnFamily.needsCompaction(vStorage)) {
            return null;
        }
        return compactionPickerForColumnFamily.pickCompaction(
                columnFamily.getStateName(),
                columnFamily.getColumnFamilyOptions(),
                vStorage,
                DBFormat.MAX_SEQUENCE_NUMBER);
    }

    @Override
    protected CompactionJob pickCompaction(Map<ColumnFamilyInfo, Compaction> candidateCompactions) {
        if (candidateCompactions.isEmpty()) {
            return null;
        }

        BiFunction<Compaction, Compaction, Integer> cmp =
                (c1, c2) -> {
                    if (Double.compare(c1.getScore(), c2.getScore()) != 0) {
                        return c1.getScore() - c2.getScore() > 0 ? 1 : -1;
                    }

                    if (c1.getStartLevel() != c2.getStartLevel()) {
                        return c1.getStartLevel() - c2.getStartLevel() < 0 ? 1 : -1;
                    }
                    return 0;
                };

        Iterator<Map.Entry<ColumnFamilyInfo, Compaction>> iter =
                candidateCompactions.entrySet().iterator();
        Map.Entry<ColumnFamilyInfo, Compaction> result = iter.next();

        while (iter.hasNext()) {
            Map.Entry<ColumnFamilyInfo, Compaction> nextCompaction = iter.next();
            if (cmp.apply(nextCompaction.getValue(), result.getValue()) > 0) {
                result = nextCompaction;
            }
        }

        return new CompactionJob(
                result.getKey(), result.getValue(), this::updateVersionStorageAfterCompaction);
    }

    @Override
    protected Compaction generateCompactionForKeyElimination(
            ColumnFamilyInfo columnFamily, VersionStorageInfo vStorage) {
        List<List<FlinkSstFileMetaData>> files = vStorage.getFiles();
        CompactionPicker compactionPicker = columnFamily.getCompactionPicker();

        int inputLevel = -1;
        FlinkSstFileMetaData initialFile = null;
        for (int level = 0; level < files.size(); level++) {
            List<FlinkSstFileMetaData> levelFiles = files.get(level);
            for (FlinkSstFileMetaData file : levelFiles) {
                if (file.isToCompactByKeyElimination() && !file.beingCompacted()) {
                    inputLevel = level;
                    initialFile = file;
                    break;
                }
            }
            if (inputLevel != -1) {
                break;
            }
        }
        if (inputLevel != -1) {
            return compactionPicker.pickCompactionWithManualInitialFiles(
                    columnFamily.getStateName(),
                    columnFamily.getColumnFamilyOptions(),
                    vStorage,
                    DBFormat.MAX_SEQUENCE_NUMBER,
                    Collections.singletonList(initialFile),
                    inputLevel);
        } else {
            return null;
        }
    }

    @Override
    protected Compaction generateTrivialMoveAtLevel(
            ColumnFamilyInfo columnFamily,
            VersionStorageInfo vStorage,
            int level) {
        List<List<FlinkSstFileMetaData>> files = vStorage.getFiles();
        CompactionPicker compactionPicker = columnFamily.getCompactionPicker();

        Preconditions.checkArgument(level >= 1 && level < files.size());

        if (files.get(level).isEmpty()) {
            return null;
        } else {
            return compactionPicker.pickTrivialMoveAtLevel(
                    columnFamily.getStateName(),
                    columnFamily.getColumnFamilyOptions(),
                    vStorage,
                    level);
        }
    }
}
