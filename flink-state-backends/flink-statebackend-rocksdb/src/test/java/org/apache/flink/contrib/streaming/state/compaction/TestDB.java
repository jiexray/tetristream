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
import org.apache.flink.contrib.streaming.state.compaction.util.CompactionInputFiles;
import org.apache.flink.contrib.streaming.state.compaction.util.DBFormat;
import org.apache.flink.contrib.streaming.state.compaction.util.FlinkSstFileMetaData;
import org.apache.flink.contrib.streaming.state.compaction.util.InternalKey;

import org.rocksdb.ColumnFamilyOptions;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Simple Test RocksDB. */
public class TestDB {
    private final ColumnFamilyOptions columnFamilyOptions;

    private VersionStorageInfo vStorage;

    private List<List<FlinkSstFileMetaData>> files;

    public TestDB(ColumnFamilyOptions columnFamilyOptions) {
        this.columnFamilyOptions = columnFamilyOptions;
        files = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            files.add(new ArrayList<>());
        }
    }

    public void add(
            int level,
            int fileNumber,
            String smallest,
            String largest,
            long smallestSeqno,
            long largestSeqno) {
        FlinkSstFileMetaData f =
                new FlinkSstFileMetaData(
                        String.format("%d", fileNumber),
                        "file-path",
                        0,
                        smallestSeqno,
                        largestSeqno,
                        smallest.getBytes(StandardCharsets.UTF_8),
                        largest.getBytes(StandardCharsets.UTF_8),
                        0,
                        false,
                        0,
                        0);
        files.get(level).add(f);
    }

    public void add(
            int level,
            int fileNumber,
            String smallest,
            String largest,
            long smallestSeqno,
            long largestSeqno,
            long fileSize) {
        FlinkSstFileMetaData f =
                new FlinkSstFileMetaData(
                        String.format("%d", fileNumber),
                        "file-path",
                        fileSize,
                        smallestSeqno,
                        largestSeqno,
                        smallest.getBytes(StandardCharsets.UTF_8),
                        largest.getBytes(StandardCharsets.UTF_8),
                        0,
                        false,
                        0,
                        0);
        files.get(level).add(f);
    }

    public void updateVersionStorageInfo() {
        vStorage = new VersionStorageInfo(files, columnFamilyOptions);
        vStorage.computeCompactionScore(columnFamilyOptions);

        files = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            files.add(new ArrayList<>());
        }
    }

    public void installLevelFiles(int level) {
        vStorage.installLevelFiles(level, files.get(level));
    }

    public String getOverlappingInputs(int level, InternalKey begin, InternalKey end) {
        VersionStorageInfo.OverlappingInputContext context =
                new VersionStorageInfo.OverlappingInputContext(
                        level, begin, end, new ArrayList<>());
        vStorage.getOverlappingInputs(context);
        return Arrays.toString(
                context.inputs.stream().map(FlinkSstFileMetaData::fileName).toArray());
    }

    public String expandInputsToCleanCut(CompactionInputFiles inputs) {
        LevelCompactionPicker compactionPicker = new LevelCompactionPicker(columnFamilyOptions);

        compactionPicker.expandInputsToCleanCut("test", vStorage, inputs);
        return Arrays.toString(inputs.files.stream().map(FlinkSstFileMetaData::fileName).toArray());
    }

    public Compaction getCompaction() {
        LevelCompactionPicker compactionPicker = new LevelCompactionPicker(columnFamilyOptions);
        return compactionPicker.pickCompaction(
                "test", columnFamilyOptions, vStorage, DBFormat.MAX_SEQUENCE_NUMBER);
    }

    public Compaction getCompaction(long earliestSeqno) {
        LevelCompactionPicker compactionPicker = new LevelCompactionPicker(columnFamilyOptions);
        return compactionPicker.pickCompaction(
                "test", columnFamilyOptions, vStorage, earliestSeqno);
    }

    public Compaction getCompactionWithInitialFiles(
            List<FlinkSstFileMetaData> initialFiles, int inputLevel) {
        LevelCompactionPicker compactionPicker = new LevelCompactionPicker(columnFamilyOptions);
        return compactionPicker.pickCompactionWithManualInitialFiles(
                "test",
                columnFamilyOptions,
                vStorage,
                DBFormat.MAX_SEQUENCE_NUMBER,
                initialFiles,
                inputLevel);
    }

    public Compaction getTrivialMoveCompaction(int inputLevel) {
        LevelCompactionPicker compactionPicker = new LevelCompactionPicker(columnFamilyOptions);
        return compactionPicker.pickTrivialMoveAtLevel(
                "test",
                columnFamilyOptions,
                vStorage,
                inputLevel);
    }

    public boolean needCompaction() {
        LevelCompactionPicker compactionPicker = new LevelCompactionPicker(columnFamilyOptions);
        return compactionPicker.needsCompaction(vStorage);
    }

    public VersionStorageInfo getvStorage() {
        return vStorage;
    }

    public List<FlinkSstFileMetaData> levelFiles(int level) {
        return files.get(level);
    }
}
