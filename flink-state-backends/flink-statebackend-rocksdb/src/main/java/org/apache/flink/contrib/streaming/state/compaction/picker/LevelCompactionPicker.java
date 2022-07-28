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

package org.apache.flink.contrib.streaming.state.compaction.picker;

import org.apache.flink.contrib.streaming.state.compaction.Compaction;
import org.apache.flink.contrib.streaming.state.compaction.mvcc.VersionStorageInfo;
import org.apache.flink.contrib.streaming.state.compaction.util.DBFormat;
import org.apache.flink.contrib.streaming.state.compaction.util.FlinkSstFileMetaData;

import org.rocksdb.ColumnFamilyOptions;

import java.util.List;

public class LevelCompactionPicker extends AbstractCompactionPicker {
    public LevelCompactionPicker(ColumnFamilyOptions columnFamilyOptions) {
        super(columnFamilyOptions);
    }

    @Override
    public boolean needsCompaction(VersionStorageInfo vStorage) {
        for (int i = 0; i < vStorage.maxInputLevel(); i++) {
            if (vStorage.getCompactionScore()[i] >= 1) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Compaction pickCompaction(
            String columnFamilyName,
            ColumnFamilyOptions columnFamilyOptions,
            VersionStorageInfo vStorage,
            long earliestMemSeqno) {

        LevelCompactionBuilder builder =
                new LevelCompactionBuilder(
                        columnFamilyName, vStorage, earliestMemSeqno, this, columnFamilyOptions);
        return builder.pickCompaction();
    }

    @Override
    public Compaction pickCompactionWithManualInitialFiles(
            String columnFamilyName,
            ColumnFamilyOptions columnFamilyOptions,
            VersionStorageInfo vStorage,
            long earliestMemSeqno,
            List<FlinkSstFileMetaData> initialFiles,
            int inputLevel) {
        LevelCompactionBuilder builder =
                new LevelCompactionBuilder(
                        columnFamilyName, vStorage, earliestMemSeqno, this, columnFamilyOptions);
        return builder.pickCompactionWithInitialFiles(initialFiles, inputLevel);
    }

    @Override
    public Compaction pickTrivialMoveAtLevel(
            String columnFamilyName,
            ColumnFamilyOptions columnFamilyOptions,
            VersionStorageInfo vStorage,
            int inputLevel) {
        LevelCompactionBuilder builder =
                new LevelCompactionBuilder(
                        columnFamilyName, vStorage, DBFormat.MAX_SEQUENCE_NUMBER, this, columnFamilyOptions);
        return builder.pickTrivialMoveAtLevel(inputLevel);
    }
}
