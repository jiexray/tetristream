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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.compaction.mvcc.VersionStorageInfo;
import org.apache.flink.contrib.streaming.state.compaction.util.CompactionInputFiles;
import org.apache.flink.contrib.streaming.state.compaction.util.DBFormat;
import org.apache.flink.contrib.streaming.state.compaction.util.FlinkSstFileMetaData;
import org.apache.flink.contrib.streaming.state.compaction.util.InternalKey;

import org.rocksdb.ColumnFamilyOptions;

import java.util.List;
import java.util.Set;

public interface CompactionPicker {
    boolean needsCompaction(final VersionStorageInfo vStorage);

    Compaction pickCompaction(
            String columnFamilyName,
            ColumnFamilyOptions columnFamilyOptions,
            VersionStorageInfo vStorage,
            long earliestMemSeqno);

    Compaction pickCompactionWithManualInitialFiles(
            String columnFamilyName,
            ColumnFamilyOptions columnFamilyOptions,
            VersionStorageInfo vStorage,
            long earliestMemSeqno,
            List<FlinkSstFileMetaData> initialFiles,
            int inputLevel);

    Compaction pickTrivialMoveAtLevel(
            String columnFamilyName,
            ColumnFamilyOptions columnFamilyOptions,
            VersionStorageInfo vStorage,
            int inputLevel);

    int numberLevels();

    Set<Compaction> getLevel0CompactionsInProgress();

    boolean expandInputsToCleanCut(
            String cfName, VersionStorageInfo vStorage, CompactionInputFiles inputs);

    boolean filesRangeOverlapWithCompaction(List<CompactionInputFiles> inputs, int level);

    Tuple2<InternalKey, InternalKey> getRange(CompactionInputFiles inputs);

    Tuple2<InternalKey, InternalKey> getRange(List<CompactionInputFiles> inputs);

    static boolean findIntraL0Compaction(
            List<FlinkSstFileMetaData> levelFiles,
            int minFilesToCompact,
            long maxCompactBytesPerDelFiles,
            long maxCompactionBytes,
            CompactionInputFiles compInputs) {
        return findIntraL0Compaction(
                levelFiles,
                minFilesToCompact,
                maxCompactBytesPerDelFiles,
                maxCompactionBytes,
                compInputs,
                DBFormat.MAX_SEQUENCE_NUMBER);
    }

    static boolean findIntraL0Compaction(
            List<FlinkSstFileMetaData> levelFiles,
            int minFilesToCompact,
            long maxCompactBytesPerDelFiles,
            long maxCompactionBytes,
            CompactionInputFiles compInputs,
            long earliestMemSeqno) {
        int start = 0;
        for (; start < levelFiles.size(); start++) {
            if (levelFiles.get(start).beingCompacted()) {
                return false;
            }
            if (levelFiles.get(start).largestSeqno() <= earliestMemSeqno) {
                break;
            }
        }
        if (start >= levelFiles.size()) {
            return false;
        }

        long compactBytes = levelFiles.get(start).size();
        long compactBytesPerDelFile = Long.MAX_VALUE;

        int limit;
        long newCompactBytesPerDelFile = 0;

        for (limit = start + 1; limit < levelFiles.size(); ++limit) {
            compactBytes += levelFiles.get(limit).size();
            newCompactBytesPerDelFile = (compactBytes) / (limit - start);
            if (levelFiles.get(limit).beingCompacted()
                    || compactBytes > maxCompactionBytes
                    || newCompactBytesPerDelFile > compactBytesPerDelFile) {
                break;
            }
            compactBytesPerDelFile = newCompactBytesPerDelFile;
        }

        if ((limit - start) >= minFilesToCompact
                && compactBytesPerDelFile < maxCompactBytesPerDelFiles) {
            compInputs.level = 0;
            for (int i = start; i < limit; i++) {
                compInputs.files.add(levelFiles.get(i));
            }
            return true;
        }
        return false;
    }

    boolean getOverlappingL0Files(
            VersionStorageInfo vStorage, CompactionInputFiles startLevelInputs, int outputLevel);

    boolean setupOtherInputs(ExpandInputsContext context);

    class ExpandInputsContext {
        public final String cfName;
        public final ColumnFamilyOptions columnFamilyOptions;
        public final VersionStorageInfo vStorage;
        public final CompactionInputFiles inputs;
        public final CompactionInputFiles outputLevelInputs;
        public int parentIndex;
        public final int baseIndex;

        public ExpandInputsContext(
                String cfName,
                ColumnFamilyOptions columnFamilyOptions,
                VersionStorageInfo vStorage,
                CompactionInputFiles inputs,
                CompactionInputFiles outputLevelInputs,
                int parentIndex,
                int baseIndex) {

            this.cfName = cfName;
            this.columnFamilyOptions = columnFamilyOptions;
            this.vStorage = vStorage;
            this.inputs = inputs;
            this.outputLevelInputs = outputLevelInputs;
            this.parentIndex = parentIndex;
            this.baseIndex = baseIndex;
        }
    }

    void registerCompaction(Compaction c);

    void unregisterCompaction(Compaction c);
}
