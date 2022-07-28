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
import org.apache.flink.contrib.streaming.state.compaction.util.CompactionInputFiles;
import org.apache.flink.contrib.streaming.state.compaction.util.FlinkSstFileMetaData;
import org.apache.flink.contrib.streaming.state.compaction.util.InternalKey;
import org.apache.flink.util.Preconditions;

import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionReason;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class Compaction {
    private final int startLevel;
    private final int outputLevel;

    private final long maxOutputFileSize;
    private final long maxCompactionBytes;
    private final int numberLevels;
    private final int outputPathId;
    private final double score;

    private final List<CompactionInputFiles> inputs;
    private final VersionStorageInfo vStorage;
    ColumnFamilyOptions columnFamilyOptions;

    private InternalKey smallestKey;
    private InternalKey largestKey;
    private final CompactionReason compactionReason;

    public Compaction(
            VersionStorageInfo vStorage,
            ColumnFamilyOptions columnFamilyOptions,
            List<CompactionInputFiles> inputs,
            int outputLevel,
            long targetFileSize,
            long maxCompactionBytes,
            int outputPathId,
            double score,
            CompactionReason compactionReason) {

        this.vStorage = vStorage;
        this.startLevel = inputs.get(0).level;
        this.outputLevel = outputLevel;
        this.outputPathId = outputPathId;

        this.inputs = inputs;

        this.maxOutputFileSize = targetFileSize;
        this.maxCompactionBytes = maxCompactionBytes;
        this.numberLevels = vStorage.getNumLevels();
        this.score = score;

        this.columnFamilyOptions = columnFamilyOptions;

        this.compactionReason = compactionReason;

        getBoundaryKeys();
    }

    public int getStartLevel() {
        return startLevel;
    }

    public int getOutputLevel() {
        return outputLevel;
    }

    public InternalKey getSmallestKey() {
        return smallestKey;
    }

    public InternalKey getLargestKey() {
        return largestKey;
    }

    public CompactionReason getCompactionReason() {
        return compactionReason;
    }

    public int numInputLevels() {
        return inputs.size();
    }

    public int numInputFiles(int compactionInputLevel) {
        if (compactionInputLevel < inputs.size()) {
            return inputs.get(compactionInputLevel).size();
        }
        return 0;
    }

    public FlinkSstFileMetaData input(int compactionInputLevel, int i) {
        Preconditions.checkArgument(compactionInputLevel < inputs.size());
        return inputs.get(compactionInputLevel).get(i);
    }

    public List<FlinkSstFileMetaData> inputs(int compactionInputLevel) {
        Preconditions.checkArgument(compactionInputLevel < inputs.size());
        return inputs.get(compactionInputLevel).files;
    }

    public List<CompactionInputFiles> getInputs() {
        return inputs;
    }

    public double getScore() {
        return score;
    }

    public VersionStorageInfo getvStorage() {
        return vStorage;
    }

    public void markFilesBeingCompaction(boolean markAsCompacted) {
        for (int i = 0; i < numInputLevels(); i++) {
            for (int j = 0; j < inputs.get(i).size(); j++) {
                Preconditions.checkArgument(
                        markAsCompacted != inputs.get(i).get(j).beingCompacted());
                inputs.get(i).get(j).setBeingCompacted(markAsCompacted);
            }
        }
    }

    private void getBoundaryKeys() {
        boolean initialized = false;
        Comparator<InternalKey> ucmp = vStorage.getUserComparator();
        for (int i = 0; i < inputs.size(); i++) {
            if (inputs.get(i).empty()) {
                continue;
            }
            if (inputs.get(i).level == 0) {
                // we need to consider all files on level 0
                for (FlinkSstFileMetaData f : inputs.get(i).files) {
                    InternalKey startUserKey = new InternalKey(f.smallestKey(), f.smallestSeqno());
                    if (!initialized || ucmp.compare(startUserKey, smallestKey) < 0) {
                        this.smallestKey = startUserKey;
                    }

                    InternalKey endUserKey = new InternalKey(f.largestKey(), f.largestSeqno());
                    if (!initialized || ucmp.compare(endUserKey, largestKey) > 0) {
                        this.largestKey = endUserKey;
                    }
                    initialized = true;
                }

            } else {
                InternalKey startUserKey =
                        new InternalKey(
                                inputs.get(i).files.get(0).smallestKey(),
                                inputs.get(i).files.get(0).smallestSeqno());
                if (!initialized || ucmp.compare(startUserKey, smallestKey) < 0) {
                    this.smallestKey = startUserKey;
                }

                InternalKey endUserKey =
                        new InternalKey(
                                inputs.get(i).files.get(0).largestKey(),
                                inputs.get(i).files.get(0).largestSeqno());
                if (!initialized || ucmp.compare(endUserKey, largestKey) > 0) {
                    this.largestKey = endUserKey;
                }
                initialized = true;
            }
        }
    }

    @Override
    public String toString() {
        List<String> files = new ArrayList<>();
        for (CompactionInputFiles input : inputs) {
            files.addAll(
                    input.files.stream()
                            .map(FlinkSstFileMetaData::fileName)
                            .collect(Collectors.toList()));
        }
        return "Compaction{"
                + "startLevel="
                + startLevel
                + ", outputLevel="
                + outputLevel
                + ", files=["
                + files
                + "]}";
    }
}
