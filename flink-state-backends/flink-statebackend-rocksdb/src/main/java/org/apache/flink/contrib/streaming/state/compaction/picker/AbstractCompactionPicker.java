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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.compaction.Compaction;
import org.apache.flink.contrib.streaming.state.compaction.CompactionPicker;
import org.apache.flink.contrib.streaming.state.compaction.mvcc.VersionStorageInfo;
import org.apache.flink.contrib.streaming.state.compaction.util.CompactionInputFiles;
import org.apache.flink.contrib.streaming.state.compaction.util.DefaultComparator;
import org.apache.flink.contrib.streaming.state.compaction.util.DefaultUserComparator;
import org.apache.flink.contrib.streaming.state.compaction.util.FlinkSstFileMetaData;
import org.apache.flink.contrib.streaming.state.compaction.util.InternalKey;
import org.apache.flink.util.Preconditions;

import org.rocksdb.ColumnFamilyOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class AbstractCompactionPicker implements CompactionPicker {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractCompactionPicker.class);

    protected final ColumnFamilyOptions columnFamilyOptions;

    protected final Set<Compaction> level0CompactionsInProgress;

    protected final Set<Compaction> compactionsInProgress;

    protected final Comparator<InternalKey> icmp;

    protected final Comparator<InternalKey> userComparator;

    public AbstractCompactionPicker(ColumnFamilyOptions columnFamilyOptions) {
        this.columnFamilyOptions = columnFamilyOptions;
        this.level0CompactionsInProgress = new HashSet<>();
        this.compactionsInProgress = new HashSet<>();

        icmp = DefaultComparator.getInstance();
        userComparator = DefaultUserComparator.getInstance();
    }

    @Override
    public int numberLevels() {
        return columnFamilyOptions.numLevels();
    }

    @Override
    public Set<Compaction> getLevel0CompactionsInProgress() {
        return level0CompactionsInProgress;
    }

    @Override
    public boolean expandInputsToCleanCut(
            String cfName, VersionStorageInfo vStorage, CompactionInputFiles inputs) {
        int level = inputs.level;
        if (level == 0) {
            return true;
        }
        int hintIndex = -1;
        int oldSize;
        do {
            oldSize = inputs.size();
            Tuple2<InternalKey, InternalKey> range = getRange(inputs);
            InternalKey smallest = range.f0;
            InternalKey largest = range.f1;
            inputs.clear();
            vStorage.getOverlappingInputs(
                    new VersionStorageInfo.OverlappingInputContext(
                            level, smallest, largest, inputs.files, hintIndex, hintIndex, true));
        } while (inputs.size() > oldSize);

        Preconditions.checkArgument(
                !inputs.empty(), "expandInputsToCleanCut error for state " + cfName);
        if (areFilesInCompaction(inputs.files)) {
            return false;
        }

        return true;
    }

    @Override
    public boolean filesRangeOverlapWithCompaction(List<CompactionInputFiles> inputs, int level) {
        boolean isEmpty = true;

        for (CompactionInputFiles in : inputs) {
            if (!in.empty()) {
                isEmpty = false;
                break;
            }
        }

        if (isEmpty) {
            return false;
        }

        Tuple2<InternalKey, InternalKey> range = getRange(inputs);
        return rangeOverlapWithCompaction(range.f0, range.f1, level);
    }

    @Override
    public boolean getOverlappingL0Files(
            VersionStorageInfo vStorage, CompactionInputFiles startLevelInputs, int outputLevel) {
        Preconditions.checkArgument(level0CompactionsInProgress.isEmpty());
        Tuple2<InternalKey, InternalKey> range = getRange(startLevelInputs);
        startLevelInputs.clear();
        vStorage.getOverlappingInputs(
                new VersionStorageInfo.OverlappingInputContext(
                        0, range.f0, range.f1, startLevelInputs.files));

        range = getRange(startLevelInputs);
        if (isRangeInCompaction(vStorage, range.f0, range.f1, outputLevel)) {
            return false;
        }

        Preconditions.checkArgument(!startLevelInputs.empty());
        return true;
    }

    @Override
    public boolean setupOtherInputs(ExpandInputsContext context) {
        Preconditions.checkArgument(!context.inputs.empty());
        Preconditions.checkArgument(context.outputLevelInputs.empty());
        int inputLevel = context.inputs.level;
        int outputLevel = context.outputLevelInputs.level;
        if (inputLevel == outputLevel) {
            return true;
        }

        for (int l = inputLevel + 1; l < outputLevel; l++) {
            Preconditions.checkArgument(context.vStorage.numLevelFiles(l) == 0);
        }

        Tuple2<InternalKey, InternalKey> range = getRange(context.inputs);

        VersionStorageInfo.OverlappingInputContext overlappingInputContext =
                new VersionStorageInfo.OverlappingInputContext(
                        outputLevel,
                        range.f0,
                        range.f1,
                        context.outputLevelInputs.files,
                        context.parentIndex,
                        context.parentIndex,
                        true);
        context.vStorage.getOverlappingInputs(overlappingInputContext);
        context.parentIndex = overlappingInputContext.fileIndex;

        if (areFilesInCompaction(context.outputLevelInputs.files)) {
            return false;
        }
        if (!context.outputLevelInputs.empty()) {
            if (!expandInputsToCleanCut(
                    context.cfName, context.vStorage, context.outputLevelInputs)) {
                return false;
            }
        }

        if (!context.outputLevelInputs.empty()) {
            long limit = columnFamilyOptions.maxCompactionBytes();
            long outputLevelInputsSize = totalCompensatedFileSize(context.outputLevelInputs.files);
            long inputsSize = totalCompensatedFileSize(context.inputs.files);
            boolean expandInputs = false;

            CompactionInputFiles expandedInputs = new CompactionInputFiles();
            expandedInputs.level = inputLevel;
            Tuple2<InternalKey, InternalKey> allRange =
                    getRange(Arrays.asList(context.inputs, context.outputLevelInputs));
            boolean tryOverlappingInputs = true;
            context.vStorage.getOverlappingInputs(
                    new VersionStorageInfo.OverlappingInputContext(
                            inputLevel,
                            allRange.f0,
                            allRange.f1,
                            expandedInputs.files,
                            context.baseIndex,
                            -1,
                            true));
            long expandedInputSize = totalCompensatedFileSize(expandedInputs.files);
            if (!expandInputsToCleanCut(context.cfName, context.vStorage, expandedInputs)) {
                tryOverlappingInputs = false;
            }
            if (tryOverlappingInputs
                    && expandedInputs.size() > context.inputs.size()
                    && outputLevelInputsSize + expandedInputSize < limit
                    && !areFilesInCompaction(expandedInputs.files)) {

                Tuple2<InternalKey, InternalKey> newRange = getRange(expandedInputs);
                CompactionInputFiles expandedOutputLevelInputs = new CompactionInputFiles();
                expandedOutputLevelInputs.level = outputLevel;
                overlappingInputContext =
                        new VersionStorageInfo.OverlappingInputContext(
                                outputLevel,
                                newRange.f0,
                                newRange.f1,
                                expandedOutputLevelInputs.files,
                                context.parentIndex,
                                context.parentIndex,
                                true);

                context.vStorage.getOverlappingInputs(overlappingInputContext);
                context.parentIndex = overlappingInputContext.fileIndex;

                Preconditions.checkArgument(!expandedOutputLevelInputs.empty());
                if (!areFilesInCompaction(expandedOutputLevelInputs.files)
                        && expandInputsToCleanCut(
                                context.cfName, context.vStorage, expandedOutputLevelInputs)
                        && expandedOutputLevelInputs.size() == context.outputLevelInputs.size()) {

                    expandInputs = true;
                }
            }
            if (!expandInputs) {
                context.vStorage.getCleanInputsWithinInterval(
                        new VersionStorageInfo.OverlappingInputContext(
                                inputLevel,
                                allRange.f0,
                                allRange.f1,
                                expandedInputs.files,
                                context.baseIndex,
                                -1,
                                true));
                expandedInputSize = totalCompensatedFileSize(expandedInputs.files);
                if (expandedInputs.files.size() > context.inputs.size()
                        && outputLevelInputsSize + expandedInputSize < limit
                        && !areFilesInCompaction(expandedInputs.files)) {
                    expandInputs = true;
                }
            }

            if (expandInputs) {
                context.inputs.files = expandedInputs.files;
            }
        }
        return true;
    }

    @Override
    public void registerCompaction(Compaction c) {
        if (c == null) {
            return;
        }

        if (c.getStartLevel() == 0) {
            level0CompactionsInProgress.add(c);
        }
        compactionsInProgress.add(c);
    }

    @Override
    public void unregisterCompaction(Compaction c) {
        if (c == null) {
            return;
        }
        if (c.getStartLevel() == 0) {
            level0CompactionsInProgress.remove(c);
        }
        compactionsInProgress.remove(c);
    }

    private boolean areFilesInCompaction(List<FlinkSstFileMetaData> files) {
        for (int i = 0; i < files.size(); i++) {
            if (files.get(i).beingCompacted()) {
                return true;
            }
        }
        return false;
    }

    private boolean rangeOverlapWithCompaction(
            InternalKey smallest, InternalKey largest, int level) {
        for (Compaction c : compactionsInProgress) {
            if (c.getOutputLevel() == level
                    && userComparator.compare(smallest, c.getLargestKey()) <= 0
                    && userComparator.compare(largest, c.getSmallestKey()) >= 0) {
                return true;
            }
        }
        return false;
    }

    private boolean isRangeInCompaction(
            VersionStorageInfo vStorage, InternalKey smallest, InternalKey largest, int level) {
        List<FlinkSstFileMetaData> inputs = new ArrayList<>();
        Preconditions.checkArgument(level < numberLevels());

        vStorage.getOverlappingInputs(
                new VersionStorageInfo.OverlappingInputContext(level, smallest, largest, inputs));
        return areFilesInCompaction(inputs);
    }

    @Override
    public Tuple2<InternalKey, InternalKey> getRange(CompactionInputFiles inputs) {
        int level = inputs.level;
        Preconditions.checkArgument(!inputs.empty());
        InternalKey smallest = null, largest = null;

        if (level == 0) {
            for (int i = 0; i < inputs.size(); i++) {
                FlinkSstFileMetaData f = inputs.get(i);
                if (i == 0) {
                    smallest = new InternalKey(f.smallestKey(), f.smallestSeqno());
                    largest = new InternalKey(f.largestKey(), f.largestSeqno());
                } else {
                    InternalKey curFileSmallest =
                            new InternalKey(f.smallestKey(), f.smallestSeqno());
                    InternalKey curFileLargest = new InternalKey(f.largestKey(), f.largestSeqno());
                    if (icmp.compare(curFileSmallest, smallest) < 0) {
                        smallest = curFileSmallest;
                    }
                    if (icmp.compare(curFileLargest, largest) > 0) {
                        largest = curFileLargest;
                    }
                }
            }
        } else {
            smallest =
                    new InternalKey(
                            inputs.files.get(0).smallestKey(), inputs.files.get(0).smallestSeqno());
            largest =
                    new InternalKey(
                            inputs.files.get(inputs.size() - 1).largestKey(),
                            inputs.files.get(inputs.size() - 1).largestSeqno());
        }

        return new Tuple2<>(smallest, largest);
    }

    @Override
    public Tuple2<InternalKey, InternalKey> getRange(List<CompactionInputFiles> inputs) {
        InternalKey smallest = null;
        InternalKey largest = null;
        boolean initialized = false;
        for (CompactionInputFiles in : inputs) {
            if (in.empty()) {
                continue;
            }
            Tuple2<InternalKey, InternalKey> currentRange = getRange(in);
            if (!initialized) {
                smallest = currentRange.f0;
                largest = currentRange.f1;
                initialized = true;
            } else {
                if (icmp.compare(currentRange.f0, smallest) < 0) {
                    smallest = currentRange.f0;
                }
                if (icmp.compare(currentRange.f1, largest) > 0) {
                    largest = currentRange.f1;
                }
            }
        }

        Preconditions.checkArgument(initialized);
        return new Tuple2<>(smallest, largest);
    }

    private long totalCompensatedFileSize(final List<FlinkSstFileMetaData> files) {
        long sum = 0L;
        for (int i = 0; i < files.size(); i++) {
            sum += files.get(i).size();
        }
        return sum;
    }
}
