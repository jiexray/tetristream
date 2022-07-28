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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.compaction.Compaction;
import org.apache.flink.contrib.streaming.state.compaction.CompactionPicker;
import org.apache.flink.contrib.streaming.state.compaction.mvcc.VersionStorageInfo;
import org.apache.flink.contrib.streaming.state.compaction.util.CompactionInputFiles;
import org.apache.flink.contrib.streaming.state.compaction.util.FlinkSstFileMetaData;
import org.apache.flink.contrib.streaming.state.compaction.util.InternalKey;
import org.apache.flink.util.Preconditions;

import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionReason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LevelCompactionBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(LevelCompactionBuilder.class);

    private final String cfName;
    private final VersionStorageInfo vStorage;
    private final long earliestMemSeqno;
    private final CompactionPicker compactionPicker;
    private final ColumnFamilyOptions columnFamilyOptions;

    private static final int MinFilesForIntraL0Compaction = 4;

    private int startLevel = -1;
    private int outputLevel = -1;
    private int parentIndex = -1;
    private int baseIndex = -1;
    private double startLevelScore = 0;

    CompactionInputFiles startLevelInputs = new CompactionInputFiles();
    List<CompactionInputFiles> compactionInputs = new ArrayList<>();
    CompactionInputFiles outputLevelInputs = new CompactionInputFiles();
    CompactionReason compactionReason;

    public LevelCompactionBuilder(
            String cfName,
            VersionStorageInfo vStorage,
            long earliestMemSeqno,
            CompactionPicker compactionPicker,
            ColumnFamilyOptions columnFamilyOptions) {

        this.cfName = cfName;
        this.vStorage = vStorage;
        this.earliestMemSeqno = earliestMemSeqno;
        this.compactionPicker = compactionPicker;
        this.columnFamilyOptions = columnFamilyOptions;
    }

    public Compaction pickCompaction() {
        setupInitialFiles();
        if (startLevelInputs.empty()) {
            return null;
        }
        Preconditions.checkArgument(startLevel >= 0 && outputLevel >= 0);

        if (!setupOtherL0FilesIfNeed()) {
            return null;
        }
        if (!setOtherInputsIfNeed()) {
            return null;
        }

        return getCompaction();
    }

    public Compaction pickCompactionWithInitialFiles(
            List<FlinkSstFileMetaData> initialFiles, int inputLevel) {
        startLevelInputs.clear();
        startLevelInputs.level = inputLevel;
        startLevel = inputLevel;
        outputLevel = startLevel + 1;

        for (FlinkSstFileMetaData file : initialFiles) {
            boolean validSstFile = false;
            for (FlinkSstFileMetaData fileInVStorage : vStorage.levelFiles(inputLevel)) {
                if (fileInVStorage == file) {
                    validSstFile = true;
                }
            }
            if (!validSstFile) {
                String errorMsg = "initial file [" + file + "] is not in current vStorage!";
                LOG.error(errorMsg);
                throw new IllegalArgumentException(errorMsg);
            }
            startLevelInputs.files.add(file);
        }

        if (!compactionPicker.expandInputsToCleanCut(cfName, vStorage, startLevelInputs)
                || compactionPicker.filesRangeOverlapWithCompaction(
                Arrays.asList(startLevelInputs), outputLevel)) {

            startLevelInputs.clear();
            return null;
        }

        Tuple2<InternalKey, InternalKey> smallestAndLargest =
                compactionPicker.getRange(startLevelInputs);
        final InternalKey smallest = smallestAndLargest.f0;
        final InternalKey largest = smallestAndLargest.f1;
        CompactionInputFiles outputLevelInputs = new CompactionInputFiles();
        outputLevelInputs.level = outputLevel;
        vStorage.getOverlappingInputs(
                new VersionStorageInfo.OverlappingInputContext(
                        outputLevel, smallest, largest, outputLevelInputs.files));

        if (!outputLevelInputs.empty()
                && !compactionPicker.expandInputsToCleanCut(
                cfName, vStorage, outputLevelInputs)) {

            startLevelInputs.clear();
            return null;
        }

        Preconditions.checkArgument(startLevel >= 0 && outputLevel >= 0);
        if (!setupOtherL0FilesIfNeed()) {
            return null;
        }

        if (!setOtherInputsIfNeed()) {
            return null;
        }

        return getCompaction();
    }

    public Compaction pickTrivialMoveAtLevel(int inputLevel) {
        startLevelInputs.clear();
        startLevelInputs.level = inputLevel;
        startLevel = inputLevel;
        outputLevel = startLevel + 1;

        Preconditions.checkArgument(inputLevel < vStorage.getNumLevels());
        for (FlinkSstFileMetaData file : vStorage.levelFiles(inputLevel)) {
            List<FlinkSstFileMetaData> overlappedFiles = new ArrayList<>();
            VersionStorageInfo.OverlappingInputContext context = new VersionStorageInfo.OverlappingInputContext(
                    inputLevel + 1,
                    new InternalKey(file.smallestKey(), 0L),
                    new InternalKey(file.largestKey(), 0L),
                    overlappedFiles,
                    -1,
                    -1,
                    false);
            vStorage.getOverlappingInputs(context);

            if (overlappedFiles.isEmpty()) {
                startLevelInputs.files.add(file);
            } else {
                if (!startLevelInputs.empty()) {
                    break;
                }
            }
        }

        if (startLevelInputs.empty()) {
            return null;
        } else {
            compactionInputs.add(startLevelInputs);
            return getCompaction();
        }
    }

    private boolean setupOtherL0FilesIfNeed() {
        if (startLevel == 0 && outputLevel != 0) {
            return compactionPicker.getOverlappingL0Files(vStorage, startLevelInputs, outputLevel);
        }
        return true;
    }

    private boolean setOtherInputsIfNeed() {
        if (outputLevel != 0) {
            outputLevelInputs.level = outputLevel;

            if (!compactionPicker.setupOtherInputs(
                    new CompactionPicker.ExpandInputsContext(
                            cfName,
                            columnFamilyOptions,
                            vStorage,
                            startLevelInputs,
                            outputLevelInputs,
                            parentIndex,
                            baseIndex))) {
                return false;
            }

            compactionInputs.add(startLevelInputs);
            if (!outputLevelInputs.empty()) {
                compactionInputs.add(outputLevelInputs);
            }

            if (compactionPicker.filesRangeOverlapWithCompaction(compactionInputs, outputLevel)) {
                return false;
            }

        } else {
            compactionInputs.add(startLevelInputs);
        }
        return true;
    }

    @VisibleForTesting
    public void setupInitialFiles() {
        boolean skippedL0ToBase = false;

        for (int i = 0; i < compactionPicker.numberLevels() - 1; i++) {
            startLevelScore = vStorage.getCompactionScore()[i];
            startLevel = vStorage.getCompactionLevel()[i];

            if (startLevelScore >= 1) {
                if (skippedL0ToBase && startLevel == vStorage.getBaseLevel()) {
                    continue;
                }
                outputLevel = (startLevel == 0) ? vStorage.getBaseLevel() : startLevel + 1;
                if (pickFileToCompact()) {
                    if (startLevel == 0) {
                        compactionReason = CompactionReason.kLevelL0FilesNum;
                    } else {
                        compactionReason = CompactionReason.kLevelMaxLevelSize;
                    }
                    break;
                } else {
                    startLevelInputs.clear();
                    if (startLevel == 0) {
                        skippedL0ToBase = true;

                        if (pickIntraL0Compaction()) {
                            outputLevel = 0;
                            compactionReason = CompactionReason.kLevelL0FilesNum;
                            break;
                        }
                    }
                }
            }
        }

        if (!startLevelInputs.empty()) {
            return;
        }

        parentIndex = baseIndex = -1;
    }

    private Compaction getCompaction() {
        Compaction c =
                new Compaction(
                        vStorage,
                        columnFamilyOptions,
                        compactionInputs,
                        outputLevel,
                        columnFamilyOptions.targetFileSizeBase(),
                        columnFamilyOptions.maxCompactionBytes(),
                        0,
                        startLevelScore,
                        CompactionReason.kManualCompaction);
        return c;
    }

    @VisibleForTesting
    public boolean pickFileToCompact() {
        if (startLevel == 0 && !compactionPicker.getLevel0CompactionsInProgress().isEmpty()) {
            return false;
        }

        startLevelInputs.clear();

        Preconditions.checkArgument(startLevel >= 0);

        final List<Integer> fileSize = vStorage.getFilesByCompactionPri().get(startLevel);
        final List<FlinkSstFileMetaData> levelFiles = vStorage.getFiles().get(startLevel);

        int cmpIdx;
        for (cmpIdx = vStorage.getNextFileToCompactBySize().get(startLevel);
                cmpIdx < fileSize.size();
                cmpIdx++) {
            int index = fileSize.get(cmpIdx);
            FlinkSstFileMetaData f = levelFiles.get(index);

            if (f.beingCompacted()) {
                continue;
            }

            startLevelInputs.files.add(f);
            startLevelInputs.level = startLevel;

            if (!compactionPicker.expandInputsToCleanCut(cfName, vStorage, startLevelInputs)
                    || compactionPicker.filesRangeOverlapWithCompaction(
                            Arrays.asList(startLevelInputs), outputLevel)) {

                startLevelInputs.clear();
                continue;
            }

            Tuple2<InternalKey, InternalKey> smallestAndLargest =
                    compactionPicker.getRange(startLevelInputs);
            final InternalKey smallest = smallestAndLargest.f0;
            final InternalKey largest = smallestAndLargest.f1;
            CompactionInputFiles outputLevelInputs = new CompactionInputFiles();
            outputLevelInputs.level = outputLevel;
            vStorage.getOverlappingInputs(
                    new VersionStorageInfo.OverlappingInputContext(
                            outputLevel, smallest, largest, outputLevelInputs.files));

            if (!outputLevelInputs.empty()
                    && !compactionPicker.expandInputsToCleanCut(
                            cfName, vStorage, outputLevelInputs)) {

                startLevelInputs.clear();
                continue;
            }
            baseIndex = index;
            break;
        }

        vStorage.setNextCompactionIndex(startLevel, cmpIdx);
        return startLevelInputs.size() > 0;
    }

    @VisibleForTesting
    public boolean pickIntraL0Compaction() {
        startLevelInputs.clear();
        List<FlinkSstFileMetaData> levelFiles = vStorage.getFiles().get(0);
        if (levelFiles.size() < columnFamilyOptions.level0FileNumCompactionTrigger() + 2
                || levelFiles.get(0).beingCompacted()) {
            return false;
        }
        return CompactionPicker.findIntraL0Compaction(
                levelFiles,
                MinFilesForIntraL0Compaction,
                Long.MAX_VALUE,
                columnFamilyOptions.maxCompactionBytes(),
                startLevelInputs,
                earliestMemSeqno);
    }
}
