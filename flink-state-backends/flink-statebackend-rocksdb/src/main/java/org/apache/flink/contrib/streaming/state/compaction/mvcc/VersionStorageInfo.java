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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.contrib.streaming.state.compaction.util.BinarySearchUtils;
import org.apache.flink.contrib.streaming.state.compaction.util.DefaultComparator;
import org.apache.flink.contrib.streaming.state.compaction.util.DefaultUserComparator;
import org.apache.flink.contrib.streaming.state.compaction.util.FlinkSstFileMetaData;
import org.apache.flink.contrib.streaming.state.compaction.util.InternalKey;
import org.apache.flink.util.Preconditions;

import org.rocksdb.ColumnFamilyMetaData;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.LevelMetaData;
import org.rocksdb.SstFileMetaData;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.function.BiFunction;

public class VersionStorageInfo {
    private final Comparator<InternalKey> comparator;
    private final Comparator<InternalKey> userComparator;

    private int numLevels;

    private int numNonEmptyLevels;

    private long[] levelMaxBytes;
    private int baseLevel;

    private final List<List<FlinkSstFileMetaData>> files;

    private List<List<Integer>> filesByCompactionPri;
    private List<Integer> nextFileToCompactBySize;

    private double[] compactionScore;
    private int[] compactionLevel;

    public VersionStorageInfo(
            ColumnFamilyMetaData columnFamilyMetaData, ColumnFamilyOptions options) {
        this(
                columnFamilyMetaData,
                options,
                DefaultComparator.getInstance(),
                DefaultUserComparator.getInstance());
    }

    public VersionStorageInfo(
            ColumnFamilyMetaData columnFamilyMetaData,
            ColumnFamilyOptions options,
            Comparator<InternalKey> comparator,
            Comparator<InternalKey> userComparator) {
        this.comparator = comparator;
        this.userComparator = userComparator;

        this.numLevels = 0;
        for (LevelMetaData levelMetaData : columnFamilyMetaData.levels()) {
            numLevels = Math.max(numLevels, levelMetaData.level() + 1);
        }

        this.files = new ArrayList<>(this.numLevels);
        for (LevelMetaData levelMetaData : columnFamilyMetaData.levels()) {
            List<SstFileMetaData> sstFilesMetaData = levelMetaData.files();

            List<FlinkSstFileMetaData> flinkSstFilesMetadata = new ArrayList<>();
            for (SstFileMetaData file : sstFilesMetaData) {
                flinkSstFilesMetadata.add(new FlinkSstFileMetaData(file));
            }
            files.add(flinkSstFilesMetadata);
        }

        prepareApply(options);
    }

    @VisibleForTesting
    public VersionStorageInfo(List<List<FlinkSstFileMetaData>> files, ColumnFamilyOptions options) {
        this.comparator = DefaultComparator.getInstance();
        this.userComparator = DefaultUserComparator.getInstance();
        this.numLevels = files.size();

        this.files = files;

        prepareApply(options);
    }

    private void prepareApply(ColumnFamilyOptions options) {
        updateNumNonEmptyLevels();
        calculateBaseBytes(options);
        updateFilesByCompactionPri();
    }

    private void updateFilesByCompactionPri() {
        filesByCompactionPri = new ArrayList<>(numLevels - 1);
        nextFileToCompactBySize = new ArrayList<>(numLevels - 1);
        for (int level = 0; level < numLevels - 1; level++) {
            final List<FlinkSstFileMetaData> levelFiles = files.get(level);
            List<Integer> levelFilesByCompactionPri = new ArrayList<>(levelFiles.size());
            filesByCompactionPri.add(level, levelFilesByCompactionPri);

            List<Fsize> temp = new ArrayList<>(levelFiles.size());
            for (int i = 0; i < levelFiles.size(); i++) {
                temp.add(i, new Fsize(i, levelFiles.get(i)));
            }
            Collections.sort(temp);

            for (int i = 0; i < temp.size(); i++) {
                levelFilesByCompactionPri.add(i, temp.get(i).index);
            }
            nextFileToCompactBySize.add(level, 0);
        }
    }

    private void updateNumNonEmptyLevels() {
        numNonEmptyLevels = numLevels;
        for (int i = numLevels - 1; i >= 0; i--) {
            if (files.get(i).size() != 0) {
                return;
            } else {
                numNonEmptyLevels = i;
            }
        }
    }

    public void computeCompactionScore(ColumnFamilyOptions columnFamilyOptions) {
        compactionLevel = new int[maxInputLevel() + 1];
        compactionScore = new double[maxInputLevel() + 1];
        for (int level = 0; level <= maxInputLevel(); level++) {
            double score;
            if (level == 0) {
                int numSortedRuns = 0;
                long totalSize = 0L;
                for (FlinkSstFileMetaData f : files.get(level)) {
                    if (!f.beingCompacted()) {
                        numSortedRuns++;
                        totalSize += f.size();
                    }
                }

                score =
                        ((double) numSortedRuns)
                                / (double) columnFamilyOptions.level0FileNumCompactionTrigger();
                if (numLevels > 1) {
                    score =
                            Math.max(
                                    score,
                                    ((double) totalSize)
                                            / (double) columnFamilyOptions.maxBytesForLevelBase());
                }
            } else {
                long levelBytesNoCompaction = 0L;
                for (FlinkSstFileMetaData f : files.get(level)) {
                    if (!f.beingCompacted()) {
                        levelBytesNoCompaction += f.size();
                    }
                }
                score = ((double) levelBytesNoCompaction) / (double) maxBytesForLevel(level);
            }
            compactionLevel[level] = level;
            compactionScore[level] = score;
        }

        for (int i = 0; i < numLevels - 2; i++) {
            for (int j = i + 1; j < numLevels - 1; j++) {
                if (compactionScore[i] < compactionScore[j]) {
                    double score = compactionScore[i];
                    int level = compactionLevel[i];
                    compactionScore[i] = compactionScore[j];
                    compactionLevel[i] = compactionLevel[j];
                    compactionScore[j] = score;
                    compactionLevel[j] = level;
                }
            }
        }
    }

    public void installLevelFiles(int level, List<FlinkSstFileMetaData> newLevelFiles) {
        Preconditions.checkArgument(level >= 0);
        Preconditions.checkArgument(level < files.size());
        files.set(level, newLevelFiles);

        updateNumNonEmptyLevels();

        if (level <= maxInputLevel()) {
            List<Integer> levelFilesByCompactionPri = filesByCompactionPri.get(level);
            levelFilesByCompactionPri.clear();

            List<Fsize> temp = new ArrayList<>(newLevelFiles.size());
            for (int i = 0; i < newLevelFiles.size(); i++) {
                temp.add(i, new Fsize(i, newLevelFiles.get(i)));
            }
            Collections.sort(temp);

            for (int i = 0; i < temp.size(); i++) {
                levelFilesByCompactionPri.add(i, temp.get(i).index);
            }
            nextFileToCompactBySize.set(level, 0);
        }
    }

    public int getNumLevels() {
        return numLevels;
    }

    public long[] getLevelMaxBytes() {
        return levelMaxBytes;
    }

    public int getBaseLevel() {
        return baseLevel;
    }

    public List<List<FlinkSstFileMetaData>> getFiles() {
        return files;
    }

    public List<List<Integer>> getFilesByCompactionPri() {
        return filesByCompactionPri;
    }

    public List<Integer> getNextFileToCompactBySize() {
        return nextFileToCompactBySize;
    }

    public void setNextCompactionIndex(int level, int index) {
        nextFileToCompactBySize.set(level, index);
    }

    public double[] getCompactionScore() {
        return compactionScore;
    }

    public int[] getCompactionLevel() {
        return compactionLevel;
    }

    public int numLevelFiles(int level) {
        return files.get(level).size();
    }

    public Comparator<InternalKey> getUserComparator() {
        return userComparator;
    }

    public long maxBytesForLevel(int level) {
        Preconditions.checkArgument(level >= 0);
        Preconditions.checkArgument(level < levelMaxBytes.length);
        return this.levelMaxBytes[level];
    }

    public List<FlinkSstFileMetaData> levelFiles(int level) {
        return this.files.get(level);
    }

    public int nextCompactionIndex(int level) {
        return nextFileToCompactBySize.get(level);
    }

    public boolean hasFileLeftToCompactionForKeyElimination() {
        for (int level = 0; level < numLevels; level++) {
            List<FlinkSstFileMetaData> levelFiles = levelFiles(level);
            for (FlinkSstFileMetaData file : levelFiles) {
                if (file.isToCompactByKeyElimination()) {
                    return true;
                }
            }
        }
        return false;
    }

    public int maxInputLevel() {
        return numLevels - 2;
    }

    void calculateBaseBytes(ColumnFamilyOptions options) {
        levelMaxBytes = new long[options.numLevels()];
        baseLevel = 1;

        for (int i = 0; i < options.numLevels(); i++) {
            if (i > 1) {
                levelMaxBytes[i] =
                        levelMaxBytes[i - 1] * (long) options.maxBytesForLevelMultiplier();
            } else {
                levelMaxBytes[i] = options.maxBytesForLevelBase();
            }
        }
    }

    public void getOverlappingInputs(OverlappingInputContext context) {
        if (context.level >= numNonEmptyLevels) {
            return;
        }

        context.inputs.clear();
        context.fileIndex = -1;

        if (context.level > 0) {
            getOverlappingInputsRangeBinarySearch(context, false);
            return;
        }

        InternalKey userBegin = null;
        InternalKey userEnd = null;
        if (context.begin != null) {
            userBegin = context.begin;
        }
        if (context.end != null) {
            userEnd = context.end;
        }

        List<Integer> index = new LinkedList<>();
        for (int i = 0; i < files.get(context.level).size(); i++) {
            index.add(i);
        }

        while (!index.isEmpty()) {
            boolean foundOverlapFile = false;
            Iterator<Integer> iter = index.iterator();
            while (iter.hasNext()) {
                int current = iter.next();

                InternalKey fileStart =
                        new InternalKey(
                                files.get(context.level).get(current).smallestKey(),
                                files.get(context.level).get(current).smallestSeqno());
                InternalKey fileLimit =
                        new InternalKey(
                                files.get(context.level).get(current).largestKey(),
                                files.get(context.level).get(current).largestSeqno());
                if (context.begin != null && userComparator.compare(fileLimit, userBegin) < 0) {
                } else if (context.end != null && userComparator.compare(fileStart, userEnd) > 0) {
                } else {
                    context.inputs.add(files.get(context.level).get(current));
                    foundOverlapFile = true;
                    if (context.fileIndex == -1) {
                        context.fileIndex = current;
                    }
                    iter.remove();
                    if (context.expandRange) {
                        if (context.begin != null
                                && userComparator.compare(fileStart, userBegin) < 0) {
                            userBegin = fileStart;
                        }
                        if (context.end != null && userComparator.compare(fileLimit, userEnd) > 0) {
                            userEnd = fileLimit;
                        }
                    }
                }
            }

            if (!foundOverlapFile) {
                break;
            }
        }
    }

    private void getOverlappingInputsRangeBinarySearch(
            OverlappingInputContext context, boolean withinInterval) {
        List<FlinkSstFileMetaData> levelFiles = files.get(context.level);
        final int numFiles = levelFiles.size();

        int startIndex = 0;
        int endIndex = numFiles;

        if (context.begin != null) {
            BiFunction<FlinkSstFileMetaData, InternalKey, Boolean> cmp =
                    (file, key) -> {
                        InternalKey fileKey =
                                withinInterval
                                        ? new InternalKey(file.smallestKey(), file.smallestSeqno())
                                        : new InternalKey(file.largestKey(), file.largestSeqno());
                        return userComparator.compare(fileKey, key) < 0;
                    };

            startIndex =
                    BinarySearchUtils.lowerBound(
                            levelFiles,
                            0,
                            (context.hintIndex == -1 ? numFiles : context.hintIndex),
                            context.begin,
                            cmp);

            if (startIndex > 0 && withinInterval) {
                boolean isOverlapping = true;
                while (isOverlapping && startIndex < numFiles) {
                    InternalKey preLimit =
                            new InternalKey(
                                    levelFiles.get(startIndex - 1).largestKey(),
                                    levelFiles.get(startIndex - 1).largestSeqno());
                    InternalKey curStart =
                            new InternalKey(
                                    levelFiles.get(startIndex).smallestKey(),
                                    levelFiles.get(startIndex).smallestSeqno());
                    isOverlapping = userComparator.compare(preLimit, curStart) == 0;
                    startIndex += (isOverlapping ? 1 : 0);
                }
            }
        }

        if (context.end != null) {
            BiFunction<FlinkSstFileMetaData, InternalKey, Boolean> cmp =
                    (file, key) -> {
                        InternalKey fileKey =
                                withinInterval
                                        ? new InternalKey(file.largestKey(), file.largestSeqno())
                                        : new InternalKey(file.smallestKey(), file.smallestSeqno());
                        return userComparator.compare(key, fileKey) < 0;
                    };

            endIndex =
                    BinarySearchUtils.upperBound(
                            levelFiles, startIndex, numFiles, context.end, cmp);

            if (endIndex < numFiles && withinInterval) {
                boolean isOverlapping = true;
                while (isOverlapping && endIndex > startIndex) {
                    InternalKey nextStart =
                            new InternalKey(
                                    levelFiles.get(endIndex).smallestKey(),
                                    levelFiles.get(endIndex).smallestSeqno());
                    InternalKey curLimit =
                            new InternalKey(
                                    levelFiles.get(endIndex - 1).largestKey(),
                                    levelFiles.get(endIndex - 1).largestSeqno());
                    isOverlapping = userComparator.compare(nextStart, curLimit) == 0;
                    endIndex -= (isOverlapping ? 1 : 0);
                }
            }
        }

        Preconditions.checkArgument(startIndex <= endIndex);
        if (startIndex == endIndex) {
            return;
        }

        context.fileIndex = startIndex;

        for (int i = startIndex; i < endIndex; i++) {
            context.inputs.add(levelFiles.get(i));
        }
    }

    public static class OverlappingInputContext {
        public final int level;
        public final InternalKey begin;
        public final InternalKey end;
        public final List<FlinkSstFileMetaData> inputs;
        public final int hintIndex;
        public int fileIndex;
        public final boolean expandRange;

        public OverlappingInputContext(
                int level,
                InternalKey begin,
                InternalKey end,
                List<FlinkSstFileMetaData> inputs,
                int hintIndex,
                int fileIndex,
                boolean expandRange) {
            this.level = level;
            this.begin = begin;
            this.end = end;
            this.inputs = inputs;
            this.hintIndex = hintIndex;
            this.fileIndex = fileIndex;
            this.expandRange = expandRange;
        }

        public OverlappingInputContext(
                int level, InternalKey begin, InternalKey end, List<FlinkSstFileMetaData> inputs) {
            this.level = level;
            this.begin = begin;
            this.end = end;
            this.inputs = inputs;
            this.hintIndex = -1;
            this.fileIndex = -1;
            this.expandRange = true;
        }
    }

    public void getCleanInputsWithinInterval(OverlappingInputContext context) {
        context.inputs.clear();
        context.fileIndex = -1;
        if (context.level >= numNonEmptyLevels
                || context.level == 0
                || files.get(context.level).size() == 0) {
            return;
        }
        getOverlappingInputsRangeBinarySearch(context, true);
    }

    private class Fsize implements Comparable<Fsize> {
        final int index;
        final FlinkSstFileMetaData file;

        public Fsize(int index, FlinkSstFileMetaData file) {
            this.index = index;
            this.file = file;
        }

        @Override
        public int compareTo(Fsize o) {
            if (o.file.size() == this.file.size()) {
                return 0;
            }
            return o.file.size() - this.file.size() > 0 ? 1 : -1;
        }
    }

    @Override
    public String toString() {
        return "VersionStorageInfo{"
                + "numLevels="
                + numLevels
                + ", numNonEmptyLevels="
                + numNonEmptyLevels
                + ", levelMaxBytes="
                + Arrays.toString(levelMaxBytes)
                + ", baseLevel="
                + baseLevel
                + ", files="
                + files
                + ", compactionScore="
                + Arrays.toString(compactionScore)
                + ", compactionLevel="
                + Arrays.toString(compactionLevel)
                + '}';
    }

    public void dumpCurrentDB() {
        System.out.println("===================================================");
        for(int level = 0; level < files.size(); level++) {
            System.out.printf("Level %d (%d): %s\n", level, files.get(level).size(), files.get(level));
        }
        System.out.println("===================================================");
    }

    public void dumpSstFile(String fileName) {
        System.out.println("===================================================");
        for(int level = 0; level < files.size(); level++) {
            for (FlinkSstFileMetaData file : files.get(level)) {
                if (file.fileName().equals(fileName)) {
                    System.out.printf("File: %s, level: %d, smallestkey: %s, largestkey: %s\n",
                            file.fileName(), level,
                            Arrays.toString(file.smallestKey()),
                            Arrays.toString(file.largestKey()));
                }
            }
        }
        System.out.println("===================================================");
    }
}
