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

import org.apache.flink.contrib.streaming.state.compaction.util.ColumnFamilyInfo;
import org.apache.flink.contrib.streaming.state.compaction.util.CompactionInputFiles;
import org.apache.flink.contrib.streaming.state.compaction.util.FlinkSstFileMetaData;
import org.apache.flink.runtime.state.rocksdb.CompactionJobMetrics;
import org.apache.flink.runtime.state.rocksdb.CompactionRunner;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.CompactionJobInfo;
import org.rocksdb.CompactionOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class CompactionJob implements CompactionRunner {
    private static final Logger LOG = LoggerFactory.getLogger(CompactionJob.class);

    private final ColumnFamilyInfo columnFamilyInfo;
    private final Compaction compaction;
    private final CompactionPicker compactionPicker;
    private final BiConsumer<CompactionJobInfo, ColumnFamilyInfo> finishCallback;
    private boolean isCompactionForKeyElimination;
    private boolean isTrivialMove;

    public CompactionJob(
            ColumnFamilyInfo columnFamilyInfo,
            Compaction compaction,
            BiConsumer<CompactionJobInfo, ColumnFamilyInfo> finishCallback) {

        this.columnFamilyInfo = columnFamilyInfo;
        this.compaction = compaction;
        this.compactionPicker = columnFamilyInfo.getCompactionPicker();

        compaction.markFilesBeingCompaction(true);

        compactionPicker.registerCompaction(compaction);

        compaction.getvStorage().computeCompactionScore(columnFamilyInfo.getColumnFamilyOptions());

        this.finishCallback = finishCallback;
        this.isCompactionForKeyElimination = false;
    }

    private static final CompactionOptions compactionOptions =
            new CompactionOptions().setCompression(CompressionType.NO_COMPRESSION);

    static {
        compactionOptions.setOutputFileSizeLimit(64 * 1024 * 1024L); // 64 * 1024 (M) * 1024 (K) B
    }

    public CompactionJobMetrics runCompaction() throws Exception {
        RocksDB db = columnFamilyInfo.getDb();
        ColumnFamilyHandle stateHandle = columnFamilyInfo.getStateHandle();
        String stateName = columnFamilyInfo.getStateName();

        List<String> files = new ArrayList<>();
        for (CompactionInputFiles input : compaction.getInputs()) {
            files.addAll(
                    input.files.stream()
                            .map(FlinkSstFileMetaData::fileName)
                            .collect(Collectors.toList()));
        }

        long startTs = System.currentTimeMillis();

        CompactionJobInfo compactionJobInfo = new CompactionJobInfo();
        try {
            if (!isCompactionForKeyElimination) {
                compactionOptions.setTrivialMoveIgnoreCompactionFilter(true);
            } else {
                compactionOptions.setTrivialMoveIgnoreCompactionFilter(false);
            }

            db.compactFiles(
                    compactionOptions,
                    stateHandle,
                    files,
                    compaction.getOutputLevel(),
                    0,
                    compactionJobInfo);

            Set<String> expectedInputFiles = new HashSet<>(files);
            Set<String> actualInputFiles = new HashSet<>();
            List<String> outputFiles = new LinkedList<>();
            for (String filePath : compactionJobInfo.inputFiles()) {
                String simpleFilePath = filePath.substring(filePath.lastIndexOf("/"));
                actualInputFiles.add(simpleFilePath);
            }

            List<String> fileNotInExpected = new ArrayList<>();
            for (String actualInputFile : actualInputFiles) {
                if (!expectedInputFiles.contains(actualInputFile)) {
                    fileNotInExpected.add(actualInputFile);
                }
            }

            List<String> fileNotInActual = new ArrayList<>();
            for (String expectInputFile : expectedInputFiles) {
                if (!actualInputFiles.contains(expectInputFile)) {
                    fileNotInActual.add(expectInputFile);
                }
            }
            if (!fileNotInActual.isEmpty() || !fileNotInExpected.isEmpty()) {
                compaction.getvStorage().dumpCurrentDB();

                for (String fileName : fileNotInActual) {
                    compaction.getvStorage().dumpSstFile(fileName);
                }

                for (String fileName : fileNotInExpected) {
                    compaction.getvStorage().dumpSstFile(fileName);
                }

                for (CompactionInputFiles inputFiles : compaction.getInputs()) {
                    for (FlinkSstFileMetaData file : inputFiles.files) {
                        compaction.getvStorage().dumpSstFile(file.fileName());
                    }
                }
            }

            for (String filePath : compactionJobInfo.outputFiles()) {
                outputFiles.add(filePath.substring(filePath.lastIndexOf("/")));
            }

            if (compactionJobInfo.status().getCode().equals(Status.Code.Ok)) {
                finishCallback.accept(compactionJobInfo, columnFamilyInfo);
            }

        } catch (RocksDBException e) {
            throw e;
        }

        long endTs = System.currentTimeMillis();
        return new CompactionJobMetrics(endTs - startTs, isCompactionForKeyElimination);
    }

    @Override
    public String toString() {
        return "CmpJob{"
                + "state: "
                + columnFamilyInfo.getStateName()
                + ", cmp:"
                + compaction
                + '}';
    }

    @Override
    public String getStateName() {
        return columnFamilyInfo.getStateName();
    }

    @Override
    public boolean isCompactionForKeyElimination() {
        return isCompactionForKeyElimination;
    }

    public void setCompactionForKeyElimination(boolean compactionForKeyElimination) {
        isCompactionForKeyElimination = compactionForKeyElimination;
    }

    @Override
    public boolean isTrivialMove() {
        return isTrivialMove;
    }

    public void setTrivialMove(boolean trivialMove) {
        isTrivialMove = trivialMove;
    }

    @Override
    public void close() throws IOException {
        compactionPicker.unregisterCompaction(compaction);
    }

    @Override
    public double getCompactionScore() {
        return compaction.getScore();
    }
}
