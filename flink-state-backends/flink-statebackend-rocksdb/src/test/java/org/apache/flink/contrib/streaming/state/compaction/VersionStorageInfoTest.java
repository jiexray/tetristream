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
import org.apache.flink.contrib.streaming.state.compaction.util.FlinkSstFileMetaData;
import org.apache.flink.contrib.streaming.state.compaction.util.InternalKey;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/** Test for VersionStorageInfo. */
public class VersionStorageInfoTest {

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void basicVersionStorageTest() throws IOException, RocksDBException {
        File dbPath = temporaryFolder.newFolder("db");
        ColumnFamilyOptions columnFamilyOptions =
                RocksDBCompactionTestUtils.createDefaultColumnFamilyOption();
        DBOptions dbOptions = RocksDBCompactionTestUtils.createDefaultDBOptions();
        RocksDB db =
                RocksDBCompactionTestUtils.createDB(
                        columnFamilyOptions, dbOptions, dbPath.getAbsolutePath());
        WriteOptions writeOptions = RocksDBCompactionTestUtils.defaultWriteOptions();

        for (int i = 0; i <= 99; i++) {
            db.put(
                    writeOptions,
                    RocksDBCompactionTestUtils.key(i).getBytes(StandardCharsets.UTF_8),
                    String.format("%d", i).getBytes(StandardCharsets.UTF_8));
        }
        db.flush(new FlushOptions());

        VersionStorageInfo vStorage =
                new VersionStorageInfo(db.GetColumnFamilyMetaData(), columnFamilyOptions);

        int numLevel = vStorage.getNumLevels();
        List<FlinkSstFileMetaData> level0Files = vStorage.getFiles().get(0);
        Assert.assertEquals(numLevel, 7);
        Assert.assertEquals(level0Files.size(), 1);
        Assert.assertArrayEquals(
                level0Files.get(0).smallestKey(),
                RocksDBCompactionTestUtils.key(0).getBytes(StandardCharsets.UTF_8));
        Assert.assertArrayEquals(
                level0Files.get(0).largestKey(),
                RocksDBCompactionTestUtils.key(99).getBytes(StandardCharsets.UTF_8));

        for (int i = 100; i <= 199; i++) {
            db.put(
                    writeOptions,
                    RocksDBCompactionTestUtils.key(i).getBytes(StandardCharsets.UTF_8),
                    String.format("%d", i).getBytes(StandardCharsets.UTF_8));
        }
        db.flush(new FlushOptions());

        vStorage = new VersionStorageInfo(db.GetColumnFamilyMetaData(), columnFamilyOptions);
        level0Files = vStorage.getFiles().get(0);
        Assert.assertEquals(level0Files.size(), 2);
        Assert.assertTrue(level0Files.get(1).smallestSeqno() > level0Files.get(0).largestSeqno());
        Assert.assertArrayEquals(
                level0Files.get(1).smallestKey(),
                RocksDBCompactionTestUtils.key(0).getBytes(StandardCharsets.UTF_8));
        Assert.assertArrayEquals(
                level0Files.get(1).largestKey(),
                RocksDBCompactionTestUtils.key(99).getBytes(StandardCharsets.UTF_8));
        Assert.assertArrayEquals(
                level0Files.get(0).smallestKey(),
                RocksDBCompactionTestUtils.key(100).getBytes(StandardCharsets.UTF_8));
        Assert.assertArrayEquals(
                level0Files.get(0).largestKey(),
                RocksDBCompactionTestUtils.key(199).getBytes(StandardCharsets.UTF_8));

        db.close();
    }

    @Test
    public void compactionScoreTest() throws IOException, RocksDBException {
        File dbPath = temporaryFolder.newFolder("db");
        ColumnFamilyOptions columnFamilyOptions =
                RocksDBCompactionTestUtils.createDefaultColumnFamilyOption();
        DBOptions dbOptions = RocksDBCompactionTestUtils.createDefaultDBOptions();
        RocksDB db =
                RocksDBCompactionTestUtils.createDB(
                        columnFamilyOptions, dbOptions, dbPath.getAbsolutePath());
        WriteOptions writeOptions = RocksDBCompactionTestUtils.defaultWriteOptions();

        for (int i = 0; i <= 9; i++) {
            db.put(
                    writeOptions,
                    RocksDBCompactionTestUtils.key(i).getBytes(StandardCharsets.UTF_8),
                    String.format("%d", i).getBytes(StandardCharsets.UTF_8));
        }
        db.flush(new FlushOptions());

        for (int i = 0; i <= 99; i++) {
            db.put(
                    writeOptions,
                    RocksDBCompactionTestUtils.key(i).getBytes(StandardCharsets.UTF_8),
                    String.format("%d", i).getBytes(StandardCharsets.UTF_8));
        }
        db.flush(new FlushOptions());

        for (int i = 0; i <= 999; i++) {
            db.put(
                    writeOptions,
                    RocksDBCompactionTestUtils.key(i).getBytes(StandardCharsets.UTF_8),
                    String.format("%d", i).getBytes(StandardCharsets.UTF_8));
        }
        db.flush(new FlushOptions());

        VersionStorageInfo vStorage =
                new VersionStorageInfo(db.GetColumnFamilyMetaData(), columnFamilyOptions);
        vStorage.computeCompactionScore(columnFamilyOptions);

        List<List<Integer>> filesByCompactionPri = vStorage.getFilesByCompactionPri();
        List<Integer> level0FilesByCompactionPri = filesByCompactionPri.get(0);
        List<FlinkSstFileMetaData> level0Files = vStorage.getFiles().get(0);

        for (int i = 0; i < level0FilesByCompactionPri.size(); i++) {
            Assert.assertTrue(level0FilesByCompactionPri.get(i) < level0Files.size());
            FlinkSstFileMetaData curFile = level0Files.get(level0FilesByCompactionPri.get(i));
            for (int j = i + 1; j < level0FilesByCompactionPri.size(); j++) {
                Assert.assertTrue(level0FilesByCompactionPri.get(j) < level0Files.size());
                FlinkSstFileMetaData smallerFile =
                        level0Files.get(level0FilesByCompactionPri.get(j));
                Assert.assertTrue(curFile.size() >= smallerFile.size());
            }
        }

        double[] compactionScore = vStorage.getCompactionScore();
        int[] compactionLevel = vStorage.getCompactionLevel();
        Assert.assertEquals(compactionScore.length, vStorage.getNumLevels() - 1);
        Assert.assertEquals(compactionLevel.length, vStorage.getNumLevels() - 1);

        Assert.assertEquals(compactionLevel[0], 0);
        Assert.assertEquals(compactionScore[0], 1.0, 0.000001);

        for (int i = 0; i <= 999; i++) {
            db.put(
                    writeOptions,
                    RocksDBCompactionTestUtils.key(i).getBytes(StandardCharsets.UTF_8),
                    String.format("%d", i).getBytes(StandardCharsets.UTF_8));
        }
        db.flush(new FlushOptions());
        vStorage = new VersionStorageInfo(db.GetColumnFamilyMetaData(), columnFamilyOptions);
        vStorage.computeCompactionScore(columnFamilyOptions);
        Assert.assertEquals(vStorage.getCompactionScore()[0], 4.0 / 3.0, 0.0000001);
    }

    @Test
    public void compactionScoreTest2() throws IOException, RocksDBException {
        File dbPath = temporaryFolder.newFolder("db");
        ColumnFamilyOptions columnFamilyOptions =
                RocksDBCompactionTestUtils.createDefaultColumnFamilyOption();
        columnFamilyOptions.setMaxBytesForLevelBase(10000);
        DBOptions dbOptions = RocksDBCompactionTestUtils.createDefaultDBOptions();
        RocksDB db =
                RocksDBCompactionTestUtils.createDB(
                        columnFamilyOptions, dbOptions, dbPath.getAbsolutePath());
        WriteOptions writeOptions = RocksDBCompactionTestUtils.defaultWriteOptions();

        for (int i = 0; i <= 99; i++) {
            db.put(
                    writeOptions,
                    RocksDBCompactionTestUtils.key(i).getBytes(StandardCharsets.UTF_8),
                    String.format("%d", i).getBytes(StandardCharsets.UTF_8));
        }
        db.flush(new FlushOptions());

        for (int i = 100; i <= 199; i++) {
            db.put(
                    writeOptions,
                    RocksDBCompactionTestUtils.key(i).getBytes(StandardCharsets.UTF_8),
                    String.format("%d", i).getBytes(StandardCharsets.UTF_8));
        }
        db.flush(new FlushOptions());

        for (int i = 200; i <= 299; i++) {
            db.put(
                    writeOptions,
                    RocksDBCompactionTestUtils.key(i).getBytes(StandardCharsets.UTF_8),
                    String.format("%d", i).getBytes(StandardCharsets.UTF_8));
        }
        db.flush(new FlushOptions());

        db.compactRange();

        VersionStorageInfo vStorage =
                new VersionStorageInfo(db.GetColumnFamilyMetaData(), columnFamilyOptions);
        List<List<FlinkSstFileMetaData>> files = vStorage.getFiles();

        Assert.assertEquals(files.get(0).size(), 0);
        Assert.assertEquals(files.get(1).size(), 3);
        vStorage.computeCompactionScore(columnFamilyOptions);

        long fileSize = files.get(1).stream().mapToLong(FlinkSstFileMetaData::size).sum();
        Assert.assertEquals(vStorage.getCompactionLevel()[0], 1);
        Assert.assertEquals(
                vStorage.getCompactionScore()[0], ((double) fileSize) / (10000), 0.0000001);

        for (int i = 200; i <= 399; i++) {
            db.put(
                    writeOptions,
                    RocksDBCompactionTestUtils.key(i).getBytes(StandardCharsets.UTF_8),
                    String.format("%d", i).getBytes(StandardCharsets.UTF_8));
        }
        db.flush(new FlushOptions());

        for (int i = 400; i <= 499; i++) {
            db.put(
                    writeOptions,
                    RocksDBCompactionTestUtils.key(i).getBytes(StandardCharsets.UTF_8),
                    String.format("%d", i).getBytes(StandardCharsets.UTF_8));
        }
        db.flush(new FlushOptions());

        vStorage = new VersionStorageInfo(db.GetColumnFamilyMetaData(), columnFamilyOptions);
        vStorage.computeCompactionScore(columnFamilyOptions);
        List<List<Integer>> filesByCompactionPri = vStorage.getFilesByCompactionPri();

        for (int k = 0; k < filesByCompactionPri.size(); k++) {
            List<Integer> level0FilesByCompactionPri = filesByCompactionPri.get(k);
            List<FlinkSstFileMetaData> level0Files = vStorage.getFiles().get(k);

            for (int i = 0; i < level0FilesByCompactionPri.size(); i++) {
                Assert.assertTrue(level0FilesByCompactionPri.get(i) < level0Files.size());
                FlinkSstFileMetaData curFile = level0Files.get(level0FilesByCompactionPri.get(i));
                for (int j = i + 1; j < level0FilesByCompactionPri.size(); j++) {
                    Assert.assertTrue(level0FilesByCompactionPri.get(j) < level0Files.size());
                    FlinkSstFileMetaData smallerFile =
                            level0Files.get(level0FilesByCompactionPri.get(j));
                    Assert.assertTrue(curFile.size() >= smallerFile.size());
                }
            }
        }
    }

    @Test
    public void testOverlappingInputs() {
        ColumnFamilyOptions columnFamilyOptions =
                RocksDBCompactionTestUtils.createDefaultColumnFamilyOption();
        columnFamilyOptions.setMaxBytesForLevelBase(10000);

        TestDB db = new TestDB(columnFamilyOptions);

        db.add(1, 1, "a", "b", 0, Long.MAX_VALUE);
        db.add(1, 2, "b", "c", 0, 0);
        db.add(1, 3, "d", "e", 0, Long.MAX_VALUE);
        db.add(1, 4, "e", "f", 0, 0);
        db.add(1, 5, "g", "h", 0, 0);
        db.add(1, 6, "i", "j", 0, 0);
        db.updateVersionStorageInfo();

        Assert.assertEquals(
                db.getOverlappingInputs(1, new InternalKey("a", 0), new InternalKey("b", 0)),
                "[1, 2]");
        Assert.assertEquals(
                db.getOverlappingInputs(
                        1, new InternalKey("a", 0), new InternalKey("b", Long.MAX_VALUE)),
                "[1, 2]");

        Assert.assertEquals(
                db.getOverlappingInputs(
                        1, new InternalKey("b", Long.MAX_VALUE), new InternalKey("c", 0)),
                "[1, 2]");
        Assert.assertEquals(
                db.getOverlappingInputs(1, new InternalKey("d", 0), new InternalKey("e", 0)),
                "[3, 4]");
        Assert.assertEquals(
                db.getOverlappingInputs(
                        1, new InternalKey("d", 0), new InternalKey("e", Long.MAX_VALUE)),
                "[3, 4]");
        Assert.assertEquals(
                db.getOverlappingInputs(
                        1, new InternalKey("e", Long.MAX_VALUE), new InternalKey("f", 0)),
                "[3, 4]");
        Assert.assertEquals(
                db.getOverlappingInputs(1, new InternalKey("e", 0), new InternalKey("f", 0)),
                "[3, 4]");
        Assert.assertEquals(
                db.getOverlappingInputs(1, new InternalKey("g", 0), new InternalKey("h", 0)),
                "[5]");
        Assert.assertEquals(
                db.getOverlappingInputs(1, new InternalKey("i", 0), new InternalKey("j", 0)),
                "[6]");
    }
}
