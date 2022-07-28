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

import org.apache.flink.contrib.streaming.state.compaction.util.CompactionInputFiles;
import org.apache.flink.contrib.streaming.state.compaction.util.FlinkSstFileMetaData;

import org.junit.Assert;
import org.junit.Test;
import org.rocksdb.ColumnFamilyOptions;

import java.util.Collections;

import static org.apache.flink.contrib.streaming.state.compaction.RocksDBCompactionTestUtils.createSstFile;
import static org.apache.flink.contrib.streaming.state.compaction.RocksDBCompactionTestUtils.key;

public class CompactionPickerTest {

    @Test
    public void testExpandInputsToCleanCut() {
        ColumnFamilyOptions columnFamilyOptions =
                RocksDBCompactionTestUtils.createDefaultColumnFamilyOption();

        TestDB db = new TestDB(columnFamilyOptions);
        db.add(1, 1, key(50), key(99), 0, 1000);
        db.add(1, 2, key(100), key(150), 0, 1000);
        db.add(1, 3, key(150), key(200), 0, 0);
        db.add(1, 4, key(201), key(250), 0, 0);

        db.updateVersionStorageInfo();

        CompactionInputFiles inputs = new CompactionInputFiles();
        inputs.level = 1;
        inputs.files.add(createSstFile(2, key(100), key(150), 0, 1000));

        Assert.assertEquals(db.expandInputsToCleanCut(inputs), "[2, 3]");
    }

    @Test
    public void level0Trigger() {
        ColumnFamilyOptions columnFamilyOptions =
                RocksDBCompactionTestUtils.createDefaultColumnFamilyOption();
        columnFamilyOptions.setLevel0FileNumCompactionTrigger(2);

        TestDB db = new TestDB(columnFamilyOptions);
        db.add(0, 1, "150", "200", 0, 0);
        db.add(0, 2, "200", "250", 0, 0);

        db.updateVersionStorageInfo();

        Compaction compaction = db.getCompaction();
        Assert.assertNotNull(compaction);

        Assert.assertEquals(compaction.numInputFiles(0), 2);
        Assert.assertEquals(compaction.input(0, 0).fileName(), "1");
        Assert.assertEquals(compaction.input(0, 1).fileName(), "2");
    }

    @Test
    public void level0Trigger2() {
        ColumnFamilyOptions columnFamilyOptions =
                RocksDBCompactionTestUtils.createDefaultColumnFamilyOption();
        columnFamilyOptions.setLevel0FileNumCompactionTrigger(1);

        TestDB db = new TestDB(columnFamilyOptions);
        db.add(0, 1, "000", "127", 0, 0);

        db.add(1, 7, "000", "057", 0, 0);
        db.add(1, 6, "057", "080", 0, 0);
        db.add(1, 8, "080", "107", 0, 0);
        db.add(1, 5, "107", "127", 0, 0);

        db.updateVersionStorageInfo();

        Compaction compaction = db.getCompaction();
        Assert.assertNotNull(compaction);
        Assert.assertEquals(2, compaction.numInputLevels());
        Assert.assertEquals(1, compaction.numInputFiles(0));
        Assert.assertEquals(4, compaction.numInputFiles(1));
    }

    @Test
    public void level1Trigger() {
        ColumnFamilyOptions columnFamilyOptions =
                RocksDBCompactionTestUtils.createDefaultColumnFamilyOption();

        TestDB db = new TestDB(columnFamilyOptions);
        db.add(1, 66, "150", "150", 0, 0, 1000000000);
        db.updateVersionStorageInfo();

        Compaction compaction = db.getCompaction();
        Assert.assertNotNull(compaction);
        Assert.assertEquals(compaction.numInputFiles(0), 1);
        Assert.assertEquals(compaction.input(0, 0).fileName(), "66");
    }

    @Test
    public void level1Trigger2() {
        ColumnFamilyOptions columnFamilyOptions =
                RocksDBCompactionTestUtils.createDefaultColumnFamilyOption();
        columnFamilyOptions.setTargetFileSizeBase(10000000000L);

        TestDB db = new TestDB(columnFamilyOptions);
        db.add(1, 66, "150", "200", 0, 0, 1000000001);
        db.add(1, 88, "201", "300", 0, 0, 1000000000);
        db.add(2, 6, "150", "179", 0, 0, 1000000000);
        db.add(2, 7, "180", "220", 0, 0, 1000000000);
        db.add(2, 7, "221", "300", 0, 0, 1000000000);
        db.updateVersionStorageInfo();

        Compaction compaction = db.getCompaction();
        Assert.assertNotNull(compaction);
        Assert.assertEquals(1, compaction.numInputFiles(0));
        Assert.assertEquals(2, compaction.numInputFiles(1));
        Assert.assertEquals("66", compaction.input(0, 0).fileName());
        Assert.assertEquals("6", compaction.input(1, 0).fileName());
        Assert.assertEquals("7", compaction.input(1, 1).fileName());
    }

    @Test
    public void levelMaxScore() {
        ColumnFamilyOptions columnFamilyOptions =
                RocksDBCompactionTestUtils.createDefaultColumnFamilyOption();
        columnFamilyOptions.setTargetFileSizeBase(10000000L);
        columnFamilyOptions.setMaxBytesForLevelBase(10 * 1024 * 1024L);

        TestDB db = new TestDB(columnFamilyOptions);

        db.add(0, 1, "150", "200", 0, 0, 1000000);
        // Level 1 score 1.2
        db.add(1, 66, "150", "200", 0, 0, 6000000);
        db.add(1, 88, "201", "300", 0, 0, 6000000);
        // Level 2 score 1.8. File 7 is the largest. Should be picked.
        db.add(2, 6, "150", "179", 0, 0, 60000000);
        db.add(2, 7, "180", "220", 0, 0, 60000001);
        db.add(2, 8, "221", "330", 0, 0, 60000000);
        // Level 3 score slightly larger than 1
        db.add(3, 26, "150", "170", 0, 0, 260000000);
        db.add(3, 27, "171", "179", 0, 0, 260000000);
        db.add(3, 28, "191", "220", 0, 0, 260000000);
        db.add(3, 29, "221", "300", 0, 0, 260000000);
        db.updateVersionStorageInfo();

        Compaction compaction = db.getCompaction();
        Assert.assertNotNull(compaction);
        Assert.assertEquals(1, compaction.numInputFiles(0));
        Assert.assertEquals("7", compaction.input(0, 0).fileName());
    }

    @Test
    public void needsCompactionLevel() {
        int kLevels = 6;
        int kFileCount = 20;
        ColumnFamilyOptions columnFamilyOptions =
                RocksDBCompactionTestUtils.createDefaultColumnFamilyOption();

        for (int level = 0; level < kLevels - 1; level++) {
            TestDB tmp = new TestDB(columnFamilyOptions);
            tmp.updateVersionStorageInfo();
            long fileSize = tmp.getvStorage().maxBytesForLevel(level) * 2 / kFileCount;
            for (int fileCount = 1; fileCount <= kFileCount; fileCount++) {
                TestDB db = new TestDB(columnFamilyOptions);
                for (int i = 0; i < fileCount; i++) {
                    db.add(
                            level,
                            i,
                            String.format("%d", (i + 100) * 1000),
                            String.format("%d", (i + 100) * 1000 + 999),
                            fileSize,
                            i * 100,
                            i * 100 + 99);
                }
                db.updateVersionStorageInfo();

                Assert.assertEquals(db.getvStorage().getCompactionLevel()[0], level);
                Assert.assertEquals(
                        db.needCompaction(), db.getvStorage().getCompactionScore()[0] >= 1);
            }
        }
    }

    @Test
    public void overlappingUserKeys() {
        ColumnFamilyOptions columnFamilyOptions =
                RocksDBCompactionTestUtils.createDefaultColumnFamilyOption();
        TestDB db = new TestDB(columnFamilyOptions);

        db.add(1, 1, "100", "150", 100, 100, 1);
        db.add(1, 2, "200", "400", 100, 100, 1);
        db.add(1, 3, "400", "500", 0, 0, 1000000000);
        db.add(2, 4, "600", "700", 100, 100, 1);

        db.updateVersionStorageInfo();

        Compaction compaction = db.getCompaction();
        Assert.assertNotNull(compaction);
        Assert.assertEquals(1, compaction.numInputLevels());
        Assert.assertEquals(2, compaction.numInputFiles(0));
        Assert.assertEquals("2", compaction.input(0, 0).fileName());
        Assert.assertEquals("3", compaction.input(0, 1).fileName());
    }

    @Test
    public void overlappingUserKeys2() {
        ColumnFamilyOptions columnFamilyOptions =
                RocksDBCompactionTestUtils.createDefaultColumnFamilyOption();
        TestDB db = new TestDB(columnFamilyOptions);

        db.add(1, 1, "200", "400", 100, 100, 1000000000);
        db.add(1, 2, "400", "500", 0, 0, 1);
        db.add(2, 3, "000", "100", 100, 100, 1);
        db.add(2, 4, "100", "600", 0, 0, 1);
        db.add(2, 5, "600", "700", 0, 0, 1);

        db.updateVersionStorageInfo();

        Compaction compaction = db.getCompaction();
        Assert.assertNotNull(compaction);
        Assert.assertEquals(2, compaction.numInputLevels());
        Assert.assertEquals(2, compaction.numInputFiles(0));
        Assert.assertEquals(3, compaction.numInputFiles(1));
        Assert.assertEquals("1", compaction.input(0, 0).fileName());
        Assert.assertEquals("2", compaction.input(0, 1).fileName());
        Assert.assertEquals("3", compaction.input(1, 0).fileName());
        Assert.assertEquals("4", compaction.input(1, 1).fileName());
        Assert.assertEquals("5", compaction.input(1, 2).fileName());
    }

    @Test
    public void overlappingUserKeys3() {
        ColumnFamilyOptions columnFamilyOptions =
                RocksDBCompactionTestUtils.createDefaultColumnFamilyOption();
        TestDB db = new TestDB(columnFamilyOptions);

        db.add(1, 1, "100", "150", 100, 100, 1);
        db.add(1, 2, "150", "200", 0, 0, 1);
        db.add(1, 3, "200", "250", 0, 0, 1000000000);
        db.add(1, 4, "250", "300", 0, 0, 1);
        db.add(1, 5, "300", "350", 0, 0, 1);

        db.add(2, 6, "050", "100", 100, 100, 1);
        db.add(2, 7, "350", "400", 100, 100, 1);

        db.updateVersionStorageInfo();

        Compaction compaction = db.getCompaction();
        Assert.assertNotNull(compaction);
        Assert.assertEquals(2, compaction.numInputLevels());
        Assert.assertEquals(5, compaction.numInputFiles(0));
        Assert.assertEquals(2, compaction.numInputFiles(1));
        Assert.assertEquals("1", compaction.input(0, 0).fileName());
        Assert.assertEquals("2", compaction.input(0, 1).fileName());
        Assert.assertEquals("3", compaction.input(0, 2).fileName());
        Assert.assertEquals("4", compaction.input(0, 3).fileName());
        Assert.assertEquals("5", compaction.input(0, 4).fileName());
        Assert.assertEquals("6", compaction.input(1, 0).fileName());
        Assert.assertEquals("7", compaction.input(1, 1).fileName());
    }

    @Test
    public void overlappingUserKeys4() {
        ColumnFamilyOptions columnFamilyOptions =
                RocksDBCompactionTestUtils.createDefaultColumnFamilyOption();
        columnFamilyOptions.setMaxBytesForLevelBase(1000000);
        TestDB db = new TestDB(columnFamilyOptions);

        db.add(1, 1, "100", "150", 100, 100, 1);
        db.add(1, 2, "150", "199", 0, 0, 1);
        db.add(1, 3, "200", "250", 0, 0, 1100000);
        db.add(1, 4, "251", "300", 0, 0, 1);
        db.add(1, 5, "300", "350", 0, 0, 1);

        db.add(2, 6, "100", "115", 100, 100, 1);
        db.add(2, 7, "125", "325", 100, 100, 1);
        db.add(2, 6, "350", "400", 100, 100, 1);

        db.updateVersionStorageInfo();
        Compaction compaction = db.getCompaction();
        Assert.assertNotNull(compaction);
        Assert.assertEquals(2, compaction.numInputLevels());
        Assert.assertEquals(1, compaction.numInputFiles(0));
        Assert.assertEquals(1, compaction.numInputFiles(1));
        Assert.assertEquals("3", compaction.input(0, 0).fileName());
        Assert.assertEquals("7", compaction.input(1, 0).fileName());
    }

    @Test
    public void overlappingUserKeys5() {
        ColumnFamilyOptions columnFamilyOptions =
                RocksDBCompactionTestUtils.createDefaultColumnFamilyOption();
        columnFamilyOptions.setMaxBytesForLevelBase(1000000);
        TestDB db = new TestDB(columnFamilyOptions);

        db.add(1, 1, "200", "400", 100, 100, 1000000000);
        db.add(1, 2, "400", "500", 0, 0, 1);
        db.add(2, 3, "000", "100", 100, 100, 1);
        db.add(2, 4, "100", "600", 0, 0, 1);
        db.add(2, 5, "600", "700", 0, 0, 1);

        db.levelFiles(2).get(2).setBeingCompacted(true);
        db.updateVersionStorageInfo();

        Compaction compaction = db.getCompaction();
        Assert.assertNull(compaction);
    }

    @Test
    public void overlappingUserKeys7() {
        ColumnFamilyOptions columnFamilyOptions =
                RocksDBCompactionTestUtils.createDefaultColumnFamilyOption();
        columnFamilyOptions.setMaxCompactionBytes(100000000000L);
        columnFamilyOptions.setMaxBytesForLevelBase(1000000);
        TestDB db = new TestDB(columnFamilyOptions);

        db.add(1, 1, "200", "400", 0, 0, 1);
        db.add(1, 2, "401", "500", 0, 0, 100000000);
        db.add(2, 3, "100", "250", 100, 100, 1);
        db.add(2, 4, "300", "600", 0, 0, 1);
        db.add(2, 5, "600", "800", 0, 0, 1);

        db.updateVersionStorageInfo();

        Compaction compaction = db.getCompaction();
        Assert.assertNotNull(compaction);
        Assert.assertEquals(2, compaction.numInputLevels());
        Assert.assertTrue(compaction.numInputFiles(0) >= 1);
        Assert.assertTrue(compaction.numInputFiles(1) >= 2);
        Assert.assertEquals(
                "5", compaction.inputs(1).get(compaction.inputs(1).size() - 1).fileName());
    }

    @Test
    public void overlappingUserKeys8() {
        ColumnFamilyOptions columnFamilyOptions =
                RocksDBCompactionTestUtils.createDefaultColumnFamilyOption();
        columnFamilyOptions.setMaxCompactionBytes(100000000000L);
        TestDB db = new TestDB(columnFamilyOptions);

        db.add(1, 1, "101", "150", 100, 100, 1);
        db.add(1, 2, "151", "200", 100, 100, 1);
        db.add(1, 3, "201", "300", 100, 100, 1000000000);
        db.add(1, 4, "301", "400", 100, 100, 1);
        db.add(1, 5, "401", "500", 100, 100, 1);
        db.add(2, 6, "150", "200", 100, 100, 1);
        db.add(2, 7, "200", "450", 0, 0, 1);
        db.add(2, 8, "500", "600", 100, 100, 1);
        db.updateVersionStorageInfo();

        Compaction compaction = db.getCompaction();
        Assert.assertNotNull(compaction);
        Assert.assertEquals(2, compaction.numInputLevels());
        Assert.assertEquals(3, compaction.numInputFiles(0));
        Assert.assertEquals(2, compaction.numInputFiles(1));
        Assert.assertEquals("2", compaction.input(0, 0).fileName());
        Assert.assertEquals("3", compaction.input(0, 1).fileName());
        Assert.assertEquals("4", compaction.input(0, 2).fileName());
        Assert.assertEquals("6", compaction.input(1, 0).fileName());
        Assert.assertEquals("7", compaction.input(1, 1).fileName());
    }

    @Test
    public void overlappingUserKeys9() {
        ColumnFamilyOptions columnFamilyOptions =
                RocksDBCompactionTestUtils.createDefaultColumnFamilyOption();
        columnFamilyOptions.setMaxCompactionBytes(100000000000L);

        TestDB db = new TestDB(columnFamilyOptions);

        db.add(1, 1, "121", "150", 100, 100, 1);
        db.add(1, 2, "151", "200", 100, 100, 1);
        db.add(1, 3, "201", "300", 100, 100, 1000000000);
        db.add(1, 4, "301", "400", 100, 100, 1);
        db.add(1, 5, "401", "500", 100, 100, 1);
        db.add(2, 6, "100", "120", 100, 100, 1);
        db.add(2, 7, "150", "200", 100, 100, 1);
        db.add(2, 8, "200", "450", 0, 0, 1);
        db.add(2, 9, "501", "600", 100, 100, 1);
        db.updateVersionStorageInfo();

        Compaction compaction = db.getCompaction();
        Assert.assertNotNull(compaction);
        Assert.assertEquals(2, compaction.numInputLevels());
        Assert.assertEquals(5, compaction.numInputFiles(0));
        Assert.assertEquals(2, compaction.numInputFiles(1));
        Assert.assertEquals("1", compaction.input(0, 0).fileName());
        Assert.assertEquals("2", compaction.input(0, 1).fileName());
        Assert.assertEquals("3", compaction.input(0, 2).fileName());
        Assert.assertEquals("4", compaction.input(0, 3).fileName());
        Assert.assertEquals("5", compaction.input(0, 4).fileName());
        Assert.assertEquals("7", compaction.input(1, 0).fileName());
        Assert.assertEquals("8", compaction.input(1, 1).fileName());
    }

    @Test
    public void overlappingUserKeys10() {
        ColumnFamilyOptions columnFamilyOptions =
                RocksDBCompactionTestUtils.createDefaultColumnFamilyOption();
        columnFamilyOptions.setMaxCompactionBytes(100000000000L);

        TestDB db = new TestDB(columnFamilyOptions);

        db.add(1, 1, "100", "150", 100, 100, 1);
        db.add(1, 2, "150", "200", 0, 0, 1000000000);
        db.add(1, 3, "201", "250", 100, 100, 900000000);
        db.add(2, 4, "100", "150", 100, 100, 1);
        db.add(2, 5, "151", "200", 100, 100, 1);
        db.add(2, 6, "201", "250", 100, 100, 1);

        db.levelFiles(1).get(0).setBeingCompacted(true);
        db.updateVersionStorageInfo();

        Compaction compaction = db.getCompaction();
        Assert.assertNotNull(compaction);
        Assert.assertEquals(2, compaction.numInputLevels());
        Assert.assertEquals(1, compaction.numInputFiles(0));
        Assert.assertEquals(1, compaction.numInputFiles(1));
        Assert.assertEquals("3", compaction.input(0, 0).fileName());
        Assert.assertEquals("6", compaction.input(1, 0).fileName());
    }

    @Test
    public void overlappingUserKeys11() {
        ColumnFamilyOptions columnFamilyOptions =
                RocksDBCompactionTestUtils.createDefaultColumnFamilyOption();
        columnFamilyOptions.setMaxCompactionBytes(100000000000L);

        TestDB db = new TestDB(columnFamilyOptions);

        db.add(1, 2, "151", "200", 100, 100, 1000000000L);
        db.add(1, 3, "201", "250", 100, 100, 1);
        db.add(2, 4, "100", "149", 100, 100, 5000000000L);
        db.add(2, 5, "150", "201", 100, 100, 1);
        db.add(2, 6, "201", "249", 0, 0, 1);
        db.add(3, 7, "100", "149", 100, 100, 1);
        db.levelFiles(2).get(2).setBeingCompacted(true);
        db.updateVersionStorageInfo();

        Compaction compaction = db.getCompaction();
        Assert.assertNotNull(compaction);
        Assert.assertEquals(2, compaction.numInputLevels());
        Assert.assertEquals(1, compaction.numInputFiles(0));
        Assert.assertEquals(1, compaction.numInputFiles(1));
        Assert.assertEquals("4", compaction.input(0, 0).fileName());
        Assert.assertEquals("7", compaction.input(1, 0).fileName());
    }

    @Test
    public void notScheduleL1IfL0WithHigherPri1() {
        ColumnFamilyOptions columnFamilyOptions =
                RocksDBCompactionTestUtils.createDefaultColumnFamilyOption();
        columnFamilyOptions.setLevel0FileNumCompactionTrigger(2);
        columnFamilyOptions.setMaxBytesForLevelBase(900000000L);

        TestDB db = new TestDB(columnFamilyOptions);

        // 6 L0 files, score 3.
        db.add(0, 1, "000", "400", 100, 100, 1);
        db.add(0, 2, "001", "400", 0, 0, 1);
        db.add(0, 3, "001", "400", 0, 0, 1000000000);
        db.add(0, 31, "001", "400", 0, 0, 1000000000);
        db.add(0, 32, "001", "400", 0, 0, 1000000000);
        db.add(0, 33, "001", "400", 0, 0, 1000000000);

        db.add(1, 4, "050", "300", 0, 0, 1000000000L);
        db.add(1, 5, "301", "350", 0, 0, 1000000000L);

        db.add(2, 6, "050", "100", 100, 100, 1);
        db.add(2, 7, "300", "400", 100, 100, 1);

        db.levelFiles(1).get(0).setBeingCompacted(true);
        db.updateVersionStorageInfo();

        Compaction compaction = db.getCompaction();
        Assert.assertNull(compaction);
        Assert.assertEquals(0, db.getvStorage().getCompactionLevel()[0]);
        Assert.assertEquals(1, db.getvStorage().getCompactionLevel()[1]);
    }

    @Test
    public void notScheduleL1IfL0WithHigherPri2() {
        ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
        columnFamilyOptions.setLevel0FileNumCompactionTrigger(2);
        columnFamilyOptions.setMaxBytesForLevelBase(900000000L);

        TestDB db = new TestDB(columnFamilyOptions);

        // 6 L0 files, score 3.
        db.add(0, 1, "000", "400", 100, 100, 1);
        db.add(0, 2, "001", "400", 0, 0, 1);
        db.add(0, 3, "001", "400", 0, 0, 1000000000);
        db.add(0, 31, "001", "400", 0, 0, 1000000000);
        db.add(0, 32, "001", "400", 0, 0, 1000000000);
        db.add(0, 33, "001", "400", 0, 0, 1000000000);

        db.add(1, 4, "050", "300", 0, 0, 1000000000L);
        db.add(1, 5, "301", "350", 0, 0, 1000000000L);

        db.add(2, 6, "050", "100", 100, 100, 1);
        db.add(2, 7, "300", "400", 100, 100, 1);

        db.updateVersionStorageInfo();
        Assert.assertEquals(0, db.getvStorage().getCompactionLevel()[0]);
        Assert.assertEquals(1, db.getvStorage().getCompactionLevel()[1]);
        Compaction compaction = db.getCompaction();
        Assert.assertNotNull(compaction);
    }

    @Test
    public void notScheduleL1IfL0WithHigherPri3() {
        ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
        columnFamilyOptions.setLevel0FileNumCompactionTrigger(2);
        columnFamilyOptions.setMaxBytesForLevelBase(900000000L);

        TestDB db = new TestDB(columnFamilyOptions);

        db.add(0, 1, "000", "400", 100, 100, 1);
        db.add(0, 2, "001", "400", 0, 0, 1);
        db.add(0, 3, "001", "400", 0, 0, 1000000000);
        db.add(0, 31, "001", "400", 0, 0, 1000000000);
        db.add(0, 32, "001", "400", 0, 0, 1000000000);
        db.add(0, 33, "001", "400", 0, 0, 1000000000);

        db.add(1, 4, "050", "300", 0, 0, 1000000000L);
        db.add(1, 5, "301", "350", 0, 0, 1000000000L);
        db.add(1, 51, "351", "400", 0, 0, 6000000000L);

        db.add(2, 6, "050", "100", 100, 100, 1);
        db.add(2, 7, "300", "400", 100, 100, 1);

        db.levelFiles(1).get(0).setBeingCompacted(true);
        db.updateVersionStorageInfo();

        Assert.assertEquals(1, db.getvStorage().getCompactionLevel()[0]);
        Assert.assertEquals(0, db.getvStorage().getCompactionLevel()[1]);
        Compaction compaction = db.getCompaction();
        Assert.assertNotNull(compaction);
    }

    @Test
    public void maxCompactionBytesHit() {
        ColumnFamilyOptions columnFamilyOptions =
                RocksDBCompactionTestUtils.createDefaultColumnFamilyOption();
        columnFamilyOptions.setMaxBytesForLevelBase(1000000L);
        columnFamilyOptions.setMaxCompactionBytes(800000L);

        TestDB db = new TestDB(columnFamilyOptions);

        db.add(1, 1, "100", "150", 100, 100, 300000);
        db.add(1, 2, "151", "200", 0, 0, 300001);
        db.add(1, 3, "201", "250", 0, 0, 300000);
        db.add(1, 4, "251", "300", 0, 0, 300000);
        db.add(2, 5, "100", "256", 100, 100, 1);
        db.updateVersionStorageInfo();

        Compaction compaction = db.getCompaction();
        Assert.assertNotNull(compaction);
        Assert.assertEquals(2, compaction.numInputLevels());
        Assert.assertEquals(1, compaction.numInputFiles(0));
        Assert.assertEquals(1, compaction.numInputFiles(1));
        Assert.assertEquals("2", compaction.input(0, 0).fileName());
        Assert.assertEquals("5", compaction.input(1, 0).fileName());
    }

    @Test
    public void maxCompactionBytesNotHit() {
        ColumnFamilyOptions columnFamilyOptions =
                RocksDBCompactionTestUtils.createDefaultColumnFamilyOption();
        columnFamilyOptions.setMaxBytesForLevelBase(80000L);
        columnFamilyOptions.setMaxCompactionBytes(1000000L);

        TestDB db = new TestDB(columnFamilyOptions);

        db.add(1, 1, "100", "150", 100, 100, 300000);
        db.add(1, 2, "151", "200", 0, 0, 300001);
        db.add(1, 3, "201", "250", 0, 0, 300000);
        db.add(1, 4, "251", "300", 0, 0, 300000);
        db.add(2, 5, "100", "251", 100, 100, 1);
        db.updateVersionStorageInfo();

        Compaction compaction = db.getCompaction();
        Assert.assertNotNull(compaction);
        Assert.assertEquals(2, compaction.numInputLevels());
        Assert.assertEquals(3, compaction.numInputFiles(0));
        Assert.assertEquals(1, compaction.numInputFiles(1));
        Assert.assertEquals("1", compaction.input(0, 0).fileName());
        Assert.assertEquals("2", compaction.input(0, 1).fileName());
        Assert.assertEquals("3", compaction.input(0, 2).fileName());
        Assert.assertEquals("5", compaction.input(1, 0).fileName());
    }

    @Test
    public void intraL0MaxCompactionBytesNotHit() {
        ColumnFamilyOptions columnFamilyOptions =
                RocksDBCompactionTestUtils.createDefaultColumnFamilyOption();
        columnFamilyOptions.setLevel0FileNumCompactionTrigger(3);
        columnFamilyOptions.setMaxCompactionBytes(1000000);

        TestDB db = new TestDB(columnFamilyOptions);

        db.add(0, 1, "100", "150", 100, 101, 200000L);
        db.add(0, 2, "151", "200", 102, 103, 200000L);
        db.add(0, 3, "201", "250", 104, 105, 200000L);
        db.add(0, 4, "251", "300", 106, 107, 200000L);
        db.add(0, 5, "301", "350", 108, 109, 200000L);
        db.add(1, 6, "100", "350", 110, 111, 200000L);

        db.levelFiles(1).get(0).setBeingCompacted(true);
        db.updateVersionStorageInfo();

        Compaction compaction = db.getCompaction();
        Assert.assertNotNull(compaction);
        Assert.assertEquals(1, compaction.numInputLevels());
        Assert.assertEquals(5, compaction.numInputFiles(0));
        Assert.assertEquals(0, compaction.getOutputLevel());
    }

    @Test
    public void intraL0MaxCompactionBytesHit() {
        ColumnFamilyOptions columnFamilyOptions =
                RocksDBCompactionTestUtils.createDefaultColumnFamilyOption();
        columnFamilyOptions.setLevel0FileNumCompactionTrigger(3);
        columnFamilyOptions.setMaxCompactionBytes(999999);

        TestDB db = new TestDB(columnFamilyOptions);

        db.add(0, 1, "100", "150", 100, 101, 200000L);
        db.add(0, 2, "151", "200", 102, 103, 200000L);
        db.add(0, 3, "201", "250", 104, 105, 200000L);
        db.add(0, 4, "251", "300", 106, 107, 200000L);
        db.add(0, 5, "301", "350", 108, 109, 200000L);
        db.add(1, 6, "100", "350", 110, 111, 200000L);

        db.levelFiles(1).get(0).setBeingCompacted(true);
        db.updateVersionStorageInfo();

        Compaction compaction = db.getCompaction();
        Assert.assertNotNull(compaction);
        Assert.assertEquals(1, compaction.numInputLevels());
        Assert.assertEquals(4, compaction.numInputFiles(0));
        Assert.assertEquals(0, compaction.getOutputLevel());
    }

    @Test
    public void intraL0ForEarliestSeqno() {
        ColumnFamilyOptions columnFamilyOptions =
                RocksDBCompactionTestUtils.createDefaultColumnFamilyOption();
        columnFamilyOptions.setLevel0FileNumCompactionTrigger(3);
        columnFamilyOptions.setMaxCompactionBytes(999999);

        TestDB db = new TestDB(columnFamilyOptions);

        db.add(1, 1, "100", "350", 110, 111, 200000);
        db.add(0, 2, "301", "350", 108, 109, 1);
        db.add(0, 3, "251", "300", 106, 107, 1);
        db.add(0, 4, "201", "250", 104, 105, 1);
        db.add(0, 5, "151", "200", 102, 103, 1);
        db.add(0, 6, "100", "150", 100, 101, 1);
        db.add(0, 7, "100", "100", 99, 100, 1);

        db.levelFiles(0).get(5).setBeingCompacted(true);
        db.levelFiles(1).get(0).setBeingCompacted(true);
        db.updateVersionStorageInfo();

        Compaction compaction = db.getCompaction(107L);
        Assert.assertNotNull(compaction);
        Assert.assertEquals(1, compaction.numInputLevels());
        Assert.assertEquals(4, compaction.numInputFiles(0));
        Assert.assertEquals(0, compaction.getOutputLevel());
    }

    @Test
    public void updateVStorage() {
        ColumnFamilyOptions columnFamilyOptions =
                RocksDBCompactionTestUtils.createDefaultColumnFamilyOption();
        columnFamilyOptions.setLevel0FileNumCompactionTrigger(2);

        TestDB db = new TestDB(columnFamilyOptions);
        db.add(0, 1, "000", "127", 0, 0);
        db.add(0, 2, "000", "127", 0, 0);
        // create a init vStorage
        db.updateVersionStorageInfo();

        db.add(1, 7, "000", "057", 0, 0);
        db.add(1, 6, "057", "080", 0, 0);
        db.add(1, 8, "080", "107", 0, 0);
        db.add(1, 5, "107", "127", 0, 0);
        db.installLevelFiles(1);

        Compaction compaction = db.getCompaction();
        Assert.assertNotNull(compaction);

        Assert.assertEquals(2, compaction.numInputLevels());
        Assert.assertEquals(2, compaction.numInputFiles(0));
        Assert.assertEquals(4, compaction.numInputFiles(1));
        Assert.assertEquals(compaction.input(0, 0).fileName(), "1");
        Assert.assertEquals(compaction.input(0, 1).fileName(), "2");
    }

    @Test
    public void compactWithInitialFilesL0Trigger() {
        ColumnFamilyOptions columnFamilyOptions =
                RocksDBCompactionTestUtils.createDefaultColumnFamilyOption();
        columnFamilyOptions.setLevel0FileNumCompactionTrigger(2);

        TestDB db = new TestDB(columnFamilyOptions);
        db.add(0, 1, "150", "200", 0, 0);
        db.add(0, 2, "200", "250", 0, 0);
        db.add(0, 3, "250", "300", 0, 0);
        db.add(0, 4, "350", "400", 0, 0);
        db.add(1, 5, "100", "400", 0, 0);

        db.updateVersionStorageInfo();
        FlinkSstFileMetaData l0File0 = db.getvStorage().levelFiles(0).get(0);
        Assert.assertEquals("1", l0File0.fileName());

        Compaction compaction =
                db.getCompactionWithInitialFiles(Collections.singletonList(l0File0), 0);

        Assert.assertNotNull(compaction);
        Assert.assertEquals(2, compaction.numInputLevels());
        Assert.assertEquals(3, compaction.numInputFiles(0));
        Assert.assertEquals(1, compaction.numInputFiles(1));
        Assert.assertEquals(compaction.input(0, 0).fileName(), "1");
        Assert.assertEquals(compaction.input(0, 1).fileName(), "2");
        Assert.assertEquals(compaction.input(0, 2).fileName(), "3");
        Assert.assertEquals(compaction.input(1, 0).fileName(), "5");
    }

    @Test
    public void compactWithInitialFilesL1Trigger() {
        ColumnFamilyOptions columnFamilyOptions =
                RocksDBCompactionTestUtils.createDefaultColumnFamilyOption();
        columnFamilyOptions.setLevel0FileNumCompactionTrigger(2);
        TestDB db = new TestDB(columnFamilyOptions);

        db.add(1, 1, "7", "7", 0, 0);
        db.add(1, 2, "8", "8", 0, 0);
        db.add(2, 3, "6", "7", 0, 0);
        db.add(2, 4, "7", "7", 0, 0);
        db.add(2, 5, "7", "7", 0, 0);
        db.add(2, 6, "7", "7", 0, 0);
        db.add(2, 7, "7", "8", 0, 0);

        db.updateVersionStorageInfo();
        FlinkSstFileMetaData l1File0 = db.getvStorage().levelFiles(1).get(0);
        Assert.assertEquals("1", l1File0.fileName());

        Compaction compaction =
                db.getCompactionWithInitialFiles(Collections.singletonList(l1File0), 1);
        Assert.assertNotNull(compaction);
    }

    @Test
    public void compactWithInitialFilesL1Trigger2() {
        ColumnFamilyOptions columnFamilyOptions =
                RocksDBCompactionTestUtils.createDefaultColumnFamilyOption();
        columnFamilyOptions.setLevel0FileNumCompactionTrigger(2);
        TestDB db = new TestDB(columnFamilyOptions);

        db.add(1, 1, "7", "7", 0, 0);
        db.add(1, 2, "7", "8", 0, 0);
        db.add(2, 3, "6", "7", 0, 0);
        db.add(2, 4, "7", "7", 0, 0);
        db.add(2, 5, "7", "8", 0, 0);
        db.add(2, 6, "9", "9", 0, 0);

        db.updateVersionStorageInfo();
        FlinkSstFileMetaData l1File0 = db.getvStorage().levelFiles(1).get(0);
        Assert.assertEquals("1", l1File0.fileName());

        Compaction compaction =
                db.getCompactionWithInitialFiles(Collections.singletonList(l1File0), 1);
        Assert.assertNotNull(compaction);
        Assert.assertEquals(2, compaction.numInputLevels());
        Assert.assertEquals(2, compaction.numInputFiles(0));
        Assert.assertEquals("1", compaction.input(0, 0).fileName());
        Assert.assertEquals("2", compaction.input(0, 1).fileName());
        Assert.assertEquals(3, compaction.numInputFiles(1));
        Assert.assertEquals("3", compaction.input(1, 0).fileName());
        Assert.assertEquals("4", compaction.input(1, 1).fileName());
        Assert.assertEquals("5", compaction.input(1, 2).fileName());
    }

    @Test
    public void pickTrivialMoveTest1() {
        ColumnFamilyOptions columnFamilyOptions =
                RocksDBCompactionTestUtils.createDefaultColumnFamilyOption();
        columnFamilyOptions.setLevel0FileNumCompactionTrigger(2);
        TestDB db = new TestDB(columnFamilyOptions);

        db.add(1, 1, "07", "08", 0, 0);
        db.add(1, 2, "09", "10", 0, 0);
        db.add(1, 3, "11", "12", 0, 0);
        db.add(2, 4, "13", "14", 0, 0);

        db.updateVersionStorageInfo();

        Compaction compaction = db.getTrivialMoveCompaction(1);
        Assert.assertNotNull(compaction);
        Assert.assertEquals(1, compaction.numInputLevels());
        Assert.assertEquals(3, compaction.numInputFiles(0));
    }

    @Test
    public void pickTrivialMoveTest2() {
        ColumnFamilyOptions columnFamilyOptions =
                RocksDBCompactionTestUtils.createDefaultColumnFamilyOption();
        columnFamilyOptions.setLevel0FileNumCompactionTrigger(2);
        TestDB db = new TestDB(columnFamilyOptions);

        db.add(1, 1, "07", "08", 0, 0);
        db.add(1, 2, "09", "10", 0, 0);
        db.add(1, 3, "11", "13", 0, 0);
        db.add(2, 4, "06", "07", 0, 0);
        db.add(2, 5, "12", "14", 0, 0);

        db.updateVersionStorageInfo();
        Compaction compaction = db.getTrivialMoveCompaction(1);
        Assert.assertNotNull(compaction);
        Assert.assertEquals(1, compaction.numInputLevels());
        Assert.assertEquals(1, compaction.numInputFiles(0));
        Assert.assertEquals("2", compaction.input(0, 0).fileName());
    }

    @Test
    public void pickTrivialMoveTest3() {
        ColumnFamilyOptions columnFamilyOptions =
                RocksDBCompactionTestUtils.createDefaultColumnFamilyOption();
        columnFamilyOptions.setLevel0FileNumCompactionTrigger(2);
        TestDB db = new TestDB(columnFamilyOptions);

        db.add(1, 1, "07", "08", 0, 0);
        db.add(1, 2, "09", "10", 0, 0);
        db.add(1, 3, "11", "13", 0, 0);
        db.add(1, 4, "16", "18", 0, 0);
        db.add(2, 5, "06", "07", 0, 0);
        db.add(2, 6, "12", "14", 0, 0);

        db.updateVersionStorageInfo();
        Compaction compaction = db.getTrivialMoveCompaction(1);
        Assert.assertNotNull(compaction);
        System.out.println("Compaction: " + compaction);
        Assert.assertEquals(1, compaction.numInputLevels());
        Assert.assertEquals(1, compaction.numInputFiles(0));
        Assert.assertEquals("2", compaction.input(0, 0).fileName());
    }
}
