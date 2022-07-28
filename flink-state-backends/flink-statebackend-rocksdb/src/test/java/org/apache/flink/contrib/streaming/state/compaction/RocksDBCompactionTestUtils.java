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

import org.apache.flink.contrib.streaming.state.compaction.util.FlinkSstFileMetaData;

import org.junit.Test;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionPriority;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

public class RocksDBCompactionTestUtils {

    public static RocksDB createDB(
            ColumnFamilyOptions columnFamilyOptions, DBOptions dbOptions, String dbPath)
            throws RocksDBException {
        RocksDB db = RocksDB.open(new Options(dbOptions, columnFamilyOptions), dbPath);
        return db;
    }

    public static ColumnFamilyOptions createDefaultColumnFamilyOption() {
        ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
        columnFamilyOptions.setLevel0FileNumCompactionTrigger(3);
        columnFamilyOptions.setDisableAutoCompactions(true);
        columnFamilyOptions.setCompactionPriority(CompactionPriority.ByCompensatedSize);
        return columnFamilyOptions;
    }

    public static DBOptions createDefaultDBOptions() {
        DBOptions dbOptions = new DBOptions();
        dbOptions.setCreateIfMissing(true);
        return dbOptions;
    }

    public static WriteOptions defaultWriteOptions() {
        WriteOptions writeOptions = new WriteOptions();
        writeOptions.setDisableWAL(true);
        return writeOptions;
    }

    public static FlinkSstFileMetaData createSstFile(
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
        return f;
    }

    public static String key(int val) {
        return String.format("key-%08d", val);
    }

    public static String val(int val) {
        return String.format("val-%d", val);
    }

    @Test
    public void testKey() {
        String key1 = key(1);
        assertEquals(key1, "key-00000001");
    }
}
