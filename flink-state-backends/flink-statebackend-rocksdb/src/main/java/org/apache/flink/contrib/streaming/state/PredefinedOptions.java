/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.InfoLogLevel;

import java.util.ArrayList;
import java.util.Collection;

/**
 * The {@code PredefinedOptions} are configuration settings for the {@link RocksDBStateBackend}. The
 * various pre-defined choices are configurations that have been empirically determined to be
 * beneficial for performance under different settings.
 *
 * <p>Some of these settings are based on experiments by the Flink community, some follow guides
 * from the RocksDB project.
 *
 * <p>All of them effectively disable the RocksDB log by default because this file would grow
 * indefinitely and will be deleted with the TM anyway.
 */
public enum PredefinedOptions {

    /**
     * Default options for all settings, except that writes are not forced to the disk.
     *
     * <p>Note: Because Flink does not rely on RocksDB data on disk for recovery, there is no need
     * to sync data to stable storage.
     *
     * <p>The following options are set:
     *
     * <ul>
     *   <li>setUseFsync(false)
     *   <li>setInfoLogLevel(InfoLogLevel.HEADER_LEVEL)
     *   <li>setStatsDumpPeriodSec(0)
     * </ul>
     */
    DEFAULT {

        @Override
        public DBOptions createDBOptions(Collection<AutoCloseable> handlesToClose) {
            return new DBOptions()
                    .setUseFsync(false)
                    .setInfoLogLevel(InfoLogLevel.INFO_LEVEL)
                    .setStatsDumpPeriodSec(180);
        }

        @Override
        public ColumnFamilyOptions createColumnOptions(Collection<AutoCloseable> handlesToClose) {
            return new ColumnFamilyOptions()
                    .setCompressionType(CompressionType.NO_COMPRESSION)
                    .setMaxCompactionBytes(25 * 64 * 1024 * 1024L);
        }
    },

    /**
     * Pre-defined options for regular spinning hard disks.
     *
     * <p>This constant configures RocksDB with some options that lead empirically to better
     * performance when the machines executing the system use regular spinning hard disks.
     *
     * <p>The following options are set:
     *
     * <ul>
     *   <li>setCompactionStyle(CompactionStyle.LEVEL)
     *   <li>setLevelCompactionDynamicLevelBytes(true)
     *   <li>setIncreaseParallelism(4)
     *   <li>setUseFsync(false)
     *   <li>setDisableDataSync(true)
     *   <li>setMaxOpenFiles(-1)
     *   <li>setInfoLogLevel(InfoLogLevel.HEADER_LEVEL)
     *   <li>setStatsDumpPeriodSec(0)
     * </ul>
     *
     * <p>Note: Because Flink does not rely on RocksDB data on disk for recovery, there is no need
     * to sync data to stable storage.
     */
    SPINNING_DISK_OPTIMIZED {

        @Override
        public DBOptions createDBOptions(Collection<AutoCloseable> handlesToClose) {
            return new DBOptions()
                    .setIncreaseParallelism(4)
                    .setUseFsync(false)
                    .setMaxOpenFiles(-1)
                    .setInfoLogLevel(InfoLogLevel.HEADER_LEVEL)
                    .setStatsDumpPeriodSec(0);
        }

        @Override
        public ColumnFamilyOptions createColumnOptions(Collection<AutoCloseable> handlesToClose) {
            return new ColumnFamilyOptions()
                    .setCompactionStyle(CompactionStyle.LEVEL)
                    .setLevelCompactionDynamicLevelBytes(true);
        }
    },

    /**
     * Pre-defined options for better performance on regular spinning hard disks, at the cost of a
     * higher memory consumption.
     *
     * <p><b>NOTE: These settings will cause RocksDB to consume a lot of memory for block caching
     * and compactions. If you experience out-of-memory problems related to, RocksDB, consider
     * switching back to {@link #SPINNING_DISK_OPTIMIZED}.</b>
     *
     * <p>The following options are set:
     *
     * <ul>
     *   <li>setLevelCompactionDynamicLevelBytes(true)
     *   <li>setTargetFileSizeBase(256 MBytes)
     *   <li>setMaxBytesForLevelBase(1 GByte)
     *   <li>setWriteBufferSize(64 MBytes)
     *   <li>setIncreaseParallelism(4)
     *   <li>setMinWriteBufferNumberToMerge(3)
     *   <li>setMaxWriteBufferNumber(4)
     *   <li>setUseFsync(false)
     *   <li>setMaxOpenFiles(-1)
     *   <li>setInfoLogLevel(InfoLogLevel.HEADER_LEVEL)
     *   <li>setStatsDumpPeriodSec(0)
     *   <li>BlockBasedTableConfig.setBlockCacheSize(256 MBytes)
     *   <li>BlockBasedTableConfigsetBlockSize(128 KBytes)
     * </ul>
     *
     * <p>Note: Because Flink does not rely on RocksDB data on disk for recovery, there is no need
     * to sync data to stable storage.
     */
    SPINNING_DISK_OPTIMIZED_HIGH_MEM {

        @Override
        public DBOptions createDBOptions(Collection<AutoCloseable> handlesToClose) {
            return new DBOptions()
                    .setIncreaseParallelism(4)
                    .setUseFsync(false)
                    .setMaxOpenFiles(-1)
                    .setInfoLogLevel(InfoLogLevel.HEADER_LEVEL)
                    .setStatsDumpPeriodSec(0);
        }

        @Override
        public ColumnFamilyOptions createColumnOptions(Collection<AutoCloseable> handlesToClose) {

            final long blockCacheSize = 256 * 1024 * 1024;
            final long blockSize = 128 * 1024;
            final long targetFileSize = 256 * 1024 * 1024;
            final long writeBufferSize = 64 * 1024 * 1024;

            BloomFilter bloomFilter = new BloomFilter();
            handlesToClose.add(bloomFilter);

            return new ColumnFamilyOptions()
                    .setCompactionStyle(CompactionStyle.LEVEL)
                    .setLevelCompactionDynamicLevelBytes(true)
                    .setTargetFileSizeBase(targetFileSize)
                    .setMaxBytesForLevelBase(4 * targetFileSize)
                    .setWriteBufferSize(writeBufferSize)
                    .setMinWriteBufferNumberToMerge(3)
                    .setMaxWriteBufferNumber(4)
                    .setTableFormatConfig(
                            new BlockBasedTableConfig()
                                    .setBlockCacheSize(blockCacheSize)
                                    .setBlockSize(blockSize)
                                    .setFilter(bloomFilter));
        }
    },

    /**
     * Pre-defined options for Flash SSDs.
     *
     * <p>This constant configures RocksDB with some options that lead empirically to better
     * performance when the machines executing the system use SSDs.
     *
     * <p>The following options are set:
     *
     * <ul>
     *   <li>setIncreaseParallelism(4)
     *   <li>setUseFsync(false)
     *   <li>setDisableDataSync(true)
     *   <li>setMaxOpenFiles(-1)
     *   <li>setInfoLogLevel(InfoLogLevel.HEADER_LEVEL)
     *   <li>setStatsDumpPeriodSec(0)
     * </ul>
     *
     * <p>Note: Because Flink does not rely on RocksDB data on disk for recovery, there is no need
     * to sync data to stable storage.
     */
    FLASH_SSD_OPTIMIZED {

        @Override
        public DBOptions createDBOptions(Collection<AutoCloseable> handlesToClose) {
            return new DBOptions()
                    .setIncreaseParallelism(4)
                    .setUseFsync(false)
                    .setMaxOpenFiles(-1)
                    .setInfoLogLevel(InfoLogLevel.HEADER_LEVEL)
                    .setStatsDumpPeriodSec(0);
        }

        @Override
        public ColumnFamilyOptions createColumnOptions(Collection<AutoCloseable> handlesToClose) {
            return new ColumnFamilyOptions();
        }
    },

    COMPACTION_DISABLED {
        @Override
        public DBOptions createDBOptions(Collection<AutoCloseable> handlesToClose) {
            return new DBOptions()
                    .setUseFsync(false)
                    .setInfoLogLevel(InfoLogLevel.INFO_LEVEL)
                    .setStatsDumpPeriodSec(180);
        }

        @Override
        public ColumnFamilyOptions createColumnOptions(Collection<AutoCloseable> handlesToClose) {
            return new ColumnFamilyOptions()
                    .setDisableAutoCompactions(true)
                    .setCompressionType(CompressionType.NO_COMPRESSION)
                    .setLevel0StopWritesTrigger(100000)
                    .setLevel0SlowdownWritesTrigger(100000)
                    .setMaxCompactionBytes(25 * 64 * 1024 * 1024L);
        }
    };

    // ------------------------------------------------------------------------

    /**
     * Creates the {@link DBOptions}for this pre-defined setting.
     *
     * @param handlesToClose The collection to register newly created {@link
     *     org.rocksdb.RocksObject}s.
     * @return The pre-defined options object.
     */
    public abstract DBOptions createDBOptions(Collection<AutoCloseable> handlesToClose);

    /**
     * @return The pre-defined options object.
     * @deprecated use {@link #createColumnOptions(Collection)} instead.
     */
    public DBOptions createDBOptions() {
        return createDBOptions(new ArrayList<>());
    }

    /**
     * Creates the {@link org.rocksdb.ColumnFamilyOptions}for this pre-defined setting.
     *
     * @param handlesToClose The collection to register newly created {@link
     *     org.rocksdb.RocksObject}s.
     * @return The pre-defined options object.
     */
    public abstract ColumnFamilyOptions createColumnOptions(
            Collection<AutoCloseable> handlesToClose);

    /**
     * @return The pre-defined options object.
     * @deprecated use {@link #createColumnOptions(Collection)} instead.
     */
    public ColumnFamilyOptions createColumnOptions() {
        return createColumnOptions(new ArrayList<>());
    }
}
