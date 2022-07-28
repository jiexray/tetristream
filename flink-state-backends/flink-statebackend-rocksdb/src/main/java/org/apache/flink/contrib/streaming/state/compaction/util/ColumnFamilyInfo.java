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

package org.apache.flink.contrib.streaming.state.compaction.util;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.contrib.streaming.state.compaction.CompactionPicker;
import org.apache.flink.contrib.streaming.state.compaction.picker.LevelCompactionPicker;
import org.apache.flink.runtime.state.KeyGroupRange;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.RocksDB;

import java.util.Objects;

public class ColumnFamilyInfo {
    private final String stateName;
    private final ColumnFamilyHandle stateHandle;
    private final ColumnFamilyOptions columnFamilyOptions;

    private final KeyGroupRange keyGroupRange;
    private final int keyGroupPrefixBytes;

    private final RocksDB db;
    private CompactionPicker compactionPicker;

    private int maxNonEmptyLevelAfterFileIngestion;

    public ColumnFamilyInfo(
            String stateName,
            ColumnFamilyHandle stateHandle,
            KeyGroupRange keyGroupRange,
            int keyGroupPrefixBytes,
            RocksDB db,
            ColumnFamilyOptions columnFamilyOptions) {

        this.stateName = stateName;
        this.stateHandle = stateHandle;
        this.db = db;

        this.keyGroupRange = keyGroupRange;
        this.keyGroupPrefixBytes = keyGroupPrefixBytes;
        this.columnFamilyOptions = columnFamilyOptions;

        compactionPicker = new LevelCompactionPicker(columnFamilyOptions);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ColumnFamilyInfo that = (ColumnFamilyInfo) o;

        return Objects.equals(stateName, that.stateName);
    }

    @Override
    public String toString() {
        return "ColumnFamilyInfo{"
                + "stateName='"
                + stateName
                + '\''
                + ", keyGroupRange="
                + keyGroupRange
                + '}';
    }

    @Override
    public int hashCode() {
        return stateName != null ? stateName.hashCode() : 0;
    }

    public String getStateName() {
        return stateName;
    }

    public ColumnFamilyHandle getStateHandle() {
        return stateHandle;
    }

    public RocksDB getDb() {
        return db;
    }

    public int getKeyGroupPrefixBytes() {
        return keyGroupPrefixBytes;
    }

    public KeyGroupRange getKeyGroupRange() {
        return keyGroupRange;
    }

    public ColumnFamilyOptions getColumnFamilyOptions() {
        return columnFamilyOptions;
    }

    public void setCompactionPicker(CompactionPicker compactionPicker) {
        this.compactionPicker = compactionPicker;
    }

    public CompactionPicker getCompactionPicker() {
        return compactionPicker;
    }

    public int getMaxNonEmptyLevelAfterFileIngestion() {
        return maxNonEmptyLevelAfterFileIngestion;
    }

    public void setMaxNonEmptyLevelAfterFileIngestion(int maxNonEmptyLevelAfterFileIngestion) {
        this.maxNonEmptyLevelAfterFileIngestion = maxNonEmptyLevelAfterFileIngestion;
    }
}
