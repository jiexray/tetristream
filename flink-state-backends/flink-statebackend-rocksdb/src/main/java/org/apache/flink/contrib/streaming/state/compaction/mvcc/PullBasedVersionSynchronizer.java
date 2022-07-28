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

import org.apache.flink.contrib.streaming.state.compaction.util.ColumnFamilyInfo;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyMetaData;
import org.rocksdb.RocksDB;
import org.rocksdb.SstFileMetaData;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

public class PullBasedVersionSynchronizer extends VersionSynchronizer {
    @Override
    public void syncRocksDBAndUpdateVersion(
            BiConsumer<Map<Integer, List<SstFileMetaData>>, ColumnFamilyInfo> syncCallback) {
        for (ColumnFamilyInfo columnFamilyInfo : columnFamilyInfos) {
            RocksDB db = columnFamilyInfo.getDb();
            ColumnFamilyHandle stateHandle = columnFamilyInfo.getStateHandle();
            ColumnFamilyMetaData cfmd = db.getColumnFamilyMetaData(stateHandle);

            Map<Integer, List<SstFileMetaData>> files = new LinkedHashMap<>();
            for (int level = 0; level < cfmd.levels().size(); level++) {
                List<SstFileMetaData> levelFiles = cfmd.levels().get(level).files();
                if (levelFiles == null) {
                    levelFiles = new ArrayList<>();
                }
                files.put(level, levelFiles);
            }
            syncCallback.accept(files, columnFamilyInfo);
        }
    }
}
