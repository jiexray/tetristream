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

package org.apache.flink.contrib.streaming.state.rescaling;

import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.util.IOUtils;

import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.FlinkRescalingCompactionFilter;
import org.rocksdb.FlinkRescalingCompactionFilter.FlinkRescalingCompactionFilterFactory;
import org.rocksdb.InfoLogLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.LinkedHashMap;

public class RocksDBRescalingCompactFiltersManager {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkRescalingCompactionFilter.class);

    private final boolean enableRescalingCompactionFilter;

    private final LinkedHashMap<String, FlinkRescalingCompactionFilterFactory>
            compactionFilterFactories;

    public RocksDBRescalingCompactFiltersManager(boolean enableRescalingCompactionFilter) {
        this.enableRescalingCompactionFilter = enableRescalingCompactionFilter;
        this.compactionFilterFactories = new LinkedHashMap<>();
    }

    public void setAndRegisterCompactFilterIfRescaling(
            @Nonnull RegisteredStateMetaInfoBase metaInfoBase,
            @Nonnull ColumnFamilyOptions options) {

        if (enableRescalingCompactionFilter
                && metaInfoBase instanceof RegisteredKeyValueStateBackendMetaInfo) {
            RegisteredKeyValueStateBackendMetaInfo kvMetaInfoBase =
                    (RegisteredKeyValueStateBackendMetaInfo) metaInfoBase;
            FlinkRescalingCompactionFilterFactory compactionFilterFactory =
                    new FlinkRescalingCompactionFilterFactory(createRocksDbNativeLogger());

            options.setCompactionFilterFactory(compactionFilterFactory);
            compactionFilterFactories.put(kvMetaInfoBase.getName(), compactionFilterFactory);
        }
    }

    private static org.rocksdb.Logger createRocksDbNativeLogger() {
        if (LOG.isDebugEnabled()) {
            try (DBOptions opts = new DBOptions().setInfoLogLevel(InfoLogLevel.DEBUG_LEVEL)) {
                return new org.rocksdb.Logger(opts) {
                    @Override
                    protected void log(InfoLogLevel infoLogLevel, String logMsg) {
                        LOG.debug("RocksDB filter native code log: " + logMsg);
                    }
                };
            }
        } else {
            return null;
        }
    }

    public void configCompactFilter(
            String stateName, int keyGroupPrefixBytes, KeyGroupRange keyGroupRange) {
        if (enableRescalingCompactionFilter) {
            FlinkRescalingCompactionFilterFactory compactionFilterFactory =
                    compactionFilterFactories.get(stateName);
            if (compactionFilterFactory == null) {
                return;
            }

            compactionFilterFactory.configure(
                    FlinkRescalingCompactionFilter.Config.createForOne(
                            keyGroupRange.getStartKeyGroup(),
                            keyGroupRange.getEndKeyGroup(),
                            keyGroupPrefixBytes));
        }
    }

    public void disposeAndClearRegisteredCompactionFactories() {
        for (FlinkRescalingCompactionFilterFactory factory : compactionFilterFactories.values()) {
            IOUtils.closeQuietly(factory);
        }
        compactionFilterFactories.clear();
    }
}
