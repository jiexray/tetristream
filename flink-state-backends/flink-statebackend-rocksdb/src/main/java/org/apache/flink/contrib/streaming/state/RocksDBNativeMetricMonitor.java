/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.View;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A monitor which pulls {{@link RocksDB}} native metrics and forwards them to Flink's metric group.
 * All metrics are unsigned longs and are reported at the column family level.
 */
@Internal
public class RocksDBNativeMetricMonitor implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBNativeMetricMonitor.class);

    private final RocksDBNativeMetricOptions options;

    private final MetricGroup metricGroup;

    private final Object lock;

    static final String COLUMN_FAMILY_KEY = "column_family";

    private static final String COMPACTION_COUNT_PROPERTY_TEMPLATE = "compaction.%s.CompCount";
    private static final String WRITE_STALL_PROPERTY_TEMPLATE = "io_stalls.%s";
    private static final String COMPACTION_TIME_PROPERTY_TEMPLATE = "compaction.%s.CompSec";
    private static final String COMPACTION_CPU_TIME_PROPERTY_TEMPLATE =
            "compaction.%s.CompMergeCPU";
    private static final long ROCKSDB_METRICS_DUMP_INTERVAL = 20000L;

    @GuardedBy("lock")
    private RocksDB rocksDB;

    public RocksDBNativeMetricMonitor(
            @Nonnull RocksDBNativeMetricOptions options,
            @Nonnull MetricGroup metricGroup,
            @Nonnull RocksDB rocksDB) {
        this.options = options;
        this.metricGroup = metricGroup;
        this.rocksDB = rocksDB;

        this.lock = new Object();
    }

    /**
     * Register gauges to pull native metrics for the column family.
     *
     * @param columnFamilyName group name for the new gauges
     * @param handle native handle to the column family
     */
    void registerColumnFamily(String columnFamilyName, ColumnFamilyHandle handle) {

        boolean columnFamilyAsVariable = options.isColumnFamilyAsVariable();
        MetricGroup group =
                columnFamilyAsVariable
                        ? metricGroup.addGroup(COLUMN_FAMILY_KEY, columnFamilyName)
                        : metricGroup.addGroup(columnFamilyName);

        for (String property : options.getProperties()) {
            RocksDBNativeMetricView gauge = new RocksDBNativeMetricView(handle, property);
            group.gauge(property, gauge);
        }

        if (options.isEnableCfstats()) {
            RocksDBNativeMapMetricView gauge =
                    new RocksDBNativeMapMetricView(
                            handle, RocksDBProperty.Cfstats.getRocksDBProperty(), columnFamilyName);
            group.gauge("cfstats", gauge);
        }
    }

    /** Updates the value of metricView if the reference is still valid. */
    private void setProperty(
            ColumnFamilyHandle handle, String property, RocksDBNativeMetricView metricView) {
        if (metricView.isClosed()) {
            return;
        }
        try {
            synchronized (lock) {
                if (rocksDB != null) {
                    long value = rocksDB.getLongProperty(handle, property);
                    metricView.setValue(value);
                }
            }
        } catch (RocksDBException e) {
            metricView.close();
            LOG.warn("Failed to read native metric {} from RocksDB.", property, e);
        }
    }

    private Map<String, String> getMapProperty(
            ColumnFamilyHandle handle, String property, RocksDBNativeMapMetricView metricView) {
        if (metricView.isClosed()) {
            return null;
        }

        try {
            synchronized (lock) {
                return rocksDB.getMapProperty(handle, property);
            }
        } catch (RocksDBException e) {
            metricView.close();
            LOG.warn("Failed to read native metric {} from RocksDB.", property, e);
        }
        return null;
    }

    @Override
    public void close() {
        synchronized (lock) {
            rocksDB = null;
        }
    }

    /**
     * A gauge which periodically pulls a RocksDB native metric for the specified column family /
     * metric pair.
     *
     * <p><strong>Note</strong>: As the returned property is of type {@code uint64_t} on C++ side
     * the returning value can be negative. Because java does not support unsigned long types, this
     * gauge wraps the result in a {@link BigInteger}.
     */
    class RocksDBNativeMetricView implements Gauge<BigInteger>, View {
        private final String property;

        private final ColumnFamilyHandle handle;

        private BigInteger bigInteger;

        private boolean closed;

        private RocksDBNativeMetricView(ColumnFamilyHandle handle, @Nonnull String property) {
            this.handle = handle;
            this.property = property;
            this.bigInteger = BigInteger.ZERO;
            this.closed = false;
        }

        public void setValue(long value) {
            if (value >= 0L) {
                bigInteger = BigInteger.valueOf(value);
            } else {
                int upper = (int) (value >>> 32);
                int lower = (int) value;

                bigInteger =
                        BigInteger.valueOf(Integer.toUnsignedLong(upper))
                                .shiftLeft(32)
                                .add(BigInteger.valueOf(Integer.toUnsignedLong(lower)));
            }
        }

        public void close() {
            closed = true;
        }

        public boolean isClosed() {
            return closed;
        }

        @Override
        public BigInteger getValue() {
            return bigInteger;
        }

        @Override
        public void update() {
            setProperty(handle, property, this);
        }
    }

    class RocksDBNativeMapMetricView implements Gauge<CfstatsInfo>, View {
        private final ColumnFamilyHandle handle;

        private final String property;

        private final String stateName;

        private Map<String, Long> lastLongResult;
        private Map<String, Double> lastDoubleResult;

        private AtomicLong lastUpdateTimestamp = new AtomicLong(0L);

        private boolean closed;

        public RocksDBNativeMapMetricView(
                ColumnFamilyHandle handle, String property, String stateName) {
            this.handle = handle;
            this.property = property;
            this.stateName = stateName;
        }

        @Override
        public CfstatsInfo getValue() {
            long currentTs = System.currentTimeMillis();
            long lastTs = lastUpdateTimestamp.get();

            boolean isSkip = true;
            while (currentTs - lastTs > ROCKSDB_METRICS_DUMP_INTERVAL) {
                if (lastUpdateTimestamp.compareAndSet(lastTs, currentTs)) {
                    isSkip = false;
                    break;
                }
                lastTs = lastUpdateTimestamp.get();
            }

            if (isSkip) {
                return null;
            }
            Map<String, String> result = getMapProperty(handle, property, this);

            if (result == null) {
                return null;
            }

            // get Long properties
            Map<String, Long> parseLongResult = parseLongCfstats(result);
            Map<String, Long> deltaLongResult = new HashMap<>();
            for (Map.Entry<String, Long> entry : parseLongResult.entrySet()) {
                if (lastLongResult == null) {
                    deltaLongResult.put(entry.getKey(), entry.getValue());
                } else {
                    Long valInLastResult = lastLongResult.get(entry.getKey());
                    deltaLongResult.put(
                            entry.getKey(),
                            entry.getValue() - (valInLastResult == null ? 0L : valInLastResult));
                }
            }
            lastLongResult = parseLongResult;

            // get Double properties
            Map<String, Double> parseDoubleResult = parseDoubleCfstats(result);
            Map<String, Double> deltaDoubleResult = new HashMap<>();
            for (Map.Entry<String, Double> entry : parseDoubleResult.entrySet()) {
                if (lastDoubleResult == null) {
                    deltaDoubleResult.put(entry.getKey(), entry.getValue());
                } else {
                    Double valInLastResult = lastDoubleResult.get(entry.getKey());
                    deltaDoubleResult.put(
                            entry.getKey(),
                            entry.getValue() - (valInLastResult == null ? 0 : valInLastResult));
                }
            }
            lastDoubleResult = parseDoubleResult;

            return new CfstatsInfo(deltaLongResult, deltaDoubleResult);
        }

        @Override
        public void update() {}

        public void close() {
            closed = true;
        }

        public boolean isClosed() {
            return closed;
        }
    }

    Map<String, Long> parseLongCfstats(Map<String, String> result) {
        Map<String, Long> parseInfo = new HashMap<>();
        long totalCompaction = 0L;
        for (Level level : Level.values()) {
            String property = String.format(COMPACTION_COUNT_PROPERTY_TEMPLATE, level.val);
            String strVal = result.getOrDefault(property, "0");
            long compactionCount = (long) Double.parseDouble(strVal);
            totalCompaction += compactionCount;
            parseInfo.put(property, compactionCount);
        }
        parseInfo.put(String.format(COMPACTION_COUNT_PROPERTY_TEMPLATE, "ALL"), totalCompaction);

        for (WriteStallCause cause : WriteStallCause.values()) {
            String property = String.format(WRITE_STALL_PROPERTY_TEMPLATE, cause.val);
            String strVal = result.getOrDefault(property, "0");
            parseInfo.put(property, Long.parseLong(strVal));
        }
        return parseInfo;
    }

    Map<String, Double> parseDoubleCfstats(Map<String, String> result) {
        Map<String, Double> parseInfo = new HashMap<>();
        double totalTime = 0;
        double totalCpuTime = 0;

        for (Level level : Level.values()) {
            String timeProperty = String.format(COMPACTION_TIME_PROPERTY_TEMPLATE, level.val);
            String cpuTimeProperty =
                    String.format(COMPACTION_CPU_TIME_PROPERTY_TEMPLATE, level.val);
            String timeVal = result.getOrDefault(timeProperty, "0");
            String cpuTimeVal = result.getOrDefault(cpuTimeProperty, "0");
            parseInfo.put(timeProperty, Double.parseDouble(timeVal));
            parseInfo.put(cpuTimeProperty, Double.parseDouble(cpuTimeVal));

            totalTime += Double.parseDouble(timeVal);
            totalCpuTime += Double.parseDouble(cpuTimeVal);
        }
        parseInfo.put(String.format(COMPACTION_TIME_PROPERTY_TEMPLATE, "ALL"), totalTime);
        parseInfo.put(String.format(COMPACTION_CPU_TIME_PROPERTY_TEMPLATE, "ALL"), totalCpuTime);

        return parseInfo;
    }

    enum Level {
        L0("L0"),
        L1("L1"),
        L2("L2"),
        L3("L3"),
        L4("L4"),
        L5("L5"),
        L6("L6"),
        L7("L7");
        String val;

        Level(String val) {
            this.val = val;
        }
    }

    enum WriteStallCause {
        Level0Slowdown("level0_slowdown"),
        Level0SlowdownWithCompaction("level0_slowdown_with_compaction"),
        Level0Numfiles("level0_numfiles"),
        Level0NumfileWithCompaction("level0_numfiles_with_compaction"),
        StopForPendingCompactionBytes("stop_for_pending_compaction_bytes"),
        SlowdownForPendingCompactionBytes("slowdown_for_pending_compaction_bytes"),
        MemtableCompaction("memtable_compaction"),
        MemtableSlowdown("memtable_slowdown"),
        TotalSlowdown("total_slowdown"),
        TotalStop("total_stop");
        String val;

        WriteStallCause(String val) {
            this.val = val;
        }
    }

    static class CfstatsInfo implements Serializable {
        final Map<String, Long> infos;
        final Map<String, Double> doubleInfos;

        public CfstatsInfo(Map<String, Long> infos, Map<String, Double> doubleInfos) {
            this.infos = infos;
            this.doubleInfos = doubleInfos;
        }

        public Map<String, Long> getInfos() {
            return infos;
        }

        @Override
        public String toString() {
            return "CfstatsInfo{" + "infos=" + infos + ", doubleInfos=" + doubleInfos + '}';
        }
    }
}
