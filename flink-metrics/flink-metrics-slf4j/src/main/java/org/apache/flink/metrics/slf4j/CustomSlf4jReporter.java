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

package org.apache.flink.metrics.slf4j;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ConcurrentModificationException;
import java.util.Map;

/** Customized metrics reporter. */
public class CustomSlf4jReporter extends AbstractReporter implements Scheduled {
    private static final Logger LOG = LoggerFactory.getLogger(CustomSlf4jReporter.class);
    private static final Logger MetricsLog = LoggerFactory.getLogger("analytics");
    private static final String lineSeparator = System.lineSeparator();
    private static final String WordSeparator = "! ";

    private int previousSize = 16384;

    private enum RequiredMetricName {
        // job level
        StateSize("lastCheckpointIncSize"),
        TotalStateSize("lastCheckpointSize"),

        // task level
        InPoolUsage("inPoolUsage"),
        OutPoolUsage("outPoolUsage"),
        NumRecordsInPerSecond("numRecordsInPerSecond"),
        NumRecordsOutPerSecond("numRecordsOutPerSecond"),

        // operator level
        Cfstats("cfstats"),
        SnapshotSyncDuration("snapshotSyncDuration"),
        SnapshotAsyncDuration("snapshotAsyncDuration");


        String name;

        RequiredMetricName(String name) {
            this.name = name;
        }
    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        final String name = group.getMetricIdentifier(metricName, this);

        boolean isRequired = false;
        for (RequiredMetricName requiredMetricName : RequiredMetricName.values()) {
            if (name.contains(requiredMetricName.name)) {
                LOG.info("Add Metrics {}", name);
                isRequired = true;
                break;
            }
        }

        if (!isRequired) {
            return;
        }

        super.notifyOfAddedMetric(metric, metricName, group);
    }

    @Override
    public void open(MetricConfig config) {}

    @Override
    public void close() {}

    @Override
    public void report() {
        try {
            tryReport();
        } catch (ConcurrentModificationException ignored) {
            // at tryReport() we don't synchronize while iterating over the various maps which might
            // cause a
            // ConcurrentModificationException to be thrown, if concurrently a metric is being added
            // or removed.
        }
    }

    private void tryReport() {
        StringBuilder builder = new StringBuilder((int) (previousSize * 1.1));

        builder.append(lineSeparator)
                .append(
                        "-- Counters -------------------------------------------------------------------")
                .append(lineSeparator);
        for (Map.Entry<Counter, String> metric : counters.entrySet()) {
            builder.append(metric.getValue())
                    .append(WordSeparator)
                    .append(metric.getKey().getCount())
                    .append(WordSeparator)
                    .append(System.currentTimeMillis())
                    .append(lineSeparator);
        }

        builder.append(lineSeparator)
                .append(
                        "-- Gauges ---------------------------------------------------------------------")
                .append(lineSeparator);
        for (Map.Entry<Gauge<?>, String> metric : gauges.entrySet()) {
            builder.append(metric.getValue())
                    .append(WordSeparator)
                    .append(metric.getKey().getValue())
                    .append(WordSeparator)
                    .append(System.currentTimeMillis())
                    .append(lineSeparator);
        }

        builder.append(lineSeparator)
                .append(
                        "-- Meters ---------------------------------------------------------------------")
                .append(lineSeparator);
        for (Map.Entry<Meter, String> metric : meters.entrySet()) {
            builder.append(metric.getValue())
                    .append(WordSeparator)
                    .append(metric.getKey().getRate())
                    .append(WordSeparator)
                    .append(System.currentTimeMillis())
                    .append(lineSeparator);
        }

        for (Map.Entry<Histogram, String> metric : histograms.entrySet()) {
            HistogramStatistics stats = metric.getKey().getStatistics();
            builder.append(metric.getValue())
                    .append(WordSeparator)
                    .append("count=")
                    .append(stats.size())
                    .append(", min=")
                    .append(stats.getMin())
                    .append(", max=")
                    .append(stats.getMax())
                    .append(", mean=")
                    .append(stats.getMean())
                    .append(", stddev=")
                    .append(stats.getStdDev())
                    .append(", p50=")
                    .append(stats.getQuantile(0.50))
                    .append(", p75=")
                    .append(stats.getQuantile(0.75))
                    .append(", p95=")
                    .append(stats.getQuantile(0.95))
                    .append(", p98=")
                    .append(stats.getQuantile(0.98))
                    .append(", p99=")
                    .append(stats.getQuantile(0.99))
                    .append(", p999=")
                    .append(stats.getQuantile(0.999))
                    .append(WordSeparator)
                    .append(System.currentTimeMillis())
                    .append(lineSeparator);
        }

        MetricsLog.info(builder.toString());
        previousSize = builder.length();
    }

    @Override
    public String filterCharacters(String input) {
        return input;
    }
}
