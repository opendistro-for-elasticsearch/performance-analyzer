/*
 * Copyright <2019> Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistro.performanceanalyzer.collectors;

import java.lang.management.MemoryUsage;
import java.util.Map;
import java.util.function.Supplier;

import com.amazon.opendistro.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazon.opendistro.performanceanalyzer.jvm.GCMetrics;
import com.amazon.opendistro.performanceanalyzer.jvm.HeapMetrics;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.GCType;
import com.amazon.opendistro.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.performanceanalyzer.metrics.MetricsProcessor;


public class HeapMetricsCollector extends PerformanceAnalyzerMetricsCollector implements MetricsProcessor {
    private static final Logger LOG = LogManager.getLogger(HeapMetricsCollector.class);
    public static final int SAMPLING_TIME_INTERVAL = MetricsConfiguration.CONFIG_MAP.get(HeapMetricsCollector.class).samplingInterval;
    private static final int KEYS_PATH_LENGTH = 0;
    private StringBuilder value;


    public HeapMetricsCollector() {
        super(SAMPLING_TIME_INTERVAL, "HeapMetrics");
        value = new StringBuilder();
    }

    @Override
    public void collectMetrics(long startTime) {
        GCMetrics.runGCMetrics();

        value.setLength(0);
        value.append(PerformanceAnalyzerMetrics.getJsonCurrentMilliSeconds())
                .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor);
        value.append(new HeapStatus(GCType.totYoungGC.name(),
                GCMetrics.totYoungGCCollectionCount,
                GCMetrics.totYoungGCCollectionTime).serialize()).append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor);

        value.append(new HeapStatus(GCType.totFullGC.name(),
                GCMetrics.totFullGCCollectionCount,
                GCMetrics.totFullGCCollectionTime).serialize()).append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor);

        for (Map.Entry<String, Supplier<MemoryUsage>> entry : HeapMetrics
                .getMemoryUsageSuppliers().entrySet()) {
            MemoryUsage memoryUsage = entry.getValue().get();

            value.append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(
                    new HeapStatus(entry.getKey(),
                            memoryUsage.getCommitted(),
                            memoryUsage.getInit(),
                            memoryUsage.getMax(),
                            memoryUsage.getUsed()).serialize()).append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor);
        }

        saveMetricValues(value.toString(), startTime);
    }

    @Override
    public String getMetricsPath(long startTime, String... keysPath) {
        // throw exception if keys.length is not equal to 0
        if (keysPath.length != KEYS_PATH_LENGTH) {
            throw new RuntimeException("keys length should be " + KEYS_PATH_LENGTH);
        }

        return PerformanceAnalyzerMetrics.generatePath(startTime, PerformanceAnalyzerMetrics.sHeapPath);
    }

    public static class HeapStatus extends MetricStatus {
        // GC type like survivor
        public final String type;

        // the total number of collections that have occurred
        public long collectionCount = -2;

        // the approximate accumulated collection elapsed time in milliseconds
        public long collectionTime = -2;

        // the amount of memory in bytes that is committed for the Java virtual machine to use
        public long committed = -2;

        // the amount of memory in bytes that the Java virtual machine initially requests from the operating system for memory management
        public long init = -2;

        // the maximum amount of memory in bytes that can be used for memory management
        public long max = -2;

        // the amount of used memory in bytes
        public long used = -2;

        public HeapStatus(String type,
                          long collectionCount,
                          long collectionTime) {


            this.type = type;
            this.collectionCount = collectionCount;
            this.collectionTime = collectionTime;
        }

        public HeapStatus(String type,
                          long committed,
                          long init,
                          long max,
                          long used) {

            this.type = type;
            this.committed = committed;
            this.init = init;
            this.max = max;
            this.used = used;

        }
    }
}
