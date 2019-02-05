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

import java.util.Iterator;

import org.elasticsearch.threadpool.ThreadPoolStats.Stats;

import com.amazon.opendistro.performanceanalyzer.ESResources;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.ThreadPoolDimension;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.ThreadPoolValue;
import com.amazon.opendistro.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.performanceanalyzer.metrics.MetricsProcessor;
import com.amazon.opendistro.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ThreadPoolMetricsCollector extends PerformanceAnalyzerMetricsCollector implements MetricsProcessor {
    public static final int SAMPLING_TIME_INTERVAL = MetricsConfiguration.CONFIG_MAP.get(ThreadPoolMetricsCollector.class).samplingInterval;
    private static final int KEYS_PATH_LENGTH = 0;
    private StringBuilder value;

    public ThreadPoolMetricsCollector() {
        super(SAMPLING_TIME_INTERVAL, "ThreadPoolMetrics");
        value = new StringBuilder();
    }

    @Override
    public void collectMetrics(long startTime) {
        if (ESResources.INSTANCE.getThreadPool() == null) {
            return;
        }

        Iterator<Stats> statsIterator = ESResources.INSTANCE.getThreadPool().stats().iterator();
        value.setLength(0);
        value.append(PerformanceAnalyzerMetrics.getJsonCurrentMilliSeconds());

        while (statsIterator.hasNext()) {
            Stats stats = statsIterator.next();
            value.append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor)
                    .append(new ThreadPoolStatus(stats.getName(),
                            stats.getQueue(), stats.getRejected(),
                            stats.getThreads(), stats.getActive()).serialize());
        }

        saveMetricValues(value.toString(), startTime);
    }

    @Override
    public String getMetricsPath(long startTime, String... keysPath) {
        // throw exception if keys.length is not equal to 0
        if (keysPath.length != KEYS_PATH_LENGTH) {
            throw new RuntimeException("keys length should be " + KEYS_PATH_LENGTH);
        }

        return PerformanceAnalyzerMetrics.generatePath(startTime, PerformanceAnalyzerMetrics.sThreadPoolPath);
    }

    public static class ThreadPoolStatus extends MetricStatus {
        private String type;
        private int queueSize;
        private long rejected;
        private int threadsCount;
        private int threadsActive;

        public ThreadPoolStatus(String type,
                                int queueSize,
                                long rejected,
                                int threadsCount,
                                int threadsActive) {
            this.type = type;
            this.queueSize = queueSize;
            this.rejected = rejected;
            this.threadsCount = threadsCount;
            this.threadsActive = threadsActive;
        }

        @JsonProperty(ThreadPoolDimension.Constants.TYPE_VALUE)
        public String getType() {
            return type;
        }

        @JsonProperty(ThreadPoolValue.Constants.QUEUE_SIZE_VALUE)
        public int getQueueSize() {
            return queueSize;
        }

        @JsonProperty(ThreadPoolValue.Constants.REJECTED_VALUE)
        public long getRejected() {
            return rejected;
        }

        @JsonProperty(ThreadPoolValue.Constants.THREADS_COUNT_VALUE)
        public int getThreadsCount() {
            return threadsCount;
        }

        @JsonProperty(ThreadPoolValue.Constants.THREADS_ACTIVE_VALUE)
        public int getThreadsActive() {
            return threadsActive;
        }
    }
}
