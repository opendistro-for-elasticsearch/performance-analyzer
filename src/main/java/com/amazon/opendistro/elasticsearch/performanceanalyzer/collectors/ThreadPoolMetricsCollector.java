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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ESResources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ThreadPoolDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ThreadPoolValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.SizeBlockingQueue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolStats.Stats;

public class ThreadPoolMetricsCollector extends PerformanceAnalyzerMetricsCollector implements MetricsProcessor {
    private static final Logger LOG = LogManager.getLogger(ThreadPoolMetricsCollector.class);
    public static final int SAMPLING_TIME_INTERVAL = MetricsConfiguration.CONFIG_MAP.get(ThreadPoolMetricsCollector.class).samplingInterval;
    private static final int KEYS_PATH_LENGTH = 0;
    private StringBuilder value;
    private final Map<String, ThreadPoolStatsRecord> statsRecordMap;

    public ThreadPoolMetricsCollector() {
        super(SAMPLING_TIME_INTERVAL, "ThreadPoolMetrics");
        value = new StringBuilder();
        statsRecordMap = new HashMap<>();
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
            long rejectionDelta = 0;
            String threadPoolName = stats.getName();
            if (statsRecordMap.containsKey(threadPoolName)) {
                ThreadPoolStatsRecord lastRecord = statsRecordMap.get(threadPoolName);
                // if the timestamp in previous record is greater than 15s (3 * intervals),
                // then the scheduler might hang or freeze due to long GC etc. We simply drop
                // previous record here and set rejectionDelta to 0.
                if (startTime - lastRecord.getTimestamp() <= SAMPLING_TIME_INTERVAL * 3) {
                    rejectionDelta = stats.getRejected() - lastRecord.getRejected();
                    // we might not run into this as rejection is a LongAdder which never decrement its count.
                    // regardless, let's set it to 0 to be safe.
                    if (rejectionDelta < 0) {
                        rejectionDelta = 0;
                    }
                }
            }
            statsRecordMap.put(threadPoolName, new ThreadPoolStatsRecord(startTime, stats.getRejected()));
            final long finalRejectionDelta = rejectionDelta;
            final int capacity = AccessController.doPrivileged((PrivilegedAction<Integer>) () -> {
                try {
                    ThreadPool threadPool = (ThreadPool) FieldUtils.readField(
                        ESResources.INSTANCE.getIndicesService(), "threadPool", true);
                    ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) threadPool.executor(threadPoolName);
                    Object queue = threadPoolExecutor.getQueue();
                    // TODO: we might want to read the capacity of SifiResizableBlockingQueue in the future.
                    // In order to do that we can create a new PerformanceAnalyzerLibrary package and push
                    // all the code which depends on core ES specific changes into that library.
                    if (queue instanceof SizeBlockingQueue) {
                        return ((SizeBlockingQueue)queue).capacity();
                    }
                } catch (Exception e) {
                    LOG.warn("Fail to read queue capacity via reflection");
                }
                return -1;
            });
            ThreadPoolStatus threadPoolStatus = new ThreadPoolStatus(stats.getName(),
                stats.getQueue(), finalRejectionDelta,
                stats.getThreads(), stats.getActive(),
                -1.0, capacity);
            value.append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor)
                    .append(threadPoolStatus.serialize());
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

    private static class ThreadPoolStatsRecord {
        private final long timestamp;
        private final long rejected;

        ThreadPoolStatsRecord(long timestamp, long rejected) {
            this.timestamp = timestamp;
            this.rejected = rejected;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public long getRejected() {
            return rejected;
        }
    }

    public static class ThreadPoolStatus extends MetricStatus {
        public final String type;
        public final int queueSize;
        public final long rejected;
        public final int threadsCount;
        public final int threadsActive;
        public final double queueLatency;
        public final int queueCapacity;

        public ThreadPoolStatus(String type,
            int queueSize,
            long rejected,
            int threadsCount,
            int threadsActive,
            double queueLatency,
            int queueCapacity) {
            this.type = type;
            this.queueSize = queueSize;
            this.rejected = rejected;
            this.threadsCount = threadsCount;
            this.threadsActive = threadsActive;
            this.queueLatency = queueLatency;
            this.queueCapacity = queueCapacity;
        }

        // default constructor for jackson to de-serialize this class
        // from json string in unit test
        @VisibleForTesting
        public ThreadPoolStatus() {
            this.type = "testing";
            this.queueSize = -1;
            this.rejected = -1;
            this.threadsCount = -1;
            this.threadsActive = -1;
            this.queueLatency = -1.0;
            this.queueCapacity = -1;
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

        @JsonProperty(ThreadPoolValue.Constants.QUEUE_LATENCY_VALUE)
        public double getQueueLatency() {
            return queueLatency;
        }

        @JsonProperty(ThreadPoolValue.Constants.QUEUE_CAPACITY_VALUE)
        public int getQueueCapacity() {
            return queueCapacity;
        }
    }
}
