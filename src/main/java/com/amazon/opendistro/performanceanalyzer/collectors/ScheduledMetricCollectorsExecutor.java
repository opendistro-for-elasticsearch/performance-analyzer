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

import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.amazon.opendistro.performanceanalyzer.http_action.config.PerformanceAnalyzerConfigAction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ScheduledMetricCollectorsExecutor extends Thread {
    private static final Logger LOG = LogManager.getLogger(ScheduledMetricCollectorsExecutor.class);
    private static final int COLLECTOR_THREAD_COUNT = 5;
    private static final int COLLECTOR_THREAD_KEEPALIVE_SECS = 1000;

    private int minTimeIntervalToSleep = Integer.MAX_VALUE;
    private Map<PerformanceAnalyzerMetricsCollector, Long> metricsCollectors;
    private ThreadPoolExecutor metricsCollectorsTP;

    public ScheduledMetricCollectorsExecutor() {
        metricsCollectors = new HashMap<>();
        metricsCollectorsTP = null;
    }

    public void addScheduledMetricCollector(PerformanceAnalyzerMetricsCollector task) {
        metricsCollectors.put(task, System.currentTimeMillis() + task.getTimeInterval());
        if (task.getTimeInterval() < minTimeIntervalToSleep) {
            minTimeIntervalToSleep = task.getTimeInterval();
        }
    }

    public void run() {
        if (metricsCollectorsTP == null) {
            //- todo - threadpool sizing
            metricsCollectorsTP = new ThreadPoolExecutor(COLLECTOR_THREAD_COUNT,
                    COLLECTOR_THREAD_COUNT,
                    COLLECTOR_THREAD_KEEPALIVE_SECS,
                    TimeUnit.SECONDS,
                    new ArrayBlockingQueue<>(metricsCollectors.size()));
        }

        long prevStartTimestamp = System.currentTimeMillis();

        while (true) {
            try {
                long millisToSleep = minTimeIntervalToSleep - System.currentTimeMillis() + prevStartTimestamp;
                if (millisToSleep > 0) {
                    Thread.sleep(millisToSleep);
                }
            } catch (Exception ex) {
                LOG.error("Exception in Thread Sleep", ex);
            }

            prevStartTimestamp = System.currentTimeMillis();

            if (PerformanceAnalyzerConfigAction.getInstance() != null && PerformanceAnalyzerConfigAction.getInstance().isFeatureEnabled()) {
                long currentTime = System.currentTimeMillis();

                for (Map.Entry<PerformanceAnalyzerMetricsCollector, Long> entry : metricsCollectors.entrySet()) {
                    if (entry.getValue() <= currentTime) {
                        PerformanceAnalyzerMetricsCollector collector = entry.getKey();
                        metricsCollectors.put(collector, entry.getValue() + collector.getTimeInterval());
                        if (!collector.inProgress()) {
                            collector.setStartTime(currentTime);
                            metricsCollectorsTP.execute(collector);
                        } else {
                            LOG.info("Collector {} is still in progress, so skipping this Interval", collector.getCollectorName());
                        }
                    }
                }
            }
        }
    }
}
