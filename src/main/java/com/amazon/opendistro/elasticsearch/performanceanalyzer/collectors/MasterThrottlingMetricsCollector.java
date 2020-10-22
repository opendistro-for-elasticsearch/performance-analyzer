/*
 *  Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PerformanceAnalyzerController;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides.ConfigOverridesWrapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ESResources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.ExceptionsAndErrors;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.WriterMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.elasticsearch.cluster.service.MasterService;

import java.lang.reflect.Method;

public class MasterThrottlingMetricsCollector extends PerformanceAnalyzerMetricsCollector implements MetricsProcessor {

    public static final int SAMPLING_TIME_INTERVAL =
            MetricsConfiguration.CONFIG_MAP.get(MasterThrottlingMetricsCollector.class).samplingInterval;
    private static final Logger LOG = LogManager.getLogger(MasterThrottlingMetricsCollector.class);
    private static final int KEYS_PATH_LENGTH = 0;
    private static final String MASTER_THROTTLING_RETRY_LISTENER_PATH =
            "org.elasticsearch.action.support.master.MasterThrottlingRetryListener";
    private static final String THROTTLED_PENDING_TASK_COUNT_METHOD_NAME = "numberOfThrottledPendingTasks";
    private static final String RETRYING_TASK_COUNT_METHOD_NAME = "getRetryingTasksCount";
    private final StringBuilder value;
    private final PerformanceAnalyzerController controller;
    private final ConfigOverridesWrapper configOverridesWrapper;

    public MasterThrottlingMetricsCollector(PerformanceAnalyzerController controller,
                                            ConfigOverridesWrapper configOverridesWrapper) {
        super(SAMPLING_TIME_INTERVAL, "MasterThrottlingMetricsCollector");
        value = new StringBuilder();
        this.controller = controller;
        this.configOverridesWrapper = configOverridesWrapper;
    }

    @Override
    void collectMetrics(long startTime) {
        if(!controller.isCollectorEnabled(configOverridesWrapper, getCollectorName())) {
            return;
        }
        try {
            long mCurrT = System.currentTimeMillis();
            if (ESResources.INSTANCE.getClusterService() == null
                    || ESResources.INSTANCE.getClusterService().getMasterService() == null) {
                return;
            }
            if(!isMasterThrottlingFeatureAvailable()) {
                LOG.debug("Master Throttling Feature is not available for this domain");
                PerformanceAnalyzerApp.WRITER_METRICS_AGGREGATOR.updateStat(
                        WriterMetrics.MASTER_THROTTLING_COLLECTOR_NOT_AVAILABLE, "", 1);
                return;
            }

            value.setLength(0);
            value.append(PerformanceAnalyzerMetrics.getJsonCurrentMilliSeconds())
                    .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor);
            value.append(new MasterThrottlingMetrics(
                    getRetryingPendingTaskCount(),
                    getTotalMasterThrottledTaskCount()).serialize());

            saveMetricValues(value.toString(), startTime);

            PerformanceAnalyzerApp.WRITER_METRICS_AGGREGATOR.updateStat(
                    WriterMetrics.MASTER_THROTTLING_COLLECTOR_EXECUTION_TIME, "",
                    System.currentTimeMillis() - mCurrT);

        } catch (Exception ex) {
            PerformanceAnalyzerApp.ERRORS_AND_EXCEPTIONS_AGGREGATOR.updateStat(
                    ExceptionsAndErrors.MASTER_THROTTLING_COLLECTOR_ERROR, "", 1);
            LOG.debug("Exception in Collecting Master Throttling Metrics: {} for startTime {}", () -> ex.toString(), () -> startTime);
        }
    }

    private boolean isMasterThrottlingFeatureAvailable() {
        try {
            Class.forName(MASTER_THROTTLING_RETRY_LISTENER_PATH);
            MasterService.class.getMethod(THROTTLED_PENDING_TASK_COUNT_METHOD_NAME);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            return false;
        }
        return true;
    }

    private long getTotalMasterThrottledTaskCount() throws Exception {
        Method method = MasterService.class.getMethod(THROTTLED_PENDING_TASK_COUNT_METHOD_NAME);
        return (long) method.invoke(ESResources.INSTANCE.getClusterService().getMasterService());
    }

    private long getRetryingPendingTaskCount() throws Exception {
        Method method = Class.forName(MASTER_THROTTLING_RETRY_LISTENER_PATH).getMethod(RETRYING_TASK_COUNT_METHOD_NAME);
        return (long) method.invoke(null);
    }

    @Override
    public String getMetricsPath(long startTime, String... keysPath) {
        if (keysPath.length != KEYS_PATH_LENGTH) {
            throw new RuntimeException("keys length should be " + KEYS_PATH_LENGTH);
        }

        return PerformanceAnalyzerMetrics.generatePath(startTime, PerformanceAnalyzerMetrics.sMasterThrottledTasksPath);
    }

    public static class MasterThrottlingMetrics extends MetricStatus {
        private final long retryingTaskCount;
        private final long throttledPendingTasksCount;

        public MasterThrottlingMetrics(long retryingTaskCount, long throttledPendingTasksCount) {
            this.retryingTaskCount = retryingTaskCount;
            this.throttledPendingTasksCount = throttledPendingTasksCount;
        }

        @JsonProperty(AllMetrics.MasterThrottlingValue.Constants.RETRYING_TASK_COUNT)
        public long getRetryingTaskCount() {
            return retryingTaskCount;
        }

        @JsonProperty(AllMetrics.MasterThrottlingValue.Constants.THROTTLED_PENDING_TASK_COUNT)
        public long getThrottledPendingTasksCount() {
            return throttledPendingTasksCount;
        }
    }
}
