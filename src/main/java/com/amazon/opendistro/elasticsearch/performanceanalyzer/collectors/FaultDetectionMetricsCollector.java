/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PerformanceAnalyzerController;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides.ConfigOverridesWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.ExceptionsAndErrors;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.WriterMetrics;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.tools.StringUtils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics.addMetricEntry;

public class FaultDetectionMetricsCollector extends PerformanceAnalyzerMetricsCollector implements MetricsProcessor {
    public static final int SAMPLING_TIME_INTERVAL = MetricsConfiguration.CONFIG_MAP.
            get(FaultDetectionMetricsCollector.class).samplingInterval;
    private static final int KEYS_PATH_LENGTH = 3;
    private static final Logger LOG = LogManager.getLogger(FaultDetectionMetricsCollector.class);
    private static final String FAULT_DETECTION_HANDLER_NAME =
            "com.amazon.opendistro.elasticsearch.performanceanalyzer.handler.ClusterFaultDetectionStatsHandler";
    private static final String FAULT_DETECTION_HANDLER_METRIC_QUEUE = "metricQueue";
    private final ConfigOverridesWrapper configOverridesWrapper;
    private final PerformanceAnalyzerController controller;
    private StringBuilder value;
    private static final ObjectMapper mapper = new ObjectMapper();

    public FaultDetectionMetricsCollector(PerformanceAnalyzerController controller,
                                          ConfigOverridesWrapper configOverridesWrapper) {
        super(SAMPLING_TIME_INTERVAL, "FaultDetectionMetricsCollector");
        value = new StringBuilder();
        this.configOverridesWrapper = configOverridesWrapper;
        this.controller = controller;
    }

    @Override
    @SuppressWarnings("unchecked")
    void collectMetrics(long startTime) {
        if(!controller.isCollectorEnabled(configOverridesWrapper, getCollectorName())) {
            return;
        }
        long mCurrT = System.currentTimeMillis();
        Class<?> faultDetectionHandler = null;
        try {
            faultDetectionHandler = Class.forName(FAULT_DETECTION_HANDLER_NAME);
        } catch (ClassNotFoundException e) {
            LOG.debug("No Handler Detected for Fault Detection. Skipping FaultDetectionMetricsCollector");
            return;
        }
        try {
            BlockingQueue<String> metricQueue = (BlockingQueue<String>)
                    getFaultDetectionHandlerMetricsQueue(faultDetectionHandler).get(null);
            List<String> metrics = new ArrayList<>();
            metricQueue.drainTo(metrics);

            List<ClusterFaultDetectionContext> faultDetectionContextsList = new ArrayList<>();
            for(String metric : metrics) {
                faultDetectionContextsList.add(mapper.readValue(metric, ClusterFaultDetectionContext.class));
            }

            for(ClusterFaultDetectionContext clusterFaultDetectionContext : faultDetectionContextsList) {
                value.setLength(0);
                value.append(PerformanceAnalyzerMetrics.getCurrentTimeMetric());
                addMetricEntry(value, AllMetrics.FaultDetectionDimension.SOURCE_NODE_ID
                        .toString(), clusterFaultDetectionContext.getSourceNodeId());
                addMetricEntry(value, AllMetrics.FaultDetectionDimension.TARGET_NODE_ID
                        .toString(), clusterFaultDetectionContext.getTargetNodeId());

                if(StringUtils.isEmpty(clusterFaultDetectionContext.getStartTime())) {
                    addMetricEntry(value, AllMetrics.CommonMetric.FINISH_TIME.toString(),
                            clusterFaultDetectionContext.getFinishTime());
                    addMetricEntry(value, PerformanceAnalyzerMetrics.FAULT,
                            clusterFaultDetectionContext.getFault());
                    saveMetricValues(value.toString(), startTime, clusterFaultDetectionContext.getType(),
                            clusterFaultDetectionContext.getRequestId(), PerformanceAnalyzerMetrics.FINISH_FILE_NAME);
                } else {
                    addMetricEntry(value, AllMetrics.CommonMetric.START_TIME.toString(),
                            clusterFaultDetectionContext.getStartTime());
                    saveMetricValues(value.toString(), startTime, clusterFaultDetectionContext.getType(),
                            clusterFaultDetectionContext.getRequestId(), PerformanceAnalyzerMetrics.START_FILE_NAME);
                }
            }
            PerformanceAnalyzerApp.WRITER_METRICS_AGGREGATOR.updateStat(
                    WriterMetrics.FAULT_DETECTION_COLLECTOR_EXECUTION_TIME, "",
                    System.currentTimeMillis() - mCurrT);
        } catch (Exception ex) {
            PerformanceAnalyzerApp.ERRORS_AND_EXCEPTIONS_AGGREGATOR.updateStat(
                    ExceptionsAndErrors.FAULT_DETECTION_COLLECTOR_ERROR, "",
                    System.currentTimeMillis() - mCurrT);
            LOG.debug("Exception in Collecting FaultDetection Metrics: {} for startTime {}",
                    () -> ex.toString(), () -> startTime);
        }
    }

    Field getFaultDetectionHandlerMetricsQueue(Class<?> faultDetectionHandler) throws Exception {
        Field metricsQueue = faultDetectionHandler.getDeclaredField(FAULT_DETECTION_HANDLER_METRIC_QUEUE);
        metricsQueue.setAccessible(true);
        return metricsQueue;
    }

    /** Sample Event
     * ^fault_detection/follower_check/7627/finish
     * current_time:1601486201861
     * SourceNodeID:g52i9a93a762cd59dda8d3379b09a752a
     * TargetNodeID:b2a5a93a762cd59dda8d3379b09a752a
     * FinishTime:1566413987986
     * fault:0$
     *
     * @param startTime time at which collector is called
     * @param keysPath List of string that would make up the metrics path
     * @return metric path
     */
    @Override
    public String getMetricsPath(long startTime, String... keysPath) {
        if (keysPath.length != KEYS_PATH_LENGTH) {
            throw new RuntimeException("keys length should be " + KEYS_PATH_LENGTH);
        }

        return PerformanceAnalyzerMetrics.generatePath(startTime, PerformanceAnalyzerMetrics.sFaultDetection,
                keysPath[0], keysPath[1], keysPath[2]);
    }

    public static class ClusterFaultDetectionContext {
        String type;
        String sourceNodeId;
        String targetNodeId;
        String requestId;
        String fault;
        String startTime;
        String finishTime;

        public String getType() {
            return this.type;
        }

        public String getSourceNodeId() {
            return this.sourceNodeId;
        }

        public String getTargetNodeId() {
            return this.targetNodeId;
        }

        public String getFault() {
            return this.fault;
        }

        public String getStartTime() {
            return this.startTime;
        }

        public String getFinishTime() {
            return this.finishTime;
        }

        public String getRequestId() {
            return this.requestId;
        }

    }
}