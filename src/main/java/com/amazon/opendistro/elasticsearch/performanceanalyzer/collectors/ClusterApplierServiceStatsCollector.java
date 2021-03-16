/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PerformanceAnalyzerController;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides.ConfigOverridesWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.ExceptionsAndErrors;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.WriterMetrics;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterApplierService;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class ClusterApplierServiceStatsCollector extends PerformanceAnalyzerMetricsCollector implements
        MetricsProcessor {
    public static final int SAMPLING_TIME_INTERVAL =
            MetricsConfiguration.CONFIG_MAP.get(ClusterApplierServiceStatsCollector.class).samplingInterval;
    private static final int KEYS_PATH_LENGTH = 0;
    private static final Logger LOG = LogManager.getLogger(ClusterApplierServiceStatsCollector.class);
    private static final String GET_CLUSTER_APPLIER_SERVICE_STATS_METHOD_NAME = "getStats";
    private static final ObjectMapper mapper;
    private static volatile ClusterApplierServiceStats prevClusterApplierServiceStats = new ClusterApplierServiceStats();
    private final StringBuilder value;
    private final PerformanceAnalyzerController controller;
    private final ConfigOverridesWrapper configOverridesWrapper;

    static {
        mapper = new ObjectMapper();
        mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public ClusterApplierServiceStatsCollector(PerformanceAnalyzerController controller,
                                               ConfigOverridesWrapper configOverridesWrapper) {
        super(SAMPLING_TIME_INTERVAL, ClusterApplierServiceStatsCollector.class.getSimpleName());
        value = new StringBuilder();
        this.controller = controller;
        this.configOverridesWrapper = configOverridesWrapper;
    }

    @Override
    public void collectMetrics(long startTime) {
        if(!controller.isCollectorEnabled(configOverridesWrapper, getCollectorName())) {
            return;
        }
        try {
            long mCurrT = System.currentTimeMillis();
            if (ESResources.INSTANCE.getClusterService() == null
                    || ESResources.INSTANCE.getClusterService().getClusterApplierService() == null) {
                return;
            }
            ClusterApplierServiceStats currentClusterApplierServiceStats = null;
            try {
                currentClusterApplierServiceStats = mapper.readValue(
                        mapper.writeValueAsString(getClusterApplierServiceStats()), ClusterApplierServiceStats.class);
            } catch (InvocationTargetException | IllegalAccessException | NoSuchMethodException ex) {
                LOG.warn("No method found to get cluster state applier thread stats. " +
                        "Skipping ClusterApplierServiceStatsCollector");
                return;
            }
            ClusterApplierServiceMetrics clusterApplierServiceMetrics = new ClusterApplierServiceMetrics(
                    computeLatency(currentClusterApplierServiceStats), computeFailure(currentClusterApplierServiceStats));

            value.setLength(0);
            value.append(PerformanceAnalyzerMetrics.getJsonCurrentMilliSeconds())
                    .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor);
            value.append(clusterApplierServiceMetrics.serialize());
            saveMetricValues(value.toString(), startTime);

            ClusterApplierServiceStatsCollector.prevClusterApplierServiceStats = currentClusterApplierServiceStats;

            PerformanceAnalyzerApp.WRITER_METRICS_AGGREGATOR.updateStat(
                    WriterMetrics.CLUSTER_APPLIER_SERVICE_STATS_COLLECTOR_EXECUTION_TIME, "",
                    System.currentTimeMillis() - mCurrT);
        } catch (Exception ex) {
            PerformanceAnalyzerApp.ERRORS_AND_EXCEPTIONS_AGGREGATOR.updateStat(
                    ExceptionsAndErrors.CLUSTER_APPLIER_SERVICE_STATS_COLLECTOR_ERROR, "", 1);
            LOG.debug("Exception in Collecting Cluster Applier Service Metrics: {} for startTime {}",
                    () -> ex.toString(), () -> startTime);
        }
    }

    @VisibleForTesting
    public void resetPrevClusterApplierServiceStats() {
        ClusterApplierServiceStatsCollector.prevClusterApplierServiceStats = new ClusterApplierServiceStats();
    }

    @VisibleForTesting
    public Object getClusterApplierServiceStats() throws InvocationTargetException, IllegalAccessException,
            NoSuchMethodException {
        Method method = ClusterApplierService.class.getMethod(GET_CLUSTER_APPLIER_SERVICE_STATS_METHOD_NAME);
        return method.invoke(ESResources.INSTANCE.getClusterService().getClusterApplierService());
    }

    /**
     * ClusterApplierServiceStats is ES is a tracker for total time taken to apply cluster state and the
     * number of times it has failed. To calculate point in time metric,
     * we will have to store its previous state and calculate the diff to get the point in time latency.
     * This might return as 0 if there is no cluster update since last retrieval.
     *
     * @param currentMetrics Current Cluster update stats in ES
     * @return point in time latency.
     */
    private double computeLatency(final ClusterApplierServiceStats currentMetrics) {
        final double rate = computeRate(currentMetrics.totalCount);
        if(rate == 0) {
            return 0D;
        }
        return (currentMetrics.timeTakenInMillis - prevClusterApplierServiceStats.timeTakenInMillis) / rate;
    }

    private double computeRate(final double currentTotalCount) {
        return currentTotalCount - prevClusterApplierServiceStats.totalCount;
    }

    /**
     * ClusterApplierServiceStats is ES is a tracker for total time taken to apply cluster state and the
     * number of times it has failed. To calculate point in time metric,
     * we will have to store its previous state and calculate the diff to get the point in time failure.
     * This might return as 0 if there is no cluster update since last retrieval.
     *
     * @param currentMetrics Current Cluster update stats in ES
     * @return point in time failure.
     */
    private double computeFailure(final ClusterApplierServiceStats currentMetrics) {
        return currentMetrics.failedCount - prevClusterApplierServiceStats.failedCount;
    }

    @Override
    public String getMetricsPath(long startTime, String... keysPath) {
        if (keysPath.length != KEYS_PATH_LENGTH) {
            throw new RuntimeException("keys length should be " + KEYS_PATH_LENGTH);
        }
        return PerformanceAnalyzerMetrics.generatePath(startTime, PerformanceAnalyzerMetrics.sClusterApplierService);
    }

    public static class ClusterApplierServiceStats  {
        private long totalCount;
        private long timeTakenInMillis;
        private long failedCount;
        private long elapsedTimeCurrentInMillis;

        @VisibleForTesting
        public ClusterApplierServiceStats(long totalCount, long timeTakenInMillis, long failedCount,
                                          long elapsedTimeCurrentInMillis) {
            this.totalCount = totalCount;
            this.timeTakenInMillis = timeTakenInMillis;
            this.failedCount = failedCount;
            this.elapsedTimeCurrentInMillis = elapsedTimeCurrentInMillis;
        }

        public ClusterApplierServiceStats() {

        }
    }

    public static class ClusterApplierServiceMetrics extends MetricStatus {
        private double clusterStateAppliedFailedCount;
        private double clusterStateAppliedTimeInMillis;

        public ClusterApplierServiceMetrics(double clusterApplierServiceLatency, double clusterApplierServiceFailed) {
            this.clusterStateAppliedTimeInMillis = clusterApplierServiceLatency;
            this.clusterStateAppliedFailedCount = clusterApplierServiceFailed;
        }

        @JsonProperty(AllMetrics.ClusterApplierServiceStatsValue.Constants.CLUSTER_APPLIER_SERVICE_LATENCY)
        public double getClusterApplierServiceLatency() {
            return clusterStateAppliedTimeInMillis;
        }

        @JsonProperty(AllMetrics.ClusterApplierServiceStatsValue.Constants.CLUSTER_APPLIER_SERVICE_FAILURE)
        public double getClusterApplierServiceFailed() {
            return clusterStateAppliedFailedCount;
        }
    }
}
