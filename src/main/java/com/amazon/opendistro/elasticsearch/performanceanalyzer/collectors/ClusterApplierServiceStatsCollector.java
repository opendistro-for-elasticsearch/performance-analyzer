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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterApplierService;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class ClusterApplierServiceStatsCollector extends PerformanceAnalyzerMetricsCollector implements
        MetricsProcessor {
    public static final int SAMPLING_TIME_INTERVAL =
            MetricsConfiguration.CONFIG_MAP.get(ClusterApplierServiceStatsCollector.class).samplingInterval;
    private static final Logger LOG = LogManager.getLogger(ClusterApplierServiceStatsCollector.class);
    private static final int KEYS_PATH_LENGTH = 0;
    private static final String GET_CLUSTER_APPLIER_SERVICE_STATS_METHOD_NAME = "getStats";
    private final StringBuilder value;
    private final PerformanceAnalyzerController controller;
    private final ConfigOverridesWrapper configOverridesWrapper;
    private static final ObjectMapper mapper = new ObjectMapper();
    private MetricCache metricCache;

    public ClusterApplierServiceStatsCollector(PerformanceAnalyzerController controller,
                                               ConfigOverridesWrapper configOverridesWrapper) {
        super(SAMPLING_TIME_INTERVAL, "ClusterApplierServiceStatsCollector");
        value = new StringBuilder();
        this.controller = controller;
        this.configOverridesWrapper = configOverridesWrapper;
        this.metricCache = new MetricCache();
    }

    @Override
    public void collectMetrics(long startTime) {
        try {
            long mCurrT = System.currentTimeMillis();
            if (ESResources.INSTANCE.getClusterService() == null
                    || ESResources.INSTANCE.getClusterService().getClusterApplierService() == null) {
                return;
            }
            mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            ClusterApplierServiceStats metricsObject = null;
            try {
                metricsObject = mapper.readValue(
                        mapper.writeValueAsString(getClusterApplierServiceStats()), ClusterApplierServiceStats.class);
            } catch (InvocationTargetException | IllegalAccessException | NoSuchMethodException ex) {
                LOG.debug("No method found to get cluster state applier thread stats. " +
                        "Skipping ClusterApplierServiceStatsCollector");
                return;
            }

            Long latencyInMillis = computeLatency(metricsObject);
            Long failure = computeFailure(metricsObject);

            if(latencyInMillis != null && failure != null) {
                ClusterApplierServiceMetrics clusterApplierServiceMetrics = new
                        ClusterApplierServiceMetrics(latencyInMillis, failure);

                value.setLength(0);
                value.append(PerformanceAnalyzerMetrics.getJsonCurrentMilliSeconds())
                        .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor);
                value.append(clusterApplierServiceMetrics.serialize());
                saveMetricValues(value.toString(), startTime);
            }
            this.metricCache = new MetricCache(metricsObject.timeTakenInMillis, metricsObject.failedCount,
                    metricsObject.totalCount);
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

    private Long computeLatency(final ClusterApplierServiceStats metricsObject) {
        if(metricCache.prevTimeTakenInMillis == null || metricCache.prevTotalCount == null) {
            return null;
        }

        final Long rate = computeRate(metricsObject);
        if(rate == 0) {
            return 0L;
        }
        return (metricsObject.timeTakenInMillis - metricCache.prevTimeTakenInMillis) / rate;
    }

    private Long computeRate(final ClusterApplierServiceStats metricsObject) {
        return metricsObject.totalCount - metricCache.prevTotalCount;
    }

    private Long computeFailure(ClusterApplierServiceStats metricsObject) {
        if(metricCache.prevFailedCount != null) {
            return metricsObject.failedCount - metricCache.prevFailedCount;
        }
        return null;
    }

    private Object getClusterApplierServiceStats() throws InvocationTargetException, IllegalAccessException,
            NoSuchMethodException {
        Method method = ClusterApplierService.class.getMethod(GET_CLUSTER_APPLIER_SERVICE_STATS_METHOD_NAME);
        return method.invoke(ESResources.INSTANCE.getClusterService().getClusterApplierService());
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
        private long startTimeCurrent;
    }

    public static class ClusterApplierServiceMetrics extends MetricStatus {
        private long clusterStateAppliedFailedCount;
        private long clusterStateAppliedTimeInMillis;

        public ClusterApplierServiceMetrics(long clusterApplierServiceLatency, long clusterApplierServiceFailed) {
            this.clusterStateAppliedTimeInMillis = clusterApplierServiceLatency;
            this.clusterStateAppliedFailedCount = clusterApplierServiceFailed;
        }

        @JsonProperty(AllMetrics.ClusterApplierServiceStatsValue.Constants.CLUSTER_APPLIER_SERVICE_LATENCY)
        public long getClusterApplierServiceLatency() {
            return clusterStateAppliedTimeInMillis;
        }

        @JsonProperty(AllMetrics.ClusterApplierServiceStatsValue.Constants.CLUSTER_APPLIER_SERVICE_FAILURE)
        public long getClusterApplierServiceFailed() {
            return clusterStateAppliedFailedCount;
        }
    }

    private static class MetricCache {
        private Long prevTimeTakenInMillis;
        private Long prevFailedCount;
        private Long prevTotalCount;


        private MetricCache(long timeInMillis, long failedCount, long totalCount) {
            this.prevTimeTakenInMillis = timeInMillis;
            this.prevFailedCount = failedCount;
            this.prevTotalCount = totalCount;
        }

        private MetricCache() {
            this.prevTimeTakenInMillis = null;
            this.prevFailedCount = null;
            this.prevTotalCount = null;
        }
    }
}
