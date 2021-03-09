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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.Utils;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.ExceptionsAndErrors;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.WriterMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.tracker.MetricsTracker;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.coordination.FollowersChecker;
import org.elasticsearch.cluster.coordination.LeaderChecker;
import org.elasticsearch.discovery.Discovery;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class FaultDetectionStatsCollector extends PerformanceAnalyzerMetricsCollector implements
        MetricsProcessor {
    public static final int SAMPLING_TIME_INTERVAL =
            MetricsConfiguration.CONFIG_MAP.get(FaultDetectionStatsCollector.class).samplingInterval;
    private static final int KEYS_PATH_LENGTH = 0;
    private static final Logger LOG = LogManager.getLogger(FaultDetectionStatsCollector.class);
    public static final String FAULT_DETECTION_STATS_CLASS_NAME = "org.elasticsearch.cluster.coordination.FaultDetectionStats";
    private static final String GET_STATS_METHOD_NAME = "getStats";
    private static final String FOLLOWERS_CHECKER_FIELD = "followersChecker";
    private static final String LEADER_CHECKER_FIELD = "leaderChecker";
    private static final ObjectMapper mapper = new ObjectMapper();
    private final StringBuilder value;
    private final PerformanceAnalyzerController controller;
    private final ConfigOverridesWrapper configOverridesWrapper;

    private MetricsTracker leaderCheckTracker;
    private MetricsTracker followerCheckTracker;

    public FaultDetectionStatsCollector(PerformanceAnalyzerController controller,
                                               ConfigOverridesWrapper configOverridesWrapper) {
        super(SAMPLING_TIME_INTERVAL, FaultDetectionStatsCollector.class.getSimpleName());
        value = new StringBuilder();
        this.controller = controller;
        this.configOverridesWrapper = configOverridesWrapper;
        this.leaderCheckTracker = new MetricsTracker();
        this.followerCheckTracker = new MetricsTracker();
    }

    @Override
    public void collectMetrics(long startTime) {
        if(!controller.isCollectorEnabled(configOverridesWrapper, getCollectorName())) {
            return;
        }
        try {
            long mCurrT = System.currentTimeMillis();
            mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            FaultDetectionStats followerCheckStats = mapper.readValue(
                    mapper.writeValueAsString(getFollowerCheckStats()), FaultDetectionStats.class);
            FaultDetectionStats leaderCheckStats = mapper.readValue(
                    mapper.writeValueAsString(getLeaderCheckStats()), FaultDetectionStats.class);
            FaultDetectionMetrics faultDetectionMetrics = new FaultDetectionMetrics();
            if (followerCheckStats != null) {
                faultDetectionMetrics.setFollowerCheckMetrics(computeLatency(followerCheckStats, followerCheckTracker),
                        computeFailure(followerCheckStats, followerCheckTracker));
                this.followerCheckTracker = new MetricsTracker(followerCheckStats.timeTakenInMillis,
                        followerCheckStats.failedCount, followerCheckStats.totalCount);
            }
            if (leaderCheckStats != null) {
                faultDetectionMetrics.setLeaderCheckMetrics(computeLatency(leaderCheckStats, leaderCheckTracker),
                        computeFailure(leaderCheckStats, leaderCheckTracker));
                this.leaderCheckTracker = new MetricsTracker(leaderCheckStats.timeTakenInMillis,
                        leaderCheckStats.failedCount, leaderCheckStats.totalCount);
            }
            StringBuilder value = new StringBuilder();
            value.append(PerformanceAnalyzerMetrics.getJsonCurrentMilliSeconds())
                    .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor);
            value.append(faultDetectionMetrics.serialize());

            saveMetricValues(value.toString(), startTime);

            PerformanceAnalyzerApp.WRITER_METRICS_AGGREGATOR.updateStat(
                    WriterMetrics.FAULT_DETECTION_COLLECTOR_EXECUTION_TIME, "",
                    System.currentTimeMillis() - mCurrT);
        }
        catch (NoSuchFieldException | InvocationTargetException | IllegalAccessException | NoSuchMethodException
                | JsonProcessingException ex) {
            PerformanceAnalyzerApp.ERRORS_AND_EXCEPTIONS_AGGREGATOR.updateStat(
                    ExceptionsAndErrors.FAULT_DETECTION_COLLECTOR_ERROR, "", 1);
            LOG.debug("Exception in Collecting Fault Detection Stats: {} for startTime {}",
                    () -> ex.toString(), () -> startTime);
        }
    }

    private Object getLeaderCheckStats() throws InvocationTargetException, IllegalAccessException,
            NoSuchMethodException, NoSuchFieldException {
        Method method = LeaderChecker.class.getMethod(GET_STATS_METHOD_NAME);
        Discovery discovery = ESResources.INSTANCE.getDiscovery();
        if(discovery instanceof Coordinator) {
            Coordinator coordinator = (Coordinator) discovery;
            Field leaderCheckerField = Coordinator.class.getDeclaredField(LEADER_CHECKER_FIELD);
            leaderCheckerField.setAccessible(true);
            LeaderChecker leaderChecker = (LeaderChecker) leaderCheckerField.get(coordinator);
            return method.invoke(leaderChecker);
        }
        return null;
    }

    private Object getFollowerCheckStats() throws InvocationTargetException, IllegalAccessException,
            NoSuchMethodException, NoSuchFieldException {
        Method method = FollowersChecker.class.getMethod(GET_STATS_METHOD_NAME);
        Discovery discovery = ESResources.INSTANCE.getDiscovery();
        if(discovery instanceof Coordinator) {
            Coordinator coordinator = (Coordinator) discovery;
            Field followerCheckerField = Coordinator.class.getDeclaredField(FOLLOWERS_CHECKER_FIELD);
            followerCheckerField.setAccessible(true);
            FollowersChecker followersChecker = (FollowersChecker) followerCheckerField.get(coordinator);
            return method.invoke(followersChecker);
        }
        return null;
    }

    private double computeLatency(final FaultDetectionStats currentMetrics, MetricsTracker tracker) {
        final double rate = computeRate(currentMetrics.totalCount, tracker);
        if(rate == 0) {
            return 0D;
        }
        return (currentMetrics.timeTakenInMillis - tracker.getPrevTimeTakenInMillis()) / rate;
    }

    private double computeRate(final double currentTotalCount, MetricsTracker tracker) {
        return currentTotalCount - tracker.getPrevTotalCount();
    }

    private double computeFailure(final FaultDetectionStats currentMetrics, MetricsTracker tracker) {
        return currentMetrics.failedCount - tracker.getPrevFailedCount();
    }

    @Override
    public String getMetricsPath(long startTime, String... keysPath) {
        if (keysPath.length != KEYS_PATH_LENGTH) {
            throw new RuntimeException("keys length should be " + KEYS_PATH_LENGTH);
        }
        return PerformanceAnalyzerMetrics.generatePath(startTime, PerformanceAnalyzerMetrics.sFaultDetection);
    }

    public static class FaultDetectionStats  {
        private long totalCount;
        private long timeTakenInMillis;
        private long failedCount;
    }

    public static class FaultDetectionMetrics extends MetricStatus {
        private double followerCheckTimeTakenInMillis;
        private double leaderCheckTimeTakenInMillis;
        private double followerCheckFailedCount;
        private double leaderCheckFailedCount;

        public FaultDetectionMetrics() {

        }

        @JsonProperty(AllMetrics.FaultDetectionMetric.Constants.FOLLOWER_CHECK_LATENCY)
        public double getFollowerCheckTimeTakenInMillis() {
            return followerCheckTimeTakenInMillis;
        }

        @JsonProperty(AllMetrics.FaultDetectionMetric.Constants.FOLLOWER_CHECK_FAILURE)
        public double getFollowerCheckFailedCount() {
            return followerCheckFailedCount;
        }

        @JsonProperty(AllMetrics.FaultDetectionMetric.Constants.LEADER_CHECK_LATENCY)
        public double getLeaderCheckTimeTakenInMillis() {
            return leaderCheckTimeTakenInMillis;
        }

        @JsonProperty(AllMetrics.FaultDetectionMetric.Constants.LEADER_CHECK_FAILURE)
        public double getLeaderCheckFailedCount() {
            return leaderCheckFailedCount;
        }

        public void setFollowerCheckMetrics(double latency, double failed) {
            this.followerCheckTimeTakenInMillis = latency;
            this.followerCheckFailedCount = failed;
        }

        public void setLeaderCheckMetrics(double latency, double failed) {
            this.leaderCheckTimeTakenInMillis = latency;
            this.leaderCheckFailedCount = failed;
        }
    }
}
