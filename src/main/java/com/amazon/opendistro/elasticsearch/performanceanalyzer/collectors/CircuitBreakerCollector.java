/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import org.elasticsearch.indices.breaker.CircuitBreakerStats;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ESResources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CircuitBreakerDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CircuitBreakerValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CircuitBreakerCollector extends PerformanceAnalyzerMetricsCollector implements MetricsProcessor {
    public static final int SAMPLING_TIME_INTERVAL = MetricsConfiguration.CONFIG_MAP.get(CircuitBreakerCollector.class).samplingInterval;
    private static final int KEYS_PATH_LENGTH = 0;
    private StringBuilder value;

    public CircuitBreakerCollector() {
        super(SAMPLING_TIME_INTERVAL, "CircuitBreaker");
        value = new StringBuilder();
    }

    @Override
    public void collectMetrics(long startTime) {
        if (ESResources.INSTANCE.getCircuitBreakerService() == null) {
            return;
        }

        CircuitBreakerStats[] allCircuitBreakerStats = ESResources.INSTANCE.getCircuitBreakerService().stats().getAllStats();
        //- Reusing the same StringBuilder across exectuions; so clearing before using
        value.setLength(0);
        value.append(PerformanceAnalyzerMetrics.getJsonCurrentMilliSeconds());

        for (CircuitBreakerStats stats : allCircuitBreakerStats) {
            value.append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor)
                    .append(new CircuitBreakerStatus(stats.getName(),
                            stats.getEstimated(), stats.getTrippedCount(),
                            stats.getLimit()).serialize());
        }

        saveMetricValues(value.toString(), startTime);
    }

    @Override
    public String getMetricsPath(long startTime, String... keysPath) {
        // throw exception if keys.length is not equal to 0
        if (keysPath.length != KEYS_PATH_LENGTH) {
            throw new RuntimeException("keys length should be " + KEYS_PATH_LENGTH);
        }

        return PerformanceAnalyzerMetrics.generatePath(startTime, PerformanceAnalyzerMetrics.sCircuitBreakerPath);
    }

    static class CircuitBreakerStatus extends MetricStatus {
        private final String type;

        private final long estimated;

        private final long tripped;

        private final long limitConfigured;

        CircuitBreakerStatus(String type, long estimated, long tripped, long limitConfigured) {
            this.type = type;
            this.estimated = estimated;
            this.tripped = tripped;
            this.limitConfigured = limitConfigured;
        }

        @JsonProperty(CircuitBreakerDimension.Constants.TYPE_VALUE)
        public String getType() {
            return type;
        }

        @JsonProperty(CircuitBreakerValue.Constants.ESTIMATED_VALUE)
        public long getEstimated() {
            return estimated;
        }

        @JsonProperty(CircuitBreakerValue.Constants.TRIPPED_VALUE)
        public long getTripped() {
            return tripped;
        }

        @JsonProperty(CircuitBreakerValue.Constants.LIMIT_CONFIGURED_VALUE)
        public long getLimitConfigured() {
            return limitConfigured;
        }


    }
}

