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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.ExceptionsAndErrors;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.WriterMetrics;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Method;
import java.util.Objects;

/**
 * AdmissionControlMetricsCollector collects `UsedQuota`, `TotalQuota`, RejectionCount
 */
public class AdmissionControlMetricsCollector extends PerformanceAnalyzerMetricsCollector implements MetricsProcessor {

    private static final Logger LOG = LogManager.getLogger(AdmissionControlMetricsCollector.class);
    private static final int sTimeInterval = MetricsConfiguration.SAMPLING_INTERVAL;
    private static final int KEYS_PATH_LENGTH = 0;
    private StringBuilder value;

    // Global JVM Memory Pressure Controller
    private final static String GLOBAL_JVMMP = "Global_JVMMP";

    // Request Size Controller
    private final static String REQUEST_SIZE = "Request_Size";

    private final static String ADMISSION_CONTROLLER =
            "com.sonian.elasticsearch.http.jetty.throttling.AdmissionController";

    private final static String ADMISSION_CONTROL_SERVICE =
            "com.sonian.elasticsearch.http.jetty.throttling.JettyAdmissionControlService";

    public AdmissionControlMetricsCollector() {
        super(sTimeInterval, "AdmissionControlMetricsCollector");
        this.value = new StringBuilder();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void collectMetrics(long startTime) {
        if(!isAdmissionControlFeatureAvailable()) {
            LOG.debug("AdmissionControl is not available for this domain");
            PerformanceAnalyzerApp.WRITER_METRICS_AGGREGATOR.updateStat(
                    WriterMetrics.ADMISSION_CONTROL_COLLECTOR_NOT_AVAILABLE, "", 1);
            return;
        }

        long startTimeMillis = System.currentTimeMillis();
        try {
            Class admissionController = Class.forName(ADMISSION_CONTROLLER);
            Class jettyAdmissionControlService = Class.forName(ADMISSION_CONTROL_SERVICE);

            Method getAdmissionController = jettyAdmissionControlService.getDeclaredMethod(
                    "getAdmissionController",
                    String.class);

            Object globalJVMMP = getAdmissionController.invoke(null, GLOBAL_JVMMP);
            Object requestSize = getAdmissionController.invoke(null, REQUEST_SIZE);

            if(Objects.isNull(globalJVMMP) && Objects.isNull(requestSize)) {
                return;
            }

            value.setLength(0);

            Method getUsedQuota = admissionController.getDeclaredMethod("getUsedQuota");
            Method getTotalQuota = admissionController.getDeclaredMethod("getTotalQuota");
            Method getRejectionCount = admissionController.getDeclaredMethod("getRejectionCount");

            if(!Objects.isNull(globalJVMMP)) {
                value.append(PerformanceAnalyzerMetrics.getJsonCurrentMilliSeconds())
                        .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor)
                        .append(new AdmissionControlMetrics(
                                GLOBAL_JVMMP,
                                (long)getUsedQuota.invoke(globalJVMMP),
                                (long)getTotalQuota.invoke(globalJVMMP),
                                (long)getRejectionCount.invoke(globalJVMMP)
                        ).serialize());
            }

            if(!Objects.isNull(requestSize)) {
                value.append(PerformanceAnalyzerMetrics.getJsonCurrentMilliSeconds())
                        .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor)
                        .append(new AdmissionControlMetrics(
                                REQUEST_SIZE,
                                (long)getUsedQuota.invoke(requestSize),
                                (long)getTotalQuota.invoke(requestSize),
                                (long)getRejectionCount.invoke(requestSize)
                        ).serialize());
            }

            saveMetricValues(value.toString(), startTime);

            PerformanceAnalyzerApp.WRITER_METRICS_AGGREGATOR.updateStat(
                    WriterMetrics.ADMISSION_CONTROL_COLLECTOR_EXECUTION_TIME, "",
                    System.currentTimeMillis() - startTimeMillis);

        } catch(Exception ex) {
            PerformanceAnalyzerApp.ERRORS_AND_EXCEPTIONS_AGGREGATOR.updateStat(
                    ExceptionsAndErrors.ADMISSION_CONTROL_COLLECTOR_ERROR, getCollectorName(),
                    System.currentTimeMillis() - startTimeMillis);
            LOG.debug("Exception in collecting AdmissionControl Metrics: {} for startTime {}",
                    ex::toString, () -> startTime);
        }
    }

    @Override
    public String getMetricsPath(long startTime, String... keysPath) {
        if (keysPath.length != KEYS_PATH_LENGTH) {
            throw new RuntimeException("keys length should be " + KEYS_PATH_LENGTH);
        }
        return PerformanceAnalyzerMetrics.generatePath(startTime,
                PerformanceAnalyzerMetrics.sAdmissionControlMetricsPath);
    }

    static class AdmissionControlMetrics extends MetricStatus {

        private String controllerName;
        private long current;
        private long threshold;
        private long rejectionCount;

        public AdmissionControlMetrics() {
            super();
        }

        public AdmissionControlMetrics(String controllerName, long current, long threshold, long rejectionCount) {
            super();
            this.controllerName = controllerName;
            this.current = current;
            this.threshold = threshold;
            this.rejectionCount = rejectionCount;
        }

        @JsonProperty(AllMetrics.AdmissionControlDimension.Constants.CONTROLLER_NAME)
        public String getControllerName() {
            return controllerName;
        }

        @JsonProperty(AllMetrics.AdmissionControlValue.Constants.CURRENT_VALUE)
        public long getCurrent() {
            return current;
        }

        @JsonProperty(AllMetrics.AdmissionControlValue.Constants.THRESHOLD_VALUE)
        public long getThreshold() {
            return threshold;
        }

        @JsonProperty(AllMetrics.AdmissionControlValue.Constants.REJECTION_COUNT)
        public long getRejectionCount() {
            return rejectionCount;
        }
    }

    private boolean isAdmissionControlFeatureAvailable() {
        try {
            Class.forName(ADMISSION_CONTROLLER);
            Class.forName(ADMISSION_CONTROL_SERVICE);
        } catch (ClassNotFoundException e) {
            return false;
        }
        return true;
    }
}
