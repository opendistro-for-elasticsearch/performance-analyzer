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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.CustomMetricsLocationTestBase;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PerformanceAnalyzerController;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides.ConfigOverridesWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.fasterxml.jackson.databind.cfg.ConfigOverrides;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FaultDetectionMetricsCollectorTest extends CustomMetricsLocationTestBase {
    @Test
    public void testShardsStateMetrics() {
        MetricsConfiguration.CONFIG_MAP.put(FaultDetectionMetricsCollector.class, MetricsConfiguration.cdefault);
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");
        long startTimeInMills = 1153721339;
        PerformanceAnalyzerController controller = Mockito.mock(PerformanceAnalyzerController.class);
        ConfigOverridesWrapper configOverrides = Mockito.mock(ConfigOverridesWrapper.class);
        Mockito.when(controller.isCollectorEnabled(configOverrides, "FaultDetectionMetricsCollector"))
                .thenReturn(true);
        FaultDetectionMetricsCollector faultDetectionMetricsCollector = new FaultDetectionMetricsCollector(
                controller, configOverrides);
        faultDetectionMetricsCollector.saveMetricValues("fault_detection", startTimeInMills,
                "follower_check", "65432", "start");
        String fetchedValue = PerformanceAnalyzerMetrics.getMetric(PluginSettings.instance().getMetricsLocation()
                + PerformanceAnalyzerMetrics.getTimeInterval(startTimeInMills)+"/fault_detection/");
        PerformanceAnalyzerMetrics.removeMetrics(PluginSettings.instance().getMetricsLocation()
                + PerformanceAnalyzerMetrics.getTimeInterval(startTimeInMills));
        assertEquals("fault_detection", fetchedValue);

        try {
            faultDetectionMetricsCollector.saveMetricValues("fault_detection_metrics", startTimeInMills);
            assertTrue("Negative scenario test: Should have been a RuntimeException", true);
        } catch (RuntimeException ex) {
            //- expecting exception...0 values passed; 3 expected
        }

        try {
            faultDetectionMetricsCollector.saveMetricValues("fault_detection_metrics", startTimeInMills,
                    "leader_check");
            assertTrue("Negative scenario test: Should have been a RuntimeException", true);
        } catch (RuntimeException ex) {
            //- expecting exception...1 values passed; 3 expected
        }

        try {
            faultDetectionMetricsCollector.saveMetricValues("fault_detection_metrics", startTimeInMills,
                    "leader_check", "823765423");
            assertTrue("Negative scenario test: Should have been a RuntimeException", true);
        } catch (RuntimeException ex) {
            //- expecting exception...2 values passed; 0 expected
        }
    }
}

