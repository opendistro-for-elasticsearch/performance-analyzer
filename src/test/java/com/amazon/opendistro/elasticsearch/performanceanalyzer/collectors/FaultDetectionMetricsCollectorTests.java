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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.CustomMetricsLocationTestBase;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PerformanceAnalyzerController;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides.ConfigOverridesWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FaultDetectionMetricsCollectorTests extends CustomMetricsLocationTestBase {

    @Test
    public void testFaultDetectionMetrics() {
        MetricsConfiguration.CONFIG_MAP.put(FaultDetectionMetricsCollector.class, MetricsConfiguration.cdefault);
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");
        long startTimeInMills = 1153721339;
        PerformanceAnalyzerController controller = Mockito.mock(PerformanceAnalyzerController.class);
        ConfigOverridesWrapper configOverrides = Mockito.mock(ConfigOverridesWrapper.class);
        FaultDetectionMetricsCollector faultDetectionMetricsCollector = new FaultDetectionMetricsCollector(
                controller, configOverrides);
        Mockito.when(controller.isCollectorEnabled(configOverrides, "FaultDetectionMetricsCollector"))
                .thenReturn(true);
        faultDetectionMetricsCollector.saveMetricValues("fault_detection", startTimeInMills,
                "follower_check", "65432", "start");
        List<Event> metrics =  new ArrayList<>();
        PerformanceAnalyzerMetrics.metricQueue.drainTo(metrics);

        assertEquals(1, metrics.size());
        assertEquals("fault_detection", metrics.get(0).value);

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

