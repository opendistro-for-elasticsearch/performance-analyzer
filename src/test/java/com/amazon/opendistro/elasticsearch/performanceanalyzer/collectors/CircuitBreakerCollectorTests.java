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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors;

import org.junit.Test;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.CustomMetricsLocationTestBase;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import static org.junit.Assert.assertEquals;

public class CircuitBreakerCollectorTests extends CustomMetricsLocationTestBase {

    @Test
    public void testCircuitBreakerMetrics() {
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");
        long startTimeInMills = 1153721339;
        CircuitBreakerCollector circuitBreakerCollector = new CircuitBreakerCollector();
        circuitBreakerCollector.saveMetricValues("werjbdsiviewur", startTimeInMills);
        String fetchedValue = PerformanceAnalyzerMetrics.getMetric(PluginSettings.instance().getMetricsLocation()
                + PerformanceAnalyzerMetrics.getTimeInterval(startTimeInMills)+"/circuit_breaker/");
        PerformanceAnalyzerMetrics.removeMetrics(PluginSettings.instance().getMetricsLocation()
                 + PerformanceAnalyzerMetrics.getTimeInterval(startTimeInMills));
        assertEquals("werjbdsiviewur", fetchedValue);

        try {
            circuitBreakerCollector.saveMetricValues("12321.5464", startTimeInMills, "123");
            assertEquals(true, true);
        } catch (RuntimeException ex) {
            //- expecting exception...1 values passed; 0 expected
        }

        try {
            circuitBreakerCollector.getMetricsPath(startTimeInMills, "123", "x");
            assertEquals(true, true);
        } catch (RuntimeException ex) {
            //- expecting exception...2 values passed; 0 expected
        }
    }
}
