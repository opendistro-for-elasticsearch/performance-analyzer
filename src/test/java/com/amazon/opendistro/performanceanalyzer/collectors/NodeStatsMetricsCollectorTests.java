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

package com.amazon.opendistro.performanceanalyzer.collectors;

import com.amazon.opendistro.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NodeStatsMetricsCollectorTests {

    @Test
    public void testNodeStatsMetrics() {
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");
        long startTimeInMills = System.currentTimeMillis() + 3 * 6000000;

        NodeStatsMetricsCollector nodeStatsMetricsCollector = new NodeStatsMetricsCollector();
        nodeStatsMetricsCollector.saveMetricValues("89123.23", startTimeInMills, "NodesStatsIndex", "55");


        String fetchedValue = PerformanceAnalyzerMetrics.getMetric(
                PerformanceAnalyzerMetrics.sDevShmLocation
                        + PerformanceAnalyzerMetrics.getTimeInterval(startTimeInMills)+"/indices/NodesStatsIndex/55/");
        PerformanceAnalyzerMetrics.removeMetrics(PerformanceAnalyzerMetrics.sDevShmLocation);
        assertEquals("89123.23", fetchedValue);

        try {
            nodeStatsMetricsCollector.saveMetricValues("89123.23", startTimeInMills, "NodesStatsIndex");
            assertTrue("Negative scenario test: Should have been a RuntimeException", true);
        } catch (RuntimeException ex) {
            //- expecting exception...only 1 values passed; 2 expected
        }

        try {
            nodeStatsMetricsCollector.saveMetricValues("89123.23", startTimeInMills);
            assertTrue("Negative scenario test: Should have been a RuntimeException", true);
        } catch (RuntimeException ex) {
            //- expecting exception...only 0 values passed; 2 expected
        }

        try {
            nodeStatsMetricsCollector.saveMetricValues("89123.23", startTimeInMills, "NodesStatsIndex", "55", "123");
            assertTrue("Negative scenario test: Should have been a RuntimeException", true);
        } catch (RuntimeException ex) {
            //- expecting exception...only 3 values passed; 2 expected
        }

        try {
            nodeStatsMetricsCollector.getNodeIndicesStatsByShardField();
        } catch (Exception exception) {
            assertTrue("There shouldn't be any exception in the code; Please check the reflection code for any changes", true);
        }
    }
}
