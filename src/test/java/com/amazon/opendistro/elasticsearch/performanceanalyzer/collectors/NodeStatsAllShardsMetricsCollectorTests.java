/*
 * Copyright <2020> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

//@Ignore
public class NodeStatsAllShardsMetricsCollectorTests extends ESSingleNodeTestCase {
    private NodeStatsAllShardsMetricsCollector nodeStatsAllShardsMetricsCollector;

    @Before
    public void init() {
        ESResources.INSTANCE.setIndicesService(getInstanceFromNode(IndicesService.class));

        MetricsConfiguration.CONFIG_MAP.put(NodeStatsAllShardsMetricsCollector.class, MetricsConfiguration.cdefault);
        nodeStatsAllShardsMetricsCollector = new NodeStatsAllShardsMetricsCollector(null);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Ignore
    @Test
    public void testNodeStatsMetrics() {
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");
        long startTimeInMills = 1253722339;
        



        nodeStatsAllShardsMetricsCollector.saveMetricValues("89123.23", startTimeInMills, "NodesStatsIndex", "55");


        String fetchedValue = PerformanceAnalyzerMetrics.getMetric(
                PluginSettings.instance().getMetricsLocation()
                        + PerformanceAnalyzerMetrics.getTimeInterval(startTimeInMills)+"/indices/NodesStatsIndex/55/");
        PerformanceAnalyzerMetrics.removeMetrics(PluginSettings.instance().getMetricsLocation()
                 + PerformanceAnalyzerMetrics.getTimeInterval(startTimeInMills));
        assertEquals("89123.23", fetchedValue);

        try {
            nodeStatsAllShardsMetricsCollector.saveMetricValues("89123.23", startTimeInMills, "NodesStatsIndex");
            assertTrue("Negative scenario test: Should have been a RuntimeException", true);
        } catch (RuntimeException ex) {
            //- expecting exception...only 1 values passed; 2 expected
        }

        try {
            nodeStatsAllShardsMetricsCollector.saveMetricValues("89123.23", startTimeInMills);
            assertTrue("Negative scenario test: Should have been a RuntimeException", true);
        } catch (RuntimeException ex) {
            //- expecting exception...only 0 values passed; 2 expected
        }

        try {
            nodeStatsAllShardsMetricsCollector.saveMetricValues("89123.23", startTimeInMills, "NodesStatsIndex", "55", "123");
            assertTrue("Negative scenario test: Should have been a RuntimeException", true);
        } catch (RuntimeException ex) {
            //- expecting exception...only 3 values passed; 2 expected
        }

        try {
            nodeStatsAllShardsMetricsCollector.getNodeIndicesStatsByShardField();
        } catch (Exception exception) {
            assertTrue("There shouldn't be any exception in the code; Please check the reflection code for any changes", true);
        }
    }

    @Test
    public void testCollectMetrics() {
        long startTimeInMills = 1153721339;
        nodeStatsAllShardsMetricsCollector.collectMetrics(startTimeInMills);
        nodeStatsAllShardsMetricsCollector.collectMetrics(startTimeInMills + 500) ;
    }


}
