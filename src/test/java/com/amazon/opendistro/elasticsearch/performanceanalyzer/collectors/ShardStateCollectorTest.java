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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides.ConfigOverridesWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ShardStateCollectorTest extends CustomMetricsLocationTestBase {

    @Test
    public void testShardsStateMetrics() {
        MetricsConfiguration.CONFIG_MAP.put(ShardStateCollector.class, MetricsConfiguration.cdefault);
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");
        long startTimeInMills = 1153721339;
        PerformanceAnalyzerController controller = Mockito.mock(PerformanceAnalyzerController.class);
        ConfigOverridesWrapper configOverrides = Mockito.mock(ConfigOverridesWrapper.class);
        Mockito.when(controller.isCollectorEnabled(configOverrides, "ShardStateCollector"))
                .thenReturn(true);
        ShardStateCollector shardsStateCollector = new ShardStateCollector(controller, configOverrides);
        shardsStateCollector.saveMetricValues("shard_state_metrics", startTimeInMills);
        String fetchedValue = PerformanceAnalyzerMetrics.getMetric(PluginSettings.instance().getMetricsLocation()
                + PerformanceAnalyzerMetrics.getTimeInterval(startTimeInMills)+"/shard_state_metrics/");
        PerformanceAnalyzerMetrics.removeMetrics(PluginSettings.instance().getMetricsLocation()
                + PerformanceAnalyzerMetrics.getTimeInterval(startTimeInMills));
        assertEquals("shard_state_metrics", fetchedValue);

        try {
            shardsStateCollector.saveMetricValues("shard_state_metrics", startTimeInMills, "123");
            assertTrue("Negative scenario test: Should have been a RuntimeException", true);
        } catch (RuntimeException ex) {
            //- expecting exception...1 values passed; 0 expected
        }
    }
}

