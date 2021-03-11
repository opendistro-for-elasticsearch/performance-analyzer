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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.CustomMetricsLocationTestBase;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.ESResources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PerformanceAnalyzerController;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides.ConfigOverridesWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.TestUtil;
import org.elasticsearch.cluster.service.ClusterService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.MockitoAnnotations.initMocks;

@PrepareForTest(Class.class)
public class ShardIndexingPressureMetricsCollectorTests extends CustomMetricsLocationTestBase {

    private ShardIndexingPressureMetricsCollector shardIndexingPressureMetricsCollector;

    @Mock
    private ClusterService mockClusterService;

    @Mock
    PerformanceAnalyzerController mockController;

    @Mock
    ConfigOverridesWrapper mockConfigOverrides;

    @Before
    public void init() {
        initMocks(this);
        ESResources.INSTANCE.setClusterService(mockClusterService);
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");
        MetricsConfiguration.CONFIG_MAP.put(ShardIndexingPressureMetricsCollector.class, MetricsConfiguration.cdefault);
        shardIndexingPressureMetricsCollector = new ShardIndexingPressureMetricsCollector(mockController, mockConfigOverrides);

        //clean metricQueue before running every test
        TestUtil.readEvents();
    }

    @Test
    public void testShardIndexingPressureMetrics() {
        long startTimeInMills = 1153721339;
        Mockito.when(mockController.isCollectorEnabled(mockConfigOverrides, "ShardIndexingPressureMetricsCollector"))
            .thenReturn(true);
        shardIndexingPressureMetricsCollector.saveMetricValues("shard_indexing_pressure_metrics", startTimeInMills);

        List<Event> metrics = TestUtil.readEvents();
        assertEquals(1, metrics.size());
        assertEquals("shard_indexing_pressure_metrics", metrics.get(0).value);

        try {
            shardIndexingPressureMetricsCollector.saveMetricValues("shard_indexing_pressure_metrics", startTimeInMills, "123");
            assertTrue("Negative scenario test: Should have been a RuntimeException", true);
        } catch (RuntimeException ex) {
            //- expecting exception...1 values passed; 0 expected
        }
    }
}
