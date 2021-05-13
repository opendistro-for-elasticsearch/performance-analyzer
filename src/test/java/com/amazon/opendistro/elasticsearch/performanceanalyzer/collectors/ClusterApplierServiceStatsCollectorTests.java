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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.CustomMetricsLocationTestBase;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.ESResources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PerformanceAnalyzerController;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides.ConfigOverridesWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.TestUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ESResources.class})
public class ClusterApplierServiceStatsCollectorTests extends CustomMetricsLocationTestBase {
    ObjectMapper mapper = new ObjectMapper();

    public void cleanUp() throws Exception {
        super.setUp();
        // clean metricQueue before running every test
        TestUtil.readEvents();
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");
    }

    @Test
    public void testClusterApplierServiceStats_saveMetricValues() throws Exception {
        cleanUp();
        MetricsConfiguration.CONFIG_MAP.put(ClusterApplierServiceStatsCollector.class, MetricsConfiguration.cdefault);
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");
        long startTimeInMills = 1153721339;
        PerformanceAnalyzerController controller =
                Mockito.mock(PerformanceAnalyzerController.class);
        ConfigOverridesWrapper configOverrides = Mockito.mock(ConfigOverridesWrapper.class);
        ClusterApplierServiceStatsCollector clusterApplierServiceStatsCollector =
                new ClusterApplierServiceStatsCollector(controller, configOverrides);
        Mockito.when(
                        controller.isCollectorEnabled(
                                configOverrides, "ClusterApplierServiceStatsCollector"))
                .thenReturn(true);
        clusterApplierServiceStatsCollector.saveMetricValues(
                "cluster_applier_service", startTimeInMills);
        List<Event> metrics = new ArrayList<>();
        PerformanceAnalyzerMetrics.metricQueue.drainTo(metrics);

        assertEquals(1, metrics.size());
        assertEquals("cluster_applier_service", metrics.get(0).value);

        try {
            clusterApplierServiceStatsCollector.saveMetricValues(
                    "cluster_applier_service", startTimeInMills, "dummy");
            assertTrue("Negative scenario test: Should have been a RuntimeException", true);
        } catch (RuntimeException ex) {
            // - expecting exception...1 values passed; 0 expected
        }
    }

    @SuppressWarnings("unchecked")
    @Ignore
    @Test
    public void testClusterApplierServiceStats_collectMetrics() throws Exception {
        cleanUp();
        MetricsConfiguration.CONFIG_MAP.put(ClusterApplierServiceStatsCollector.class, MetricsConfiguration.cdefault);
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");
        long startTimeInMills = 1153721339;
        PerformanceAnalyzerController controller =
                Mockito.mock(PerformanceAnalyzerController.class);
        ConfigOverridesWrapper configOverrides = Mockito.mock(ConfigOverridesWrapper.class);
        ClusterApplierServiceStatsCollector clusterApplierServiceStatsCollector =
                new ClusterApplierServiceStatsCollector(controller, configOverrides);
        ClusterApplierServiceStatsCollector spyCollector =
                Mockito.spy(clusterApplierServiceStatsCollector);
        Mockito.doReturn(
                        new ClusterApplierServiceStatsCollector.ClusterApplierServiceStats(
                                23L, 15L, 2L, -1L))
                .when(spyCollector)
                .getClusterApplierServiceStats();
        Mockito.when(
                        controller.isCollectorEnabled(
                                configOverrides,
                                ClusterApplierServiceStatsCollector.class.getSimpleName()))
                .thenReturn(true);

        ESResources esResources = Mockito.mock(ESResources.class);
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        ClusterApplierService clusterApplierService = Mockito.mock(ClusterApplierService.class);
        Whitebox.setInternalState(ESResources.class, "INSTANCE", esResources);
        Mockito.when(esResources.getClusterService()).thenReturn(clusterService);
        Mockito.when(clusterService.getClusterApplierService()).thenReturn(clusterApplierService);

        spyCollector.collectMetrics(startTimeInMills);

        List<Event> metrics = new ArrayList<>();
        PerformanceAnalyzerMetrics.metricQueue.drainTo(metrics);

        assertEquals(1, metrics.size());
        String[] lines = metrics.get(0).value.split(System.lineSeparator());
        Map<String, String> map = mapper.readValue(lines[1], Map.class);
        assertEquals(
                0.6521739130434783,
                map.get(
                        AllMetrics.ClusterApplierServiceStatsValue.CLUSTER_APPLIER_SERVICE_LATENCY
                                .toString()));
        assertEquals(
                2.0,
                map.get(
                        AllMetrics.ClusterApplierServiceStatsValue.CLUSTER_APPLIER_SERVICE_FAILURE
                                .toString()));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testClusterApplierServiceStats_collectMetricsWithPreviousClusterApplierMetrics() throws Exception {
        cleanUp();
        MetricsConfiguration.CONFIG_MAP.put(ClusterApplierServiceStatsCollector.class, MetricsConfiguration.cdefault);
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");
        long startTimeInMills = 1153721339;
        PerformanceAnalyzerController controller =
                Mockito.mock(PerformanceAnalyzerController.class);
        ConfigOverridesWrapper configOverrides = Mockito.mock(ConfigOverridesWrapper.class);
        ClusterApplierServiceStatsCollector clusterApplierServiceStatsCollector =
                new ClusterApplierServiceStatsCollector(controller, configOverrides);
        ClusterApplierServiceStatsCollector spyCollector =
                Mockito.spy(clusterApplierServiceStatsCollector);
        Mockito.doReturn(
                        new ClusterApplierServiceStatsCollector.ClusterApplierServiceStats(
                                23L, 46L, 2L, -1L))
                .when(spyCollector)
                .getClusterApplierServiceStats();
        Mockito.when(
                        controller.isCollectorEnabled(
                                configOverrides,
                                ClusterApplierServiceStatsCollector.class.getSimpleName()))
                .thenReturn(true);

        ESResources esResources = Mockito.mock(ESResources.class);
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        ClusterApplierService clusterApplierService = Mockito.mock(ClusterApplierService.class);
        Whitebox.setInternalState(ESResources.class, "INSTANCE", esResources);
        Mockito.when(esResources.getClusterService()).thenReturn(clusterService);
        Mockito.when(clusterService.getClusterApplierService()).thenReturn(clusterApplierService);

        spyCollector.collectMetrics(startTimeInMills);

        List<Event> metrics = new ArrayList<>();
        PerformanceAnalyzerMetrics.metricQueue.drainTo(metrics);

        assertEquals(1, metrics.size());
        String[] lines = metrics.get(0).value.split(System.lineSeparator());
        Map<String, String> map = mapper.readValue(lines[1], Map.class);
        assertEquals(
                2.0,
                map.get(
                        AllMetrics.ClusterApplierServiceStatsValue.CLUSTER_APPLIER_SERVICE_LATENCY
                                .toString()));
        assertEquals(
                2.0,
                map.get(
                        AllMetrics.ClusterApplierServiceStatsValue.CLUSTER_APPLIER_SERVICE_FAILURE
                                .toString()));

        Mockito.doReturn(
                        new ClusterApplierServiceStatsCollector.ClusterApplierServiceStats(
                                25L, 54L, 2L, -1L))
                .when(spyCollector)
                .getClusterApplierServiceStats();

        spyCollector.collectMetrics(startTimeInMills);

        metrics.clear();
        PerformanceAnalyzerMetrics.metricQueue.drainTo(metrics);

        assertEquals(1, metrics.size());
        String[] lines2 = metrics.get(0).value.split(System.lineSeparator());
        map = mapper.readValue(lines2[1], Map.class);
        assertEquals(
                4.0,
                map.get(
                        AllMetrics.ClusterApplierServiceStatsValue.CLUSTER_APPLIER_SERVICE_LATENCY
                                .toString()));
        assertEquals(
                0.0,
                map.get(
                        AllMetrics.ClusterApplierServiceStatsValue.CLUSTER_APPLIER_SERVICE_FAILURE
                                .toString()));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testClusterApplierServiceStats_collectMetrics_ClassNotFoundException() throws Exception {
        cleanUp();
        MetricsConfiguration.CONFIG_MAP.put(ClusterApplierServiceStatsCollector.class, MetricsConfiguration.cdefault);
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");
        long startTimeInMills = 1153721339;
        PerformanceAnalyzerController controller =
                Mockito.mock(PerformanceAnalyzerController.class);
        ConfigOverridesWrapper configOverrides = Mockito.mock(ConfigOverridesWrapper.class);
        ClusterApplierServiceStatsCollector clusterApplierServiceStatsCollector =
                new ClusterApplierServiceStatsCollector(controller, configOverrides);
        ClusterApplierServiceStatsCollector spyCollector =
                Mockito.spy(clusterApplierServiceStatsCollector);
        Mockito.when(
                        controller.isCollectorEnabled(
                                configOverrides,
                                ClusterApplierServiceStatsCollector.class.getSimpleName()))
                .thenReturn(true);

        ESResources esResources = Mockito.mock(ESResources.class);
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        ClusterApplierService clusterApplierService = Mockito.mock(ClusterApplierService.class);
        Whitebox.setInternalState(ESResources.class, "INSTANCE", esResources);
        Mockito.when(esResources.getClusterService()).thenReturn(clusterService);
        Mockito.when(clusterService.getClusterApplierService()).thenReturn(clusterApplierService);

        spyCollector.collectMetrics(startTimeInMills);

        List<Event> metrics = new ArrayList<>();
        PerformanceAnalyzerMetrics.metricQueue.drainTo(metrics);
        // No method found to get cluster state applier thread stats. Skipping
        // ClusterApplierServiceStatsCollector.
        assertEquals(0, metrics.size());
    }
}
