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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ESResources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PerformanceAnalyzerController;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides.ConfigOverridesWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import junit.framework.TestCase;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ESResources.class})
public class MasterClusterStateUpdateStatsCollectorTests extends TestCase {
    ObjectMapper mapper = new ObjectMapper();
    @Test
    public void testMasterClusterStateUpdateStats_saveMetricValues() {
        MetricsConfiguration.CONFIG_MAP.put(MasterClusterStateUpdateStatsCollector.class, MetricsConfiguration.cdefault);
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");
        long startTimeInMills = 1153721339;
        PerformanceAnalyzerController controller = Mockito.mock(
                PerformanceAnalyzerController.class);
        ConfigOverridesWrapper configOverrides = Mockito.mock(ConfigOverridesWrapper.class);
        MasterClusterStateUpdateStatsCollector masterClusterStateUpdateStatsCollector = new MasterClusterStateUpdateStatsCollector(
                controller, configOverrides);
        Mockito.when(controller.isCollectorEnabled(configOverrides, "MasterClusterStateUpdateStatsCollector"))
                .thenReturn(true);
        masterClusterStateUpdateStatsCollector.saveMetricValues("master_cluster_update",
                startTimeInMills);
        List<Event> metrics =  new ArrayList<>();
        PerformanceAnalyzerMetrics.metricQueue.drainTo(metrics);

        assertEquals(1, metrics.size());
        assertEquals("master_cluster_update", metrics.get(0).value);

        try {
            masterClusterStateUpdateStatsCollector.saveMetricValues("master_cluster_update", startTimeInMills,
                    "dummy");
            assertTrue("Negative scenario test: Should have been a RuntimeException", true);
        } catch (RuntimeException ex) {
            //- expecting exception...1 values passed; 0 expected
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testMasterClusterStateUpdateStats_collectMetrics() throws NoSuchMethodException, IllegalAccessException,
            InvocationTargetException, JsonProcessingException {
        System.out.println("test 1");
        MetricsConfiguration.CONFIG_MAP.put(MasterClusterStateUpdateStatsCollector.class, MetricsConfiguration.cdefault);
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");
        long startTimeInMills = 1153721339;
        PerformanceAnalyzerController controller = Mockito.mock(PerformanceAnalyzerController.class);
        ConfigOverridesWrapper configOverrides = Mockito.mock(ConfigOverridesWrapper.class);
        MasterClusterStateUpdateStatsCollector masterClusterStateUpdateStatsCollector = new
                MasterClusterStateUpdateStatsCollector(controller, configOverrides);
        MasterClusterStateUpdateStatsCollector spyCollector = Mockito.spy(masterClusterStateUpdateStatsCollector);
        Mockito.doReturn(new MasterClusterStateUpdateStatsCollector.MasterClusterStateUpdateStats(23L, 15L, 2L))
                .when(spyCollector).getMasterClusterStateUpdateStats();
        Mockito.when(controller.isCollectorEnabled(configOverrides,
                MasterClusterStateUpdateStatsCollector.class.getSimpleName())).thenReturn(true);

        ESResources esResources = Mockito.mock(ESResources.class);
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        MasterService masterService = Mockito.mock(MasterService.class);
        Whitebox.setInternalState(ESResources.class, "INSTANCE", esResources);
        Mockito.when(esResources.getClusterService()).thenReturn(clusterService);
        Mockito.when(clusterService.getMasterService()).thenReturn(masterService);

        spyCollector.collectMetrics(startTimeInMills);

        List<Event> metrics =  new ArrayList<>();
        PerformanceAnalyzerMetrics.metricQueue.drainTo(metrics);

        assertEquals(1, metrics.size());
        String[] lines = metrics.get(0).value.split(System.lineSeparator());
        Map<String, String> map = mapper.readValue(lines[1], Map.class);
        assertEquals(0.6521739130434783, map.get(AllMetrics.MasterClusterUpdateStatsValue
                .PUBLISH_CLUSTER_STATE_LATENCY.toString()));
        assertEquals(2.0, map.get(AllMetrics.MasterClusterUpdateStatsValue
                .PUBLISH_CLUSTER_STATE_FAILURE.toString()));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testMasterClusterStateUpdateStats_collectMetricsWithPreviousMasterClusterUpdateStats() throws NoSuchMethodException, IllegalAccessException,
            InvocationTargetException, JsonProcessingException {
        System.out.println("test 2");
        MetricsConfiguration.CONFIG_MAP.put(MasterClusterStateUpdateStatsCollector.class, MetricsConfiguration.cdefault);
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");
        long startTimeInMills = 1153721339;
        PerformanceAnalyzerController controller = Mockito.mock(PerformanceAnalyzerController.class);
        ConfigOverridesWrapper configOverrides = Mockito.mock(ConfigOverridesWrapper.class);
        MasterClusterStateUpdateStatsCollector masterClusterStateUpdateStatsCollector = new
                MasterClusterStateUpdateStatsCollector(controller, configOverrides);
        MasterClusterStateUpdateStatsCollector spyCollector = Mockito.spy(masterClusterStateUpdateStatsCollector);
        Mockito.doReturn(new MasterClusterStateUpdateStatsCollector.
                MasterClusterStateUpdateStats(23L, 46L, 2L))
                .when(spyCollector).getMasterClusterStateUpdateStats();
        Mockito.when(controller.isCollectorEnabled(configOverrides,
                MasterClusterStateUpdateStatsCollector.class.getSimpleName())).thenReturn(true);

        ESResources esResources = Mockito.mock(ESResources.class);
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        MasterService masterService = Mockito.mock(MasterService.class);
        Whitebox.setInternalState(ESResources.class, "INSTANCE", esResources);
        Mockito.when(esResources.getClusterService()).thenReturn(clusterService);
        Mockito.when(clusterService.getMasterService()).thenReturn(masterService);

        spyCollector.collectMetrics(startTimeInMills);

        List<Event> metrics =  new ArrayList<>();
        PerformanceAnalyzerMetrics.metricQueue.drainTo(metrics);

        assertEquals(1, metrics.size());
        String[] lines = metrics.get(0).value.split(System.lineSeparator());
        Map<String, String> map = mapper.readValue(lines[1], Map.class);
        assertEquals(2.0, map.get(AllMetrics.MasterClusterUpdateStatsValue
                .PUBLISH_CLUSTER_STATE_LATENCY.toString()));
        assertEquals(2.0, map.get(AllMetrics.MasterClusterUpdateStatsValue
                .PUBLISH_CLUSTER_STATE_FAILURE.toString()));

        Mockito.doReturn(new MasterClusterStateUpdateStatsCollector.
                MasterClusterStateUpdateStats(25L, 54L, 2L))
                .when(spyCollector).getMasterClusterStateUpdateStats();

        spyCollector.collectMetrics(startTimeInMills);

        metrics.clear();
        PerformanceAnalyzerMetrics.metricQueue.drainTo(metrics);

        assertEquals(1, metrics.size());
        String[] lines2 = metrics.get(0).value.split(System.lineSeparator());
        map = mapper.readValue(lines2[1], Map.class);
        assertEquals(4.0, map.get(AllMetrics.MasterClusterUpdateStatsValue
                .PUBLISH_CLUSTER_STATE_LATENCY.toString()));
        assertEquals(0.0, map.get(AllMetrics.MasterClusterUpdateStatsValue
                .PUBLISH_CLUSTER_STATE_FAILURE.toString()));


    }

    @SuppressWarnings("unchecked")
    @Test
    public void testMasterClusterStateUpdateStats_collectMetrics_ClassNotFoundException() {
        MetricsConfiguration.CONFIG_MAP.put(MasterClusterStateUpdateStatsCollector.class, MetricsConfiguration.cdefault);
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");
        long startTimeInMills = 1153721339;
        PerformanceAnalyzerController controller = Mockito.mock(PerformanceAnalyzerController.class);
        ConfigOverridesWrapper configOverrides = Mockito.mock(ConfigOverridesWrapper.class);
        MasterClusterStateUpdateStatsCollector masterClusterStateUpdateStatsCollector = new
                MasterClusterStateUpdateStatsCollector(controller, configOverrides);
        MasterClusterStateUpdateStatsCollector spyCollector = Mockito.spy(masterClusterStateUpdateStatsCollector);
        Mockito.when(controller.isCollectorEnabled(configOverrides,
                MasterClusterStateUpdateStatsCollector.class.getSimpleName())).thenReturn(true);

        ESResources esResources = Mockito.mock(ESResources.class);
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        MasterService masterService = Mockito.mock(MasterService.class);
        Whitebox.setInternalState(ESResources.class, "INSTANCE", esResources);
        Mockito.when(esResources.getClusterService()).thenReturn(clusterService);
        Mockito.when(clusterService.getMasterService()).thenReturn(masterService);

        spyCollector.collectMetrics(startTimeInMills);

        List<Event> metrics =  new ArrayList<>();
        PerformanceAnalyzerMetrics.metricQueue.drainTo(metrics);
        // No method found to get master cluster state update stats. Skipping MasterClusterStateUpdateStatsCollector.
        assertEquals(0, metrics.size());
    }
}
