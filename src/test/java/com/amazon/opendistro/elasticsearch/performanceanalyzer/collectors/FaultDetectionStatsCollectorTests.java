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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PerformanceAnalyzerController;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides.ConfigOverridesWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FaultDetectionStatsCollectorTests extends CustomMetricsLocationTestBase {
    private static final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testFaultDetectionStats_saveMetricValues() {
        MetricsConfiguration.CONFIG_MAP.put(FaultDetectionStatsCollector.class, MetricsConfiguration.cdefault);
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");
        long startTimeInMills = 1153721339;
        PerformanceAnalyzerController controller = Mockito.mock(
                PerformanceAnalyzerController.class);
        ConfigOverridesWrapper configOverrides = Mockito.mock(ConfigOverridesWrapper.class);
        FaultDetectionStatsCollector faultDetectionStatsCollector = new FaultDetectionStatsCollector(
                controller, configOverrides);
        Mockito.when(controller.isCollectorEnabled(configOverrides, "FaultDetectionStatsCollector"))
                .thenReturn(true);
        faultDetectionStatsCollector.saveMetricValues("fault_detection", startTimeInMills);

        List<Event> metrics =  new ArrayList<>();
        PerformanceAnalyzerMetrics.metricQueue.drainTo(metrics);

        assertEquals(1, metrics.size());
        assertEquals("fault_detection", metrics.get(0).value);

        try {
            faultDetectionStatsCollector.saveMetricValues("fault_detection", startTimeInMills, "dummy");
            assertTrue("Negative scenario test: Should have been a RuntimeException", true);
        } catch (RuntimeException ex) {
            //- expecting exception...1 values passed; 0 expected
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testFaultDetectionStats_collectMetrics() throws NoSuchMethodException, IllegalAccessException,
            InvocationTargetException, JsonProcessingException, NoSuchFieldException {
        System.out.println("test 1");
        MetricsConfiguration.CONFIG_MAP.put(FaultDetectionStatsCollector.class, MetricsConfiguration.cdefault);
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");
        long startTimeInMills = 1153721339;
        PerformanceAnalyzerController controller = Mockito.mock(PerformanceAnalyzerController.class);
        ConfigOverridesWrapper configOverrides = Mockito.mock(ConfigOverridesWrapper.class);
        FaultDetectionStatsCollector faultDetectionStatsCollector = new
                FaultDetectionStatsCollector(controller, configOverrides);
        FaultDetectionStatsCollector spyCollector = Mockito.spy(faultDetectionStatsCollector);

        Mockito.doReturn(new FaultDetectionStatsCollector.FaultDetectionStats(23L, 15L, 2L))
                .when(spyCollector).getFollowerCheckStats();
        Mockito.doReturn(new FaultDetectionStatsCollector.FaultDetectionStats(27L, 19L, 5L))
                .when(spyCollector).getLeaderCheckStats();

        Mockito.when(controller.isCollectorEnabled(configOverrides,
                FaultDetectionStatsCollector.class.getSimpleName())).thenReturn(true);

        spyCollector.collectMetrics(startTimeInMills);

        List<Event> metrics =  new ArrayList<>();
        PerformanceAnalyzerMetrics.metricQueue.drainTo(metrics);

        assertEquals(1, metrics.size());
        String[] lines  = metrics.get(0).value.split(System.lineSeparator());
        Map<String, String> map = mapper.readValue(lines[1], Map.class);
        assertEquals(0.6521739130434783, map.get(AllMetrics.FaultDetectionMetric
                .FOLLOWER_CHECK_LATENCY.toString()));
        assertEquals(2.0, map.get(AllMetrics.FaultDetectionMetric
                .FOLLOWER_CHECK_FAILURE.toString()));
        assertEquals(0.7037037037037037, map.get(AllMetrics.FaultDetectionMetric
                .LEADER_CHECK_LATENCY.toString()));
        assertEquals(5.0, map.get(AllMetrics.FaultDetectionMetric
                .LEADER_CHECK_FAILURE.toString()));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testFaultDetectionStats_collectMetricsWithPreviousClusterApplierMetrics() throws NoSuchMethodException, IllegalAccessException,
            InvocationTargetException, JsonProcessingException, NoSuchFieldException {
        System.out.println("test 2");
        MetricsConfiguration.CONFIG_MAP.put(FaultDetectionStatsCollector.class, MetricsConfiguration.cdefault);
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");
        long startTimeInMills = 1153721339;
        PerformanceAnalyzerController controller = Mockito.mock(PerformanceAnalyzerController.class);
        ConfigOverridesWrapper configOverrides = Mockito.mock(ConfigOverridesWrapper.class);
        FaultDetectionStatsCollector faultDetectionStatsCollector = new
                FaultDetectionStatsCollector(controller, configOverrides);
        FaultDetectionStatsCollector spyCollector = Mockito.spy(faultDetectionStatsCollector);

        Mockito.doReturn(new FaultDetectionStatsCollector.FaultDetectionStats(23L, 15L, 2L))
                .when(spyCollector).getFollowerCheckStats();
        Mockito.doReturn(new FaultDetectionStatsCollector.FaultDetectionStats(27L, 19L, 5L))
                .when(spyCollector).getLeaderCheckStats();

        Mockito.when(controller.isCollectorEnabled(configOverrides,
                FaultDetectionStatsCollector.class.getSimpleName())).thenReturn(true);

        spyCollector.resetFaultDetectionStats();
        spyCollector.collectMetrics(startTimeInMills);

        List<Event> metrics =  new ArrayList<>();
        PerformanceAnalyzerMetrics.metricQueue.drainTo(metrics);

        assertEquals(1, metrics.size());
        String[] lines  = metrics.get(0).value.split(System.lineSeparator());
        Map<String, String> map = mapper.readValue(lines[1], Map.class);
        assertEquals(0.6521739130434783, map.get(AllMetrics.FaultDetectionMetric
                .FOLLOWER_CHECK_LATENCY.toString()));
        assertEquals(2.0, map.get(AllMetrics.FaultDetectionMetric
                .FOLLOWER_CHECK_FAILURE.toString()));
        assertEquals(0.7037037037037037, map.get(AllMetrics.FaultDetectionMetric
                .LEADER_CHECK_LATENCY.toString()));
        assertEquals(5.0, map.get(AllMetrics.FaultDetectionMetric
                .LEADER_CHECK_FAILURE.toString()));

        Mockito.doReturn(new FaultDetectionStatsCollector.FaultDetectionStats(24L, 17L, 2L))
                .when(spyCollector).getFollowerCheckStats();
        Mockito.doReturn(new FaultDetectionStatsCollector.FaultDetectionStats(30L, 22L, 6L))
                .when(spyCollector).getLeaderCheckStats();

        spyCollector.collectMetrics(startTimeInMills);

        metrics.clear();
        PerformanceAnalyzerMetrics.metricQueue.drainTo(metrics);

        assertEquals(1, metrics.size());
        lines  = metrics.get(0).value.split(System.lineSeparator());
        map = mapper.readValue(lines[1], Map.class);
        assertEquals(2.0, map.get(AllMetrics.FaultDetectionMetric.FOLLOWER_CHECK_LATENCY.toString()));
        assertEquals(0.0, map.get(AllMetrics.FaultDetectionMetric.FOLLOWER_CHECK_FAILURE.toString()));
        assertEquals(1.0, map.get(AllMetrics.FaultDetectionMetric.LEADER_CHECK_LATENCY.toString()));
        assertEquals(1.0, map.get(AllMetrics.FaultDetectionMetric.LEADER_CHECK_FAILURE.toString()));


    }

    @SuppressWarnings("unchecked")
    @Test
    public void testFaultDetectionStats_collectMetrics_ClassNotFoundException() {
        MetricsConfiguration.CONFIG_MAP.put(FaultDetectionStatsCollector.class, MetricsConfiguration.cdefault);
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");
        long startTimeInMills = 1153721339;
        PerformanceAnalyzerController controller = Mockito.mock(PerformanceAnalyzerController.class);
        ConfigOverridesWrapper configOverrides = Mockito.mock(ConfigOverridesWrapper.class);
        FaultDetectionStatsCollector faultDetectionStatsCollector = new
                FaultDetectionStatsCollector(controller, configOverrides);
        FaultDetectionStatsCollector spyCollector = Mockito.spy(faultDetectionStatsCollector);
        Mockito.when(controller.isCollectorEnabled(configOverrides,
                FaultDetectionStatsCollector.class.getSimpleName())).thenReturn(true);

        spyCollector.collectMetrics(startTimeInMills);

        List<Event> metrics =  new ArrayList<>();
        PerformanceAnalyzerMetrics.metricQueue.drainTo(metrics);
        // No method found to get cluster state applier thread stats. Skipping ClusterApplierServiceStatsCollector.
        assertEquals(0, metrics.size());
    }
}
