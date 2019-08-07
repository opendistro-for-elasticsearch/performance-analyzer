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


package com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import static org.junit.Assert.assertEquals;

@PowerMockIgnore({"org.apache.logging.log4j.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({PerformanceAnalyzerMetrics.class, PluginSettings.class})
@SuppressStaticInitializationFor({"PluginSettings"})
public class PerformanceAnalyzerMetricsTests {

    @Before
    public void setUp() throws Exception {
        PluginSettings config = Mockito.mock(PluginSettings.class);
        Mockito.when(config.getMetricsLocation()).thenReturn("/dev/shm/performanceanalyzer");
        PowerMockito.mockStatic(PluginSettings.class);
        PowerMockito.when(PluginSettings.instance()).thenReturn(config);
    }

    @Test
    public void testBasicMetric() {
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");
        PerformanceAnalyzerMetrics.emitMetric(PerformanceAnalyzerMetrics.sDevShmLocation + "/dir1/test1", "value1");
        assertEquals("value1", PerformanceAnalyzerMetrics.getMetric(PerformanceAnalyzerMetrics.sDevShmLocation + "/dir1/test1"));

        assertEquals("", PerformanceAnalyzerMetrics.getMetric(PerformanceAnalyzerMetrics.sDevShmLocation + "/dir1/test2"));

        PerformanceAnalyzerMetrics.removeMetrics(PerformanceAnalyzerMetrics.sDevShmLocation + "/dir1");
    }

    @Test
    public void testGeneratePath() {
        long startTimeInMillis = 1553725339;
        String generatedPath = PerformanceAnalyzerMetrics.generatePath(startTimeInMillis, "dir1", "id", "dir2");
        String expectedPath = PerformanceAnalyzerMetrics.sDevShmLocation +
                "/" + PerformanceAnalyzerMetrics.getTimeInterval(startTimeInMillis) + "/dir1/id/dir2";
        assertEquals(expectedPath, generatedPath);
    }
}
