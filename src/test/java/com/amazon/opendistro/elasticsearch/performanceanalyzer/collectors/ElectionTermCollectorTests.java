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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.MockitoAnnotations.initMocks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ESResources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PerformanceAnalyzerController;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides.ConfigOverridesWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ElectionTermValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.TestUtil;
import java.util.List;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

public class ElectionTermCollectorTests {
    private ElectionTermCollector electionTermCollector;
    private long startTimeInMills = 1153721339;
    private ThreadPool threadPool;
    private PerformanceAnalyzerController controller;
    private ConfigOverridesWrapper configOverrides;

    @Mock
    private ClusterService mockedClusterService;

    @Before
    public void init() {
        initMocks(this);
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");
        threadPool = new TestThreadPool("test");
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        ESResources.INSTANCE.setClusterService(clusterService);
        controller = Mockito.mock(PerformanceAnalyzerController.class);
        configOverrides = Mockito.mock(ConfigOverridesWrapper.class);

        MetricsConfiguration.CONFIG_MAP.put(ElectionTermCollector.class, MetricsConfiguration.cdefault);
        electionTermCollector = new ElectionTermCollector(controller,configOverrides);

        //clean metricQueue before running every test
        TestUtil.readEvents();
    }

    @After
    public void tearDown() {
        threadPool.shutdownNow();
    }

    @Test
    public void testGetMetricPath() {
        String expectedPath = PluginSettings.instance().getMetricsLocation()
                + PerformanceAnalyzerMetrics.getTimeInterval(startTimeInMills) + "/"
                + PerformanceAnalyzerMetrics.sElectionTermPath;
        String actualPath = electionTermCollector.getMetricsPath(startTimeInMills);
        assertEquals(expectedPath,actualPath);

        try {
            electionTermCollector.getMetricsPath(startTimeInMills,"current");
            fail("Negative scenario test: Should have been a RuntimeException");
        } catch (RuntimeException ex){
            //- expecting exception...1 value passed; 0 expected
        }
    }

    @Test
    public void testCollectMetrics() {
        electionTermCollector.collectMetrics(startTimeInMills);
        String jsonStr = readMetricsInJsonString(1);
        String[] jsonStrArray = jsonStr.split(":",2);
        assertTrue(jsonStrArray[0].contains(ElectionTermValue.Constants.ELECTION_TERM_VALUE));
        assertTrue(jsonStrArray[1].contains("0"));
    }

    @Test
    public void testWithMockClusterService() {
        ESResources.INSTANCE.setClusterService(mockedClusterService);
        electionTermCollector.collectMetrics(startTimeInMills);
        String jsonStr = readMetricsInJsonString(0);
        assertNull(jsonStr);

        ESResources.INSTANCE.setClusterService(null);
        electionTermCollector.collectMetrics(startTimeInMills);
        jsonStr = readMetricsInJsonString(0);
        assertNull(jsonStr);
    }

    private String readMetricsInJsonString(int size) {
        List<Event> metrics = TestUtil.readEvents();
        assert metrics.size() == size;
        if (size != 0) {
            String[] jsonStrs = metrics.get(0).value.split("\n");
            assert jsonStrs.length == 2;
            return jsonStrs[1];
        } else {
            return null;
        }
    }
}

