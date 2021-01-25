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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ESResources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.MasterPendingValue;
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

public class MasterServiceMetricsTests {
    private MasterServiceMetrics masterServiceMetrics;
    private long startTimeInMills = 1153721339;
    private ThreadPool threadPool;

    @Mock
    private ClusterService mockedClusterService;

    @Before
    public void init() {
        initMocks(this);
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");
        threadPool = new TestThreadPool("test");
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        ESResources.INSTANCE.setClusterService(clusterService);

        MetricsConfiguration.CONFIG_MAP.put(MasterServiceMetrics.class, MetricsConfiguration.cdefault);
        masterServiceMetrics = new MasterServiceMetrics();

        //clean metricQueue before running every test
        TestUtil.readEvents();
    }

    @After
    public void tearDown() {
        threadPool.shutdownNow();
    }

    @Test
    public void testGetMetricsPath() {
        String expectedPath = PluginSettings.instance().getMetricsLocation()
            + PerformanceAnalyzerMetrics.getTimeInterval(startTimeInMills) + "/"
            + PerformanceAnalyzerMetrics.sPendingTasksPath + "/"
            + "current" + "/"
            + PerformanceAnalyzerMetrics.FINISH_FILE_NAME;
        String actualPath = masterServiceMetrics.getMetricsPath(startTimeInMills, "current", PerformanceAnalyzerMetrics.FINISH_FILE_NAME);
        assertEquals(expectedPath, actualPath);

        try {
            masterServiceMetrics.getMetricsPath(startTimeInMills, "current");
            fail("Negative scenario test: Should have been a RuntimeException");
        } catch (RuntimeException ex) {
            //- expecting exception...1 values passed; 2 expected
        }
    }

    @Test
    public void testCollectMetrics() {
        masterServiceMetrics.collectMetrics(startTimeInMills);
        String jsonStr = readMetricsInJsonString(1);
        assertFalse(jsonStr.contains(MasterPendingValue.Constants.PENDING_TASKS_COUNT_VALUE));
    }

    @Test
    public void testWithMockClusterService() {
        ESResources.INSTANCE.setClusterService(mockedClusterService);
        masterServiceMetrics.collectMetrics(startTimeInMills);
        String jsonStr = readMetricsInJsonString(0);
        assertNull(jsonStr);

        ESResources.INSTANCE.setClusterService(mockedClusterService);
        when(mockedClusterService.getMasterService()).thenThrow(new RuntimeException());
        masterServiceMetrics.collectMetrics(startTimeInMills);
        jsonStr = readMetricsInJsonString(0);
        assertNull(jsonStr);

        ESResources.INSTANCE.setClusterService(null);
        masterServiceMetrics.collectMetrics(startTimeInMills);
        jsonStr = readMetricsInJsonString(0);
        assertNull(jsonStr);
    }

    private String readMetricsInJsonString(int size) {
        List<Event> metrics = TestUtil.readEvents();
        assert metrics.size() == size;
        if (size != 0) {
            String[] jsonStrs = metrics.get(0).value.split("\n");
            assert jsonStrs.length == 1;
            return jsonStrs[0];
        } else {
            return null;
        }
    }
}
