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
import static org.mockito.MockitoAnnotations.initMocks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.CustomMetricsLocationTestBase;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.ESResources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.ThreadPoolMetricsCollector.ThreadPoolStatus;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.TestUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.paranamer.ParanamerModule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

public class ThreadPoolMetricsCollectorTests extends CustomMetricsLocationTestBase {

    private ThreadPoolMetricsCollector threadPoolMetricsCollector;

    @Mock
    private ThreadPool mockThreadPool;

    @Before
    public void init() {
        initMocks(this);

        ESResources.INSTANCE.setThreadPool(mockThreadPool);
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");
        MetricsConfiguration.CONFIG_MAP.put(ThreadPoolMetricsCollector.class, MetricsConfiguration.cdefault);
        threadPoolMetricsCollector = new ThreadPoolMetricsCollector();

        //clean metricQueue before running every test
        TestUtil.readEvents();
    }

    @Test
    public void testThreadPoolMetrics() {
        long startTimeInMills = 1453724339;
        threadPoolMetricsCollector.saveMetricValues("12321.5464", startTimeInMills);
        List<Event> metrics = TestUtil.readEvents();
        assertEquals(1, metrics.size());
        assertEquals("12321.5464", metrics.get(0).value);

        try {
            threadPoolMetricsCollector.saveMetricValues("12321.5464", startTimeInMills, "123");
            assertEquals(true, true);
        } catch (RuntimeException ex) {
            //- expecting exception...1 values passed; 0 expected
        }

        try {
            threadPoolMetricsCollector.getMetricsPath(startTimeInMills, "123", "x");
            assertEquals(true, true);
        } catch (RuntimeException ex) {
            //- expecting exception...2 values passed; 0 expected
        }
    }

    @Test
    public void testCollectMetrics() throws IOException {
        long startTimeInMills = 1453724339;
        Mockito.when(mockThreadPool.stats()).thenReturn(generateThreadPoolStat(2));
        threadPoolMetricsCollector.collectMetrics(startTimeInMills);
        ThreadPoolStatus threadPoolStatus = readMetrics();
        assertEquals(0, threadPoolStatus.getRejected());

        startTimeInMills += 5000;
        Mockito.when(mockThreadPool.stats()).thenReturn(generateThreadPoolStat(4));
        threadPoolMetricsCollector.collectMetrics(startTimeInMills);
        threadPoolStatus = readMetrics();
        assertEquals(2, threadPoolStatus.getRejected());

        startTimeInMills += 12000;
        Mockito.when(mockThreadPool.stats()).thenReturn(generateThreadPoolStat(9));
        threadPoolMetricsCollector.collectMetrics(startTimeInMills);
        threadPoolStatus = readMetrics();
        assertEquals(5, threadPoolStatus.getRejected());

        startTimeInMills += 16000;
        Mockito.when(mockThreadPool.stats()).thenReturn(generateThreadPoolStat(20));
        threadPoolMetricsCollector.collectMetrics(startTimeInMills);
        threadPoolStatus = readMetrics();
        assertEquals(0, threadPoolStatus.getRejected());

        startTimeInMills += 3000;
        Mockito.when(mockThreadPool.stats()).thenReturn(generateThreadPoolStat(21));
        threadPoolMetricsCollector.collectMetrics(startTimeInMills);
        threadPoolStatus = readMetrics();
        assertEquals(1, threadPoolStatus.getRejected());

        startTimeInMills += 3000;
        Mockito.when(mockThreadPool.stats()).thenReturn(generateThreadPoolStat(19));
        threadPoolMetricsCollector.collectMetrics(startTimeInMills);
        threadPoolStatus = readMetrics();
        assertEquals(0, threadPoolStatus.getRejected());
    }

    private ThreadPoolStats generateThreadPoolStat(long rejected) {
        List<ThreadPoolStats.Stats> stats = new ArrayList<>();
        stats.add(new ThreadPoolStats.Stats("write", 0, 0, 0, rejected, 0, 0));
        return new ThreadPoolStats(stats);
    }

    private ThreadPoolStatus readMetrics() throws IOException {
        List<Event> metrics = TestUtil.readEvents();
        assert metrics.size() == 1;
        ObjectMapper objectMapper = new ObjectMapper().registerModule(new ParanamerModule());
        String[] jsonStrs = metrics.get(0).value.split("\n");
        assert jsonStrs.length == 2;
        return objectMapper.readValue(jsonStrs[1], ThreadPoolStatus.class);
    }
}
