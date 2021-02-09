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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ESResources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.CircuitBreakerCollector.CircuitBreakerStatus;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.TestUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.paranamer.ParanamerModule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CircuitBreakerCollectorTests extends ESSingleNodeTestCase {
    private static final String TEST_INDEX = "test";
    private CircuitBreakerCollector collector;
    private long startTimeInMills = 1153721339;

    @Before
    public void init() {
        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        ESResources.INSTANCE.setIndicesService(indicesService);
        ESResources.INSTANCE.setCircuitBreakerService(indicesService.getCircuitBreakerService());

        MetricsConfiguration.CONFIG_MAP.put(CircuitBreakerCollector.class, MetricsConfiguration.cdefault);
        collector = new CircuitBreakerCollector();

        //clean metricQueue before running every test
        TestUtil.readEvents();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void testGetMetricsPath() {
        String expectedPath = PluginSettings.instance().getMetricsLocation()
            + PerformanceAnalyzerMetrics.getTimeInterval(startTimeInMills)+ "/" + PerformanceAnalyzerMetrics.sCircuitBreakerPath;
        String actualPath = collector.getMetricsPath(startTimeInMills);
        assertEquals(expectedPath, actualPath);

        try {
            collector.getMetricsPath(startTimeInMills, "circuitBreakerPath");
            fail("Negative scenario test: Should have been a RuntimeException");
        } catch (RuntimeException ex) {
            //- expecting exception...1 values passed; 0 expected
        }
    }

    @Test
    public void testCollectMetrics() throws IOException {
        createIndex(TEST_INDEX);
        collector.collectMetrics(startTimeInMills);
        List<CircuitBreakerStatus> metrics = readMetrics();
        assertEquals(5, metrics.size());
        assertEquals(CircuitBreaker.REQUEST, metrics.get(0).getType());
        assertEquals(CircuitBreaker.FIELDDATA, metrics.get(1).getType());
        assertEquals(CircuitBreaker.IN_FLIGHT_REQUESTS, metrics.get(2).getType());
        assertEquals(CircuitBreaker.ACCOUNTING, metrics.get(3).getType());
        assertEquals(CircuitBreaker.PARENT, metrics.get(4).getType());
    }

    private List<CircuitBreakerStatus> readMetrics() throws IOException {
        List<Event> metrics = TestUtil.readEvents();
        assert metrics.size() == 1;
        ObjectMapper objectMapper = new ObjectMapper().registerModule(new ParanamerModule());
        String[] jsonStrs = metrics.get(0).value.split("\n");
        assert jsonStrs.length == 6;
        List<CircuitBreakerStatus> list = new ArrayList<>();
        for (int i = 1; i < 6; i++) {
            list.add(objectMapper.readValue(jsonStrs[i], CircuitBreakerStatus.class));
        }
        return list;
    }
}
