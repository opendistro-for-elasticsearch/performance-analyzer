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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.CustomMetricsLocationTestBase;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.TestUtil;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class AdmissionControlMetricsCollectorTests extends CustomMetricsLocationTestBase {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        // clean metricQueue before running every test
        TestUtil.readEvents();
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");
    }

    @Test
    public void admissionControlMetricsCollector() {
        MetricsConfiguration.CONFIG_MAP.put(
                AdmissionControlMetricsCollector.class, MetricsConfiguration.cdefault);
        AdmissionControlMetricsCollector admissionControlMetricsCollector =
                new AdmissionControlMetricsCollector();

        long startTimeInMills = System.currentTimeMillis();
        admissionControlMetricsCollector.saveMetricValues("testMetric", startTimeInMills);

        List<Event> metrics = TestUtil.readEvents();
        assertEquals(1, metrics.size());
        assertEquals("testMetric", metrics.get(0).value);
    }
}
