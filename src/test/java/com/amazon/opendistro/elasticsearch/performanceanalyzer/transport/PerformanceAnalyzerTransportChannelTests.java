/*
 * Copyright <2020> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.transport;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

import java.io.IOException;
import org.apache.commons.lang3.SystemUtils;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class PerformanceAnalyzerTransportChannelTests {
    private PerformanceAnalyzerTransportChannel channel;

    @Mock private TransportChannel originalChannel;
    @Mock private TransportResponse response;

    @Before
    public void init() {
        // this test only runs in Linux system
        // as some of the static members of the ThreadList class are specific to Linux
        org.junit.Assume.assumeTrue(SystemUtils.IS_OS_LINUX);

        initMocks(this);
        channel = new PerformanceAnalyzerTransportChannel();
        channel.set(originalChannel, 0, "testIndex", 1, 0, false);
        assertEquals("PerformanceAnalyzerTransportChannelProfile", channel.getProfileName());
        assertEquals("PerformanceAnalyzerTransportChannelType", channel.getChannelType());
        assertEquals(originalChannel, channel.getInnerChannel());
    }

//    @Test
//    public void testShardBulkMetrics() {
//        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");
//        long startTimeInMills = 1593719339;
//        PerformanceAnalyzerTransportChannel performanceanalyzerTransportChannel = new PerformanceAnalyzerTransportChannel();
//        performanceanalyzerTransportChannel.saveMetricValues("ABCDEF", startTimeInMills, "BulkThread", "ShardBulkId", "start");
//        String fetchedValue = PerformanceAnalyzerMetrics.getMetric(
//                PluginSettings.instance().getMetricsLocation() +
//                        PerformanceAnalyzerMetrics.getTimeInterval(startTimeInMills)+"/threads/BulkThread/shardbulk/ShardBulkId/start");
//        PerformanceAnalyzerMetrics.removeMetrics(PluginSettings.instance().getMetricsLocation()
//                 + PerformanceAnalyzerMetrics.getTimeInterval(startTimeInMills));
//        assertEquals("ABCDEF", fetchedValue);
//    }

    @Test
    public void testResponse() throws IOException {
        channel.sendResponse(response);
        verify(originalChannel).sendResponse(response);

        Exception exception = new Exception("dummy exception");
        channel.sendResponse(exception);
        verify(originalChannel).sendResponse(exception);
    }

}
