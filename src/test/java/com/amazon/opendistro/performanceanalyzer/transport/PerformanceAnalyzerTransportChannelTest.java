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

package com.amazon.opendistro.performanceanalyzer.transport;

import com.amazon.opendistro.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class PerformanceAnalyzerTransportChannelTest {
    @Test
    public void testShardBulkMetrics() {
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");
        long startTimeInMills = System.currentTimeMillis() + 6000000;
        PerformanceAnalyzerTransportChannel performanceanalyzerTransportChannel = new PerformanceAnalyzerTransportChannel();
        performanceanalyzerTransportChannel.saveMetricValues("ABCDEF", startTimeInMills, "BulkThread", "ShardBulkId", "start");
        String fetchedValue = PerformanceAnalyzerMetrics.getMetric(
                PerformanceAnalyzerMetrics.sDevShmLocation +
                        PerformanceAnalyzerMetrics.getTimeInterval(startTimeInMills)+"/threads/BulkThread/shardbulk/ShardBulkId/start");
        PerformanceAnalyzerMetrics.removeMetrics(PerformanceAnalyzerMetrics.sDevShmLocation);
        assertEquals("ABCDEF", fetchedValue);
    }
}
