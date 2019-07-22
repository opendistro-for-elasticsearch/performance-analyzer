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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.action;

import org.junit.Test;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.CustomMetricsLocationTestBase;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import static org.junit.Assert.assertEquals;

public class PerformanceAnalyzerActionListenerTests extends CustomMetricsLocationTestBase {

    @Test
    public void testHttpMetrics() {
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");
        long startTimeInMills = 1553725339;
        PerformanceAnalyzerActionListener performanceanalyzerActionListener = new PerformanceAnalyzerActionListener();
        performanceanalyzerActionListener.saveMetricValues("XYZADFAS", startTimeInMills, "bulk", "bulkId", "start");
        String fetchedValue = PerformanceAnalyzerMetrics.getMetric(
                PluginSettings.instance().getMetricsLocation()
                        + PerformanceAnalyzerMetrics.getTimeInterval(startTimeInMills)+"/threads/http/bulk/bulkId/start");
        assertEquals("XYZADFAS", fetchedValue);

        String startMetricsValue = performanceanalyzerActionListener.generateStartMetrics(123, "val2", 0).toString();
        performanceanalyzerActionListener.saveMetricValues(startMetricsValue,  startTimeInMills, "search", "searchId1", "start");
        fetchedValue = PerformanceAnalyzerMetrics.getMetric(
                PluginSettings.instance().getMetricsLocation()
                        + PerformanceAnalyzerMetrics.getTimeInterval(startTimeInMills)+"/threads/http/search/searchId1/start");
        assertEquals(startMetricsValue, fetchedValue);

        String finishMetricsValue = performanceanalyzerActionListener.generateFinishMetrics(456, 200, "val4").toString();
        performanceanalyzerActionListener.saveMetricValues(finishMetricsValue, startTimeInMills, "search", "searchId1", "finish");
        fetchedValue = PerformanceAnalyzerMetrics.getMetric(
                PluginSettings.instance().getMetricsLocation()
                        + PerformanceAnalyzerMetrics.getTimeInterval(startTimeInMills)+"/threads/http/search/searchId1/finish");
        assertEquals(finishMetricsValue, fetchedValue);

        performanceanalyzerActionListener.saveMetricValues(finishMetricsValue, startTimeInMills, "search", "searchId2", "finish");
        fetchedValue = PerformanceAnalyzerMetrics.getMetric(
                PluginSettings.instance().getMetricsLocation()
                        + PerformanceAnalyzerMetrics.getTimeInterval(startTimeInMills)+"/threads/http/search/searchId2/finish");
        assertEquals(finishMetricsValue, fetchedValue);

        PerformanceAnalyzerMetrics.removeMetrics(PluginSettings.instance().getMetricsLocation()
                + PerformanceAnalyzerMetrics.getTimeInterval(startTimeInMills));
    }
}
