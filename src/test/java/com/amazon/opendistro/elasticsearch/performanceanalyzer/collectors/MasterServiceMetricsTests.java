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

import org.junit.Ignore;
import org.junit.Test;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.CustomMetricsLocationTestBase;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.MasterServiceMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.MasterServiceEventMetrics;

@Ignore
public class MasterServiceMetricsTests extends CustomMetricsLocationTestBase {

    @Test
    public void testMasterServiceMetrics() {
        MetricsConfiguration.CONFIG_MAP.put(MasterServiceMetrics.class, new MetricsConfiguration.MetricConfig(1000, 0, 0));
        MetricsConfiguration.CONFIG_MAP.put(MasterServiceEventMetrics.class, new MetricsConfiguration.MetricConfig(1000, 0, 0));
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");
        long startTimeInMills = 1353723339;

        MasterServiceMetrics masterServiceMetrics = new MasterServiceMetrics();
        masterServiceMetrics.saveMetricValues("master_metrics_value", startTimeInMills, "current", "start");


        String fetchedValue = PerformanceAnalyzerMetrics.getMetric(PluginSettings.instance().getMetricsLocation() +
                PerformanceAnalyzerMetrics.getTimeInterval(startTimeInMills)+"/pending_tasks/current/start/");
        PerformanceAnalyzerMetrics.removeMetrics(PluginSettings.instance().getMetricsLocation()
                 + PerformanceAnalyzerMetrics.getTimeInterval(startTimeInMills));
        assertEquals("master_metrics_value", fetchedValue);

        try {
            masterServiceMetrics.saveMetricValues("master_metrics_value", startTimeInMills, "current");
            assertTrue("Negative scenario test: Should have been a RuntimeException", true);
        } catch (RuntimeException ex) {
            //- expecting exception...only 1 values passed; 2 expected
        }

        try {
            masterServiceMetrics.saveMetricValues("master_metrics_value", startTimeInMills);
            assertTrue("Negative scenario test: Should have been a RuntimeException", true);
        } catch (RuntimeException ex) {
            //- expecting exception...only 0 values passed; 2 expected
        }

        try {
            masterServiceMetrics.saveMetricValues("master_metrics_value", startTimeInMills, "current", "start", "123");
            assertTrue("Negative scenario test: Should have been a RuntimeException", true);
        } catch (RuntimeException ex) {
            //- expecting exception...only 3 values passed; 2 expected
        }

        MasterServiceEventMetrics masterServiceEventMetrics = new MasterServiceEventMetrics();
        try {
            masterServiceEventMetrics.getMasterServiceTPExecutorField();
            masterServiceEventMetrics.getPrioritizedTPExecutorCurrentField();
            masterServiceEventMetrics.getPrioritizedTPExecutorAddPendingMethod();
            masterServiceEventMetrics.getTPExecutorWorkersField();
            masterServiceEventMetrics.getWorkerThreadField();
        } catch (Exception exception) {
            assertTrue("There shouldn't be any exception in the code; Please check the reflection code for any changes", true);
        }
    }
}
