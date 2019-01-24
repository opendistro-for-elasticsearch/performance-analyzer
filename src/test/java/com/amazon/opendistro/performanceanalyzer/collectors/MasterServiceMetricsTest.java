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

package com.amazon.opendistro.performanceanalyzer.collectors;

import com.amazon.opendistro.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MasterServiceMetricsTest {

    @Test
    public void testMasterServiceMetrics() {
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");
        long startTimeInMills = System.currentTimeMillis() + 4 * 6000000;

        MasterServiceMetrics masterServiceMetrics = new MasterServiceMetrics();
        masterServiceMetrics.saveMetricValues("master_metrics_value", startTimeInMills, "current", "start");


        String fetchedValue = PerformanceAnalyzerMetrics.getMetric(PerformanceAnalyzerMetrics.sDevShmLocation +
                PerformanceAnalyzerMetrics.getTimeInterval(startTimeInMills)+"/pending_tasks/current/start/");
        PerformanceAnalyzerMetrics.removeMetrics(PerformanceAnalyzerMetrics.sDevShmLocation);
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

        try {
            masterServiceMetrics.getMasterServiceTPExecutorField();
            masterServiceMetrics.getPrioritizedTPExecutorCurrentField();
            masterServiceMetrics.getPrioritizedTPExecutorAddPendingMethod();
        } catch (Exception exception) {
            assertTrue("There shouldn't be any exception in the code; Please check the reflection code for any changes", true);
        }
    }
}
