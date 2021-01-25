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


import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ESResources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.MasterPendingValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsProcessor;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.elasticsearch.cluster.service.PendingClusterTask;
import java.util.HashMap;
import java.util.List;

@SuppressWarnings("unchecked")
public class MasterServiceMetrics extends PerformanceAnalyzerMetricsCollector implements MetricsProcessor {
    public static final int SAMPLING_TIME_INTERVAL = MetricsConfiguration.CONFIG_MAP.get(MasterServiceMetrics.class).samplingInterval;
    private static final Logger LOG = LogManager.getLogger(MasterServiceMetrics.class);
    private static final int KEYS_PATH_LENGTH = 2;
    private StringBuilder value;

    public MasterServiceMetrics() {
        super(SAMPLING_TIME_INTERVAL, "MasterServiceMetrics");
        value = new StringBuilder();
    }

    @Override
    public String getMetricsPath(long startTime, String... keysPath) {
        // throw exception if keysPath.length is not equal to 2
        // (Keys should be Pending_Task_ID, start/finish OR current, metadata)
        if (keysPath.length != KEYS_PATH_LENGTH) {
            throw new RuntimeException("keys length should be " + KEYS_PATH_LENGTH);
        }

        return PerformanceAnalyzerMetrics.generatePath(startTime, PerformanceAnalyzerMetrics.sPendingTasksPath,
                keysPath[0], keysPath[1]);
    }

    @Override
    public void collectMetrics(long startTime) {
        try {
            if (ESResources.INSTANCE.getClusterService() == null
                    || ESResources.INSTANCE.getClusterService().getMasterService() == null) {
                return;
            }

            List<PendingClusterTask> pendingTasks = ESResources.INSTANCE.getClusterService().getMasterService()
                                                    .pendingTasks();
            HashMap<String,Integer> pendingTaskCountPerTaskType = new HashMap<>();

            for (PendingClusterTask pendingTask: pendingTasks){
                String pendingTaskType = pendingTask.getSource().toString().split(" ",2)[0];
                if (pendingTaskCountPerTaskType.containsKey(pendingTaskType)) {
                    pendingTaskCountPerTaskType.put(pendingTaskType, pendingTaskCountPerTaskType.get(pendingTaskType) + 1);
                }
                else{
                    pendingTaskCountPerTaskType.put(pendingTaskType,1);
                }
            }

            value.setLength(0);
            value.append(PerformanceAnalyzerMetrics.getJsonCurrentMilliSeconds());
            for (String pendingTaskType : pendingTaskCountPerTaskType.keySet()){
                value.append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor);
                value.append(new MasterPendingStatus(
                        pendingTaskType,pendingTaskCountPerTaskType.get(pendingTaskType)).serialize());
            }

            saveMetricValues(value.toString(), startTime,
                    PerformanceAnalyzerMetrics.MASTER_CURRENT, PerformanceAnalyzerMetrics.MASTER_META_DATA);

        } catch (Exception ex) {
            LOG.debug("Exception in Collecting Master Metrics: {} for startTime {}", () -> ex.toString(), () -> startTime);
        }
    }

    public static class MasterPendingStatus extends MetricStatus {
        private final String pendingTaskType;
        private final int pendingTasksCount;

        public MasterPendingStatus(String pendingTaskType, int pendingTasksCount) {
            this.pendingTaskType = pendingTaskType;
            this.pendingTasksCount = pendingTasksCount;

        }

        @JsonProperty(AllMetrics.MasterPendingTaskDimension.Constants.PENDING_TASK_TYPE)
        public String getMasterTaskType(){
            return pendingTaskType;
        }

        @JsonProperty(MasterPendingValue.Constants.PENDING_TASKS_COUNT_VALUE)
        public int getPendingTasksCount() {
            return pendingTasksCount;
        }

    }
}
