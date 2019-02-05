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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.cluster.service.SourcePrioritizedRunnable;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;

import com.amazon.opendistro.performanceanalyzer.ESResources;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.MasterMetrics;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.MasterPendingValue;
import com.amazon.opendistro.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.performanceanalyzer.metrics.MetricsProcessor;
import com.amazon.opendistro.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.fasterxml.jackson.annotation.JsonProperty;

@SuppressWarnings("unchecked")
public class MasterServiceMetrics extends PerformanceAnalyzerMetricsCollector implements MetricsProcessor {
    public static final int SAMPLING_TIME_INTERVAL = MetricsConfiguration.CONFIG_MAP.get(MasterServiceMetrics.class).samplingInterval;
    private static final Logger LOG = LogManager.getLogger(MasterServiceMetrics.class);
    private Queue<Runnable> masterServiceCurrentQueue;
    private PrioritizedEsThreadPoolExecutor prioritizedEsThreadPoolExecutor;
    private long lastTaskInsertionOrder;
    private static final int KEYS_PATH_LENGTH = 2;
    private static final int TPEXECUTOR_ADD_PENDING_PARAM_COUNT = 3;
    private StringBuilder value;

    public MasterServiceMetrics() {
        super(SAMPLING_TIME_INTERVAL, "MasterServiceMetrics");
        masterServiceCurrentQueue = null;
        prioritizedEsThreadPoolExecutor = null;
        lastTaskInsertionOrder = -1;
        value = new StringBuilder();
    }

    @Override
    public String getMetricsPath(long startTime, String... keysPath) {
        // throw exception if keysPath.length is not equal to 2
        // (Keys should be Pending_Task_ID, start/finish OR current, metadata)
        if (keysPath.length != KEYS_PATH_LENGTH) {
            throw new RuntimeException("keys length should be " + KEYS_PATH_LENGTH);
        }


        return PerformanceAnalyzerMetrics.generatePath(startTime, PerformanceAnalyzerMetrics.sPendingTasksPath, keysPath[0], keysPath[1]);
    }

    @Override
    public void collectMetrics(long startTime) {
        try {
            if (ESResources.INSTANCE.getClusterService() == null
                    || ESResources.INSTANCE.getClusterService().getMasterService() == null) {
                return;
            }

            value.setLength(0);
            value.append(PerformanceAnalyzerMetrics.getJsonCurrentMilliSeconds())
                    .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor);
            value.append(new MasterPendingStatus(
                    ESResources.INSTANCE.getClusterService().getMasterService()
                            .numberOfPendingTasks()).serialize());
            saveMetricValues(value.toString(), startTime,
                    PerformanceAnalyzerMetrics.MASTER_CURRENT, PerformanceAnalyzerMetrics.MASTER_META_DATA);

            value.setLength(0);
            Queue<Runnable> current = getMasterServiceCurrentQueue();

            if (current == null || current.size() == 0) {
                generateFinishMetrics(startTime);
                return;
            }

            List<PrioritizedEsThreadPoolExecutor.Pending> pending = new ArrayList<>();

            Object[] parameters = new Object[TPEXECUTOR_ADD_PENDING_PARAM_COUNT];
            parameters[0] = new ArrayList<>(current);
            parameters[1] = pending;
            parameters[2] = true;

            getPrioritizedTPExecutorAddPendingMethod().invoke(prioritizedEsThreadPoolExecutor, parameters);

            if (pending.size() != 0) {
                PrioritizedEsThreadPoolExecutor.Pending firstPending = pending.get(0);

                if (lastTaskInsertionOrder != firstPending.insertionOrder) {
                    generateFinishMetrics(startTime);
                    SourcePrioritizedRunnable task = (SourcePrioritizedRunnable) firstPending.task;
                    lastTaskInsertionOrder = firstPending.insertionOrder;
                    int firstSpaceIndex = task.source().indexOf(" ");
                    value.append(PerformanceAnalyzerMetrics.getCurrentTimeMetric())
                            .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(MasterMetrics.ID.toString())
                            .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(lastTaskInsertionOrder)
                            .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(MasterMetrics.priority.toString())
                            .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(firstPending.priority)
                            .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(MasterMetrics.startTime.toString())
                            //- as it is sampling, we won't exactly know the start time of the current task, we will be
                            //- capturing start time as midpoint of previous time bucket
                            .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(startTime - SAMPLING_TIME_INTERVAL / 2)
                            .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(MasterMetrics.type.toString())
                            .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor)
                            .append(firstSpaceIndex == -1 ? task.source() : task.source().substring(0, firstSpaceIndex))
                            .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(MasterMetrics.metadata.toString())
                            .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor)
                            .append(firstSpaceIndex == -1 ? "" : task.source().substring(firstSpaceIndex))
                            .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(MasterMetrics.ageInMillis.toString())
                            .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(task.getAgeInMillis());

                    saveMetricValues(value.toString(), startTime, String.valueOf(lastTaskInsertionOrder),
                            PerformanceAnalyzerMetrics.START_FILE_NAME);
                    value.setLength(0);
                }
            } else {
                generateFinishMetrics(startTime);
            }
        } catch (Exception ex) {
            LOG.debug("Exception in Collecting Master Metrics: {} for startTime {}", () -> ex.toString(), () -> startTime);
        }
    }

    private void generateFinishMetrics(long startTime) {
        if (lastTaskInsertionOrder != -1) {
            value.append(PerformanceAnalyzerMetrics.getCurrentTimeMetric())
                    .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(MasterMetrics.finishTime.toString())
                    .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(startTime - SAMPLING_TIME_INTERVAL / 2);
            saveMetricValues(value.toString(), startTime,
                    String.valueOf(lastTaskInsertionOrder),
                    PerformanceAnalyzerMetrics.FINISH_FILE_NAME);
            value.setLength(0);
            lastTaskInsertionOrder = -1;
        }
    }

    //- Separated to have a unit test; and catch any code changes around this field
    Field getMasterServiceTPExecutorField() throws Exception {
        Field threadPoolExecutorField = MasterService.class.getDeclaredField("threadPoolExecutor");
        threadPoolExecutorField.setAccessible(true);
        return threadPoolExecutorField;
    }

    //- Separated to have a unit test; and catch any code changes around this field
    Field getPrioritizedTPExecutorCurrentField() throws Exception {
        Field currentField = PrioritizedEsThreadPoolExecutor.class.getDeclaredField("current");
        currentField.setAccessible(true);
        return currentField;
    }

    //- Separated to have a unit test; and catch any code changes around this field
    Method getPrioritizedTPExecutorAddPendingMethod() throws Exception {
        Class<?>[] classArray = new Class<?>[TPEXECUTOR_ADD_PENDING_PARAM_COUNT];
        classArray[0] = List.class;
        classArray[1] = List.class;
        classArray[2] = boolean.class;
        Method addPendingMethod = PrioritizedEsThreadPoolExecutor.class.getDeclaredMethod("addPending", classArray);
        addPendingMethod.setAccessible(true);
        return addPendingMethod;
    }

    private Queue<Runnable> getMasterServiceCurrentQueue() throws Exception {
        if (masterServiceCurrentQueue == null) {
            if (ESResources.INSTANCE.getClusterService() != null) {
                MasterService masterService = ESResources.INSTANCE.getClusterService().getMasterService();

                if (masterService != null) {
                    if (prioritizedEsThreadPoolExecutor == null) {
                        prioritizedEsThreadPoolExecutor =
                                (PrioritizedEsThreadPoolExecutor) getMasterServiceTPExecutorField().get(masterService);
                    }

                    masterServiceCurrentQueue =
                            (Queue<Runnable>) getPrioritizedTPExecutorCurrentField().get(prioritizedEsThreadPoolExecutor);
                }
            }
        }

        return masterServiceCurrentQueue;
    }

    public static class MasterPendingStatus extends MetricStatus {
        private final int pendingTasksCount;
        public MasterPendingStatus(int pendingTasksCount) {
            this.pendingTasksCount = pendingTasksCount;
        }

        @JsonProperty(MasterPendingValue.Constants.PENDING_TASKS_COUNT_VALUE)
        public int getPendingTasksCount() {
            return pendingTasksCount;
        }
    }
}

