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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.HashSet;
import java.util.concurrent.ThreadPoolExecutor;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.cluster.service.SourcePrioritizedRunnable;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ESResources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.Master_Metric_Dimensions;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.Master_Metric_Values;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.ThreadIDUtil;

@SuppressWarnings("unchecked")
public class MasterServiceEventMetrics extends PerformanceAnalyzerMetricsCollector implements MetricsProcessor {
    public static final int SAMPLING_TIME_INTERVAL = MetricsConfiguration.CONFIG_MAP.get(MasterServiceMetrics.class).samplingInterval;
    private static final Logger LOG = LogManager.getLogger(MasterServiceEventMetrics.class);
    private long lastTaskInsertionOrder;
    private static final int KEYS_PATH_LENGTH = 3;
    private StringBuilder value;
    private static final int TPEXECUTOR_ADD_PENDING_PARAM_COUNT = 3;
    private Queue<Runnable> masterServiceCurrentQueue;
    private PrioritizedEsThreadPoolExecutor prioritizedEsThreadPoolExecutor;
    private HashSet<Object> masterServiceWorkers;
    private long currentThreadId;
    private Object currentWorker;

    public MasterServiceEventMetrics() {
        super(SAMPLING_TIME_INTERVAL, "MasterServiceEventMetrics");
        masterServiceCurrentQueue = null;
        masterServiceWorkers = null;
        prioritizedEsThreadPoolExecutor = null;
        currentWorker = null;
        currentThreadId = -1;
        lastTaskInsertionOrder = -1;
        value = new StringBuilder();
    }

    @Override
    public String getMetricsPath(long startTime, String... keysPath) {
        // throw exception if keys.length is not equal to 3 (Keys should be threadID, taskID, start/finish)
        if (keysPath.length != KEYS_PATH_LENGTH) {
            throw new RuntimeException("keys length should be " + KEYS_PATH_LENGTH);
        }
        return PerformanceAnalyzerMetrics.generatePath(startTime, PerformanceAnalyzerMetrics.sThreadsPath,
                keysPath[0], PerformanceAnalyzerMetrics.sMasterTaskPath, keysPath[1], keysPath[2]);
    }

    @Override
    public void collectMetrics(long startTime) {
        try {
            if (ESResources.INSTANCE.getClusterService() == null
                    || ESResources.INSTANCE.getClusterService().getMasterService() == null) {
                return;
            }

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
                    value.append(PerformanceAnalyzerMetrics.getCurrentTimeMetric());
                    PerformanceAnalyzerMetrics.addMetricEntry(value, Master_Metric_Dimensions.MASTER_TASK_PRIORITY.toString(),
                            firstPending.priority.toString());
                    //- as it is sampling, we won't exactly know the start time of the current task, we will be
                    //- capturing start time as midpoint of previous time bucket
                    PerformanceAnalyzerMetrics.addMetricEntry(value, Master_Metric_Values.START_TIME.toString(),
                            startTime - SAMPLING_TIME_INTERVAL / 2);
                    PerformanceAnalyzerMetrics.addMetricEntry(value, Master_Metric_Dimensions.MASTER_TASK_TYPE.toString(),
                            firstSpaceIndex == -1 ? task.source() : task.source().substring(0, firstSpaceIndex));
                    PerformanceAnalyzerMetrics.addMetricEntry(value, Master_Metric_Dimensions.MASTER_TASK_METADATA.toString(),
                            firstSpaceIndex == -1 ? "" : task.source().substring(firstSpaceIndex));
                    PerformanceAnalyzerMetrics.addMetricEntry(value, Master_Metric_Dimensions.MASTER_TASK_AGE.toString(),
                            task.getAgeInMillis());

                    saveMetricValues(value.toString(), startTime, String.valueOf(getMasterThreadId()),
                            String.valueOf(lastTaskInsertionOrder), PerformanceAnalyzerMetrics.START_FILE_NAME);

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
            value.append(PerformanceAnalyzerMetrics.getCurrentTimeMetric());
            PerformanceAnalyzerMetrics.addMetricEntry(value, Master_Metric_Values.FINISH_TIME.toString(),
                    startTime - SAMPLING_TIME_INTERVAL / 2);
            saveMetricValues(value.toString(), startTime, String.valueOf(currentThreadId),
                    String.valueOf(lastTaskInsertionOrder), PerformanceAnalyzerMetrics.FINISH_FILE_NAME);
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
    Field getTPExecutorWorkersField() throws Exception {
        Field workersField = ThreadPoolExecutor.class.getDeclaredField("workers");
        workersField.setAccessible(true);
        return workersField;
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

    Queue<Runnable> getMasterServiceCurrentQueue() throws Exception {
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

    HashSet<Object> getMasterServiceWorkers() throws Exception {
        if (masterServiceWorkers == null) {
            if (ESResources.INSTANCE.getClusterService() != null) {
                MasterService masterService = ESResources.INSTANCE.getClusterService().getMasterService();

                if (masterService != null) {
                    if (prioritizedEsThreadPoolExecutor == null) {
                        prioritizedEsThreadPoolExecutor =
                                (PrioritizedEsThreadPoolExecutor) getMasterServiceTPExecutorField().get(masterService);
                    }

                    masterServiceWorkers =
                            (HashSet<Object>) getTPExecutorWorkersField().get(prioritizedEsThreadPoolExecutor);
                }
            }
        }

        return masterServiceWorkers;
    }


    long getMasterThreadId() throws Exception {
        HashSet<Object> currentWorkers = getMasterServiceWorkers();

        if(currentWorkers.size() > 0) {
            if(currentWorkers.size() > 1) {
                LOG.error("Master threads are more than 1 (expected); current Master threads count: {}", currentWorkers.size());
                currentThreadId = -1;
                currentWorker = null;
            } else {
                Object currentTopWorker = currentWorkers.iterator().next();
                if (currentWorker != currentTopWorker) {
                    currentWorker = currentTopWorker;
                    Thread masterThread = (Thread) getWorkerThreadField().get(currentWorker);
                    currentThreadId = ThreadIDUtil.INSTANCE.getNativeThreadId(masterThread.getId());
                }
            }
        } else {
            currentThreadId = -1;
            currentWorker = null;
        }

        return currentThreadId;
    }

    Field getWorkerThreadField() throws Exception {
        Class<?> tpExecutorWorkerClass = Class.forName("java.util.concurrent.ThreadPoolExecutor$Worker");
        Field workerField = tpExecutorWorkerClass.getDeclaredField("thread");
        workerField.setAccessible(true);
        return workerField;
    }
}

