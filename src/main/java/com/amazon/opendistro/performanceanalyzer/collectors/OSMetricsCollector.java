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

import java.util.Map;

import com.amazon.opendistro.performanceanalyzer.metrics.MetricsConfiguration;

import com.amazon.opendistro.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.performanceanalyzer.metrics.MetricsProcessor;

import com.amazon.opendistro.performanceanalyzer.metrics_generator.DiskIOMetricsGenerator;
import com.amazon.opendistro.performanceanalyzer.metrics_generator.OSMetricsGenerator;
import com.amazon.opendistro.performanceanalyzer.OSMetricsGeneratorFactory;
import com.amazon.opendistro.performanceanalyzer.metrics_generator.CPUPagingActivityGenerator;
import com.amazon.opendistro.performanceanalyzer.metrics_generator.SchedMetricsGenerator;
import com.amazon.opendistro.performanceanalyzer.jvm.ThreadList;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.OS_Metrics;

public class OSMetricsCollector extends PerformanceAnalyzerMetricsCollector implements MetricsProcessor {
    public static final int SAMPLING_TIME_INTERVAL = MetricsConfiguration.CONFIG_MAP.get(ThreadList.class).samplingInterval;
    private static final int KEYS_PATH_LENGTH = 1;
    private StringBuilder value;
    private OSMetricsGenerator osMetricsGenerator;

    public enum MetaDataFields {
        threadName
    }

    public OSMetricsCollector() {
        super(SAMPLING_TIME_INTERVAL, "OSMetrics");
        value = new StringBuilder();
        osMetricsGenerator = OSMetricsGeneratorFactory.getInstance();
    }

    @Override
    public void collectMetrics(long startTime) {

        CPUPagingActivityGenerator threadCPUPagingActivityGenerator = osMetricsGenerator.getPagingActivityGenerator();
        threadCPUPagingActivityGenerator.addSample();

        SchedMetricsGenerator schedMetricsGenerator = osMetricsGenerator.getSchedMetricsGenerator();
        schedMetricsGenerator.addSample();

        Map<Long, ThreadList.threadState> threadStates = ThreadList.getNativeTidMap();

        DiskIOMetricsGenerator diskIOMetricsGenerator = osMetricsGenerator.getDiskIOMetricsGenerator();
        diskIOMetricsGenerator.addSample();

        for (String threadId : osMetricsGenerator.getAllThreadIds()) {
            value.setLength(0);
            value.append(PerformanceAnalyzerMetrics.getCurrentTimeMetric())
                    .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(OS_Metrics.cpu.name())
                    .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor)
                    .append(threadCPUPagingActivityGenerator.getCPUUtilization(threadId));

            if (threadCPUPagingActivityGenerator.hasPagingActivity(threadId)) {
                value.append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(OS_Metrics.paging_majflt.name())
                        .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor)
                        .append(threadCPUPagingActivityGenerator.getMajorFault(threadId));
                value.append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(OS_Metrics.paging_minflt.name())
                        .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor)
                        .append(threadCPUPagingActivityGenerator.getMinorFault(threadId));
                value.append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(OS_Metrics.rss.name())
                        .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor)
                        .append(threadCPUPagingActivityGenerator.getResidentSetSize(threadId));
            }

            if (schedMetricsGenerator.hasSchedMetrics(threadId)) {
                value.append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(OS_Metrics.runtime.name())
                        .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(schedMetricsGenerator.getAvgRuntime(threadId));
                value.append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(OS_Metrics.waittime.name())
                        .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(schedMetricsGenerator.getAvgWaittime(threadId));
                value.append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(OS_Metrics.ctxrate.name())
                        .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(schedMetricsGenerator.getContextSwitchRate(threadId));
            }

            ThreadList.threadState threadState = threadStates.get(Long.valueOf(threadId));
            if (threadState != null) {
                value.append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(OS_Metrics.heap_usage.name())
                        .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(threadState.heapAllocRate);
                value.append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(MetaDataFields.threadName.name())
                        .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(threadState.threadName);
                value.append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(OS_Metrics.avgBlockedTime.name())
                        .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(threadState.avgBlockedTime);
                value.append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(OS_Metrics.blockedCount.name())
                        .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(threadState.blockedCount);
            }

            if (diskIOMetricsGenerator.hasDiskIOMetrics(threadId)) {
                value.append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(OS_Metrics.avgReadThroughputBps.name())
                        .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor)
                        .append(diskIOMetricsGenerator.getAvgReadThroughputBps(threadId));
                value.append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(OS_Metrics.avgWriteThroughputBps.name())
                        .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor)
                        .append(diskIOMetricsGenerator.getAvgWriteThroughputBps(threadId));
                value.append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(OS_Metrics.avgTotalThroughputBps.name())
                        .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor)
                        .append(diskIOMetricsGenerator.getAvgTotalThroughputBps(threadId));
                value.append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(OS_Metrics.avgReadSyscallRate.name())
                        .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor)
                        .append(diskIOMetricsGenerator.getAvgReadSyscallRate(threadId));
                value.append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(OS_Metrics.avgWriteSyscallRate.name())
                        .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor)
                        .append(diskIOMetricsGenerator.getAvgWriteSyscallRate(threadId));
                value.append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(OS_Metrics.avgTotalSyscallRate.name())
                        .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor)
                        .append(diskIOMetricsGenerator.getAvgTotalSyscallRate(threadId));
            }

            saveMetricValues(value.toString(), startTime, threadId);
        }
    }

    @Override
    public String getMetricsPath(long startTime, String... keysPath) {
        // throw exception if keys.length is not equal to 1...which is thread ID
        if (keysPath.length != KEYS_PATH_LENGTH) {
            throw new RuntimeException("keys length should be " + KEYS_PATH_LENGTH);
        }
        return PerformanceAnalyzerMetrics.generatePath(startTime, PerformanceAnalyzerMetrics.sThreadsPath,
                keysPath[0], PerformanceAnalyzerMetrics.sOSPath);
    }
}
