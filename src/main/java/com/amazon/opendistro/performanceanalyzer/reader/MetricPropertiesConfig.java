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


package com.amazon.opendistro.performanceanalyzer.reader;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.CircuitBreakerDimension;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.CircuitBreakerValue;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.DiskDimension;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.DiskValue;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.HeapDimension;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.HeapValue;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.IPDimension;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.IPValue;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.MasterPendingValue;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.MetricName;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.ShardStatsDerivedDimension;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.ShardStatsValue;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.TCPDimension;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.TCPValue;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.ThreadPoolDimension;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.ThreadPoolValue;
import com.amazon.opendistro.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.google.common.annotations.VisibleForTesting;

public class MetricPropertiesConfig {

    /**
     *  Find files under /dev/shm/performanceanalyzer/TS_BUCKET/metricPathElements
     * @param metricPathElements path element array
     * @return a list of Files
     */
    static FileHandler createFileHandler(String... metricPathElements)
    {
        return new FileHandler() {
            @Override
            public List<File> findFiles4Metric(
                    long startTimeThirtySecondBucket) {
                List<File> ret = new ArrayList<File>(1);
                StringBuilder sb = new StringBuilder();
                sb.append(getRootLocation());
                sb.append(startTimeThirtySecondBucket);

                for (String element: metricPathElements) {
                    sb.append(File.separator);
                    sb.append(element);
                }
                File metricFile = new File(sb.toString());
                if (metricFile.exists()) {
                    ret.add(metricFile);
                }
                return ret;
            }

        };
    }

    public static class ShardStatFileHandler extends FileHandler {
        @Override
        public List<File> findFiles4Metric(long timeBucket) {
            File indicesFolder = new File(
                    this.getRootLocation()
                    + File.separator
                    + timeBucket
                    + File.separator
                    + PerformanceAnalyzerMetrics.sIndicesPath);

            if (!indicesFolder.exists()) {
                return Collections.emptyList();
            }

            List<File> metricFiles = new ArrayList<>();

            for (File indexFolder : indicesFolder.listFiles()) {
                for (File shardIdFile: indexFolder.listFiles()) {
                    metricFiles.add(shardIdFile);
                }
            }
            return metricFiles;
        }

        @Override
        public String filePathRegex() {
            // getRootLocation() may or may not end with File.separator.  So
            // I put ? next to File.separator.
            return getRootLocation() + File.separator + "?\\d+" + File.separator
                    + "indices" + File.separator + "(.*)" + File.separator
                    + "(\\d+)";
        }
    }

    private final
    Map<MetricName, MetricProperties> metricName2Property;

    private static final MetricPropertiesConfig instance = new
            MetricPropertiesConfig();

    private MetricPropertiesConfig() {
        metricName2Property = new HashMap<>();

        metricName2Property.put(MetricName.circuit_breaker,
                new MetricProperties(CircuitBreakerDimension.values(),
                        CircuitBreakerValue.values(),
                        createFileHandler(PerformanceAnalyzerMetrics.sCircuitBreakerPath)));
        metricName2Property.put(MetricName.heap_metrics,
                new MetricProperties(HeapDimension.values(),
                        HeapValue.values(),
                        createFileHandler(PerformanceAnalyzerMetrics.sHeapPath)));
        metricName2Property.put(MetricName.disk_metrics,
                new MetricProperties(DiskDimension.values(),
                        DiskValue.values(),
                        createFileHandler(PerformanceAnalyzerMetrics.sDisksPath)));
        metricName2Property.put(MetricName.tcp_metrics,
                new MetricProperties(TCPDimension.values(),
                        TCPValue.values(),
                        createFileHandler(PerformanceAnalyzerMetrics.sTCPPath)));
        metricName2Property.put(MetricName.ip_metrics,
                new MetricProperties(IPDimension.values(),
                        IPValue.values(),
                        createFileHandler(PerformanceAnalyzerMetrics.sIPPath)));
        metricName2Property.put(MetricName.thread_pool,
                new MetricProperties(ThreadPoolDimension.values(),
                        ThreadPoolValue.values(),
                        createFileHandler(PerformanceAnalyzerMetrics.sThreadPoolPath)));
        metricName2Property.put(MetricName.shard_stats,
                new MetricProperties(
                        ShardStatsDerivedDimension.values(),
                        MetricProperties.EMPTY_DIMENSION,
                        ShardStatsValue.values(),
                        new ShardStatFileHandler()
                        ));
        metricName2Property.put(MetricName.master_pending,
                new MetricProperties(MetricProperties.EMPTY_DIMENSION,
                        MasterPendingValue.values(),
                        createFileHandler(PerformanceAnalyzerMetrics.sPendingTasksPath,
                                PerformanceAnalyzerMetrics.MASTER_CURRENT,
                                PerformanceAnalyzerMetrics.MASTER_META_DATA)
                        ));
    }

    public static MetricPropertiesConfig getInstance() {
        return instance;
    }

    public MetricProperties getProperty(MetricName name) {
        return metricName2Property.get(name);
    }

    @VisibleForTesting
    Map<MetricName, MetricProperties> getMetricName2Property() {
        return metricName2Property;
    }

}
