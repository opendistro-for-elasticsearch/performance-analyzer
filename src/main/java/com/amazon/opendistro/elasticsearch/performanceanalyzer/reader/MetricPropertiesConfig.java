/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.reader;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CircuitBreakerDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CircuitBreakerValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.DiskDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.DiskValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.HeapDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.HeapValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.IPDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.IPValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.MasterPendingValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.MetricName;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ShardStatsDerivedDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ShardStatsValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.TCPDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.TCPValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ThreadPoolDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ThreadPoolValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;
import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class MetricPropertiesConfig {

  /**
   * Find files under /dev/shm/performanceanalyzer/TS_BUCKET/metricPathElements
   *
   * @param metricPathElements path element array
   * @return a list of Files
   */
  static FileHandler createFileHandler(String... metricPathElements) {
    return new FileHandler() {
      @Override
      public List<File> findFiles4Metric(long startTimeThirtySecondBucket) {
        List<File> ret = new ArrayList<File>(1);
        StringBuilder sb = new StringBuilder();
        sb.append(getRootLocation());
        sb.append(startTimeThirtySecondBucket);

        for (String element : metricPathElements) {
          sb.append(File.separator);
          sb.append(element);
        }
        File metricFile = new File(sb.toString());
        if (metricFile.exists()) {
          ret.add(metricFile);
        }
        return ret;
      }

      public List<Event> getMetricData(Map<String, List<Event>> metricDataMap) {
        Objects.requireNonNull(metricDataMap);
        List<Event> entries = metricDataMap.get(metricPathElements[0]);
        return (entries == null ? Collections.emptyList() : entries);
      }
    };
  }

  public static class ShardStatFileHandler extends FileHandler {
    @Override
    public List<File> findFiles4Metric(long timeBucket) {
      File indicesFolder =
          new File(
              this.getRootLocation()
                  + File.separator
                  + timeBucket
                  + File.separator
                  + PerformanceAnalyzerMetrics.sIndicesPath);

      if (!indicesFolder.exists()) {
        return Collections.emptyList();
      }

      List<File> metricFiles = new ArrayList<>();

      File[] files = indicesFolder.listFiles();
      if (files != null) {
        for (File indexFolder : files) {
          if (indexFolder != null) {
            File[] shardIdFiles = indexFolder.listFiles();
            if (shardIdFiles != null) {
              for (File shardIdFile : shardIdFiles) {
                metricFiles.add(shardIdFile);
              }
            }
          }
        }
      }
      return metricFiles;
    }

    // An example shard data can be:
    // ^indices/nyc_taxis/29
    // {"current_time":1566413966497}
    // {"Indexing_ThrottleTime":0,"Cache_Query_Hit":0,"Cache_Query_Miss":0,"Cache_Query_Size":0,
    // "Cache_FieldData_Eviction":0,"Cache_FieldData_Size":0,"Cache_Request_Hit":0,
    // "Cache_Request_Miss":0,"Cache_Request_Eviction":0,"Cache_Request_Size":0,"Refresh_Event":2,
    // "Refresh_Time":0,"Flush_Event":0,"Flush_Time":0,"Merge_Event":0,"Merge_Time":0,
    // "Merge_CurrentEvent":0,"Indexing_Buffer":0,"Segments_Total":0,"Segments_Memory":0,
    // "Terms_Memory":0,"StoredFields_Memory":0,"TermVectors_Memory":0,"Norms_Memory":0,
    // "Points_Memory":0,"DocValues_Memory":0,"IndexWriter_Memory":0,"VersionMap_Memory":0,"Bitset_Memory":0}$
    public List<Event> getMetricData(Map<String, List<Event>> metricDataMap) {
      Objects.requireNonNull(metricDataMap);
      return metricDataMap.computeIfAbsent(
          PerformanceAnalyzerMetrics.sIndicesPath, k -> Collections.emptyList());
    }

    @Override
    public String filePathRegex() {
      // getRootLocation() may or may not end with File.separator.  So
      // I put ? next to File.separator.
      return getRootLocation()
          + File.separator
          + "?\\d+"
          + File.separator
          + PerformanceAnalyzerMetrics.sIndicesPath
          + File.separator
          + "(.*)"
          + File.separator
          + "(\\d+)";
    }
  }

  private final Map<MetricName, MetricProperties> metricName2Property;

  private static final MetricPropertiesConfig INSTANCE = new MetricPropertiesConfig();

  private Map<MetricName, String> metricPathMap;
  private Map<String, MetricName> eventKeyToMetricNameMap;

  private MetricPropertiesConfig() {
    metricPathMap = new HashMap<>();
    metricPathMap.put(MetricName.CIRCUIT_BREAKER, PerformanceAnalyzerMetrics.sCircuitBreakerPath);
    metricPathMap.put(MetricName.HEAP_METRICS, PerformanceAnalyzerMetrics.sHeapPath);
    metricPathMap.put(MetricName.DISK_METRICS, PerformanceAnalyzerMetrics.sDisksPath);
    metricPathMap.put(MetricName.TCP_METRICS, PerformanceAnalyzerMetrics.sTCPPath);
    metricPathMap.put(MetricName.IP_METRICS, PerformanceAnalyzerMetrics.sIPPath);
    metricPathMap.put(MetricName.THREAD_POOL, PerformanceAnalyzerMetrics.sThreadPoolPath);
    metricPathMap.put(MetricName.SHARD_STATS, PerformanceAnalyzerMetrics.sIndicesPath);
    metricPathMap.put(MetricName.MASTER_PENDING, PerformanceAnalyzerMetrics.sPendingTasksPath);

    eventKeyToMetricNameMap = new HashMap<>();
    eventKeyToMetricNameMap.put(
        PerformanceAnalyzerMetrics.sCircuitBreakerPath, MetricName.CIRCUIT_BREAKER);
    eventKeyToMetricNameMap.put(PerformanceAnalyzerMetrics.sHeapPath, MetricName.HEAP_METRICS);
    eventKeyToMetricNameMap.put(PerformanceAnalyzerMetrics.sDisksPath, MetricName.DISK_METRICS);
    eventKeyToMetricNameMap.put(PerformanceAnalyzerMetrics.sTCPPath, MetricName.TCP_METRICS);
    eventKeyToMetricNameMap.put(PerformanceAnalyzerMetrics.sIPPath, MetricName.IP_METRICS);
    eventKeyToMetricNameMap.put(PerformanceAnalyzerMetrics.sThreadPoolPath, MetricName.THREAD_POOL);
    eventKeyToMetricNameMap.put(PerformanceAnalyzerMetrics.sIndicesPath, MetricName.SHARD_STATS);
    eventKeyToMetricNameMap.put(
        PerformanceAnalyzerMetrics.sPendingTasksPath, MetricName.MASTER_PENDING);

    metricName2Property = new HashMap<>();

    metricName2Property.put(
        MetricName.CIRCUIT_BREAKER,
        new MetricProperties(
            CircuitBreakerDimension.values(),
            CircuitBreakerValue.values(),
            createFileHandler(metricPathMap.get(MetricName.CIRCUIT_BREAKER))));
    metricName2Property.put(
        MetricName.HEAP_METRICS,
        new MetricProperties(
            HeapDimension.values(),
            HeapValue.values(),
            createFileHandler(metricPathMap.get(MetricName.HEAP_METRICS))));
    metricName2Property.put(
        MetricName.DISK_METRICS,
        new MetricProperties(
            DiskDimension.values(),
            DiskValue.values(),
            createFileHandler(metricPathMap.get(MetricName.DISK_METRICS))));
    metricName2Property.put(
        MetricName.TCP_METRICS,
        new MetricProperties(
            TCPDimension.values(),
            TCPValue.values(),
            createFileHandler(metricPathMap.get(MetricName.TCP_METRICS))));
    metricName2Property.put(
        MetricName.IP_METRICS,
        new MetricProperties(
            IPDimension.values(),
            IPValue.values(),
            createFileHandler(metricPathMap.get(MetricName.IP_METRICS))));
    metricName2Property.put(
        MetricName.THREAD_POOL,
        new MetricProperties(
            ThreadPoolDimension.values(),
            ThreadPoolValue.values(),
            createFileHandler(metricPathMap.get(MetricName.THREAD_POOL))));
    metricName2Property.put(
        MetricName.SHARD_STATS,
        new MetricProperties(
            ShardStatsDerivedDimension.values(),
            MetricProperties.EMPTY_DIMENSION,
            ShardStatsValue.values(),
            new ShardStatFileHandler()));
    metricName2Property.put(
        MetricName.MASTER_PENDING,
        new MetricProperties(
            MetricProperties.EMPTY_DIMENSION,
            MasterPendingValue.values(),
            createFileHandler(
                metricPathMap.get(MetricName.MASTER_PENDING),
                PerformanceAnalyzerMetrics.MASTER_CURRENT,
                PerformanceAnalyzerMetrics.MASTER_META_DATA)));
  }

  public static MetricPropertiesConfig getInstance() {
    return INSTANCE;
  }

  public MetricProperties getProperty(MetricName name) {
    return metricName2Property.get(name);
  }

  public Map<MetricName, String> getMetricPathMap() {
    return metricPathMap;
  }

  Map<String, MetricName> getEventKeyToMetricNameMap() {
    return eventKeyToMetricNameMap;
  }

  @VisibleForTesting
  Map<MetricName, MetricProperties> getMetricName2Property() {
    return metricName2Property;
  }
}
