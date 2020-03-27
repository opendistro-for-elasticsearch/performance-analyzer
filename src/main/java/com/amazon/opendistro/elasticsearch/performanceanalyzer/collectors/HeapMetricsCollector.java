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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.jvm.GCMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.jvm.HeapMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.GCType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.HeapDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.HeapValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.lang.management.MemoryUsage;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HeapMetricsCollector extends PerformanceAnalyzerMetricsCollector
    implements MetricsProcessor {
  private static final Logger LOG = LogManager.getLogger(HeapMetricsCollector.class);
  public static final int SAMPLING_TIME_INTERVAL =
      MetricsConfiguration.CONFIG_MAP.get(HeapMetricsCollector.class).samplingInterval;
  private static final int KEYS_PATH_LENGTH = 0;
  private StringBuilder value;

  public HeapMetricsCollector() {
    super(SAMPLING_TIME_INTERVAL, "HeapMetrics");
    value = new StringBuilder();
  }

  @Override
  public void collectMetrics(long startTime) {
    GCMetrics.runGCMetrics();

    value.setLength(0);
    value
        .append(PerformanceAnalyzerMetrics.getJsonCurrentMilliSeconds())
        .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor);
    value
        .append(
            new HeapStatus(
                    GCType.TOT_YOUNG_GC.toString(),
                    GCMetrics.getTotYoungGCCollectionCount(),
                    GCMetrics.getTotYoungGCCollectionTime())
                .serialize())
        .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor);

    value
        .append(
            new HeapStatus(
                    GCType.TOT_FULL_GC.toString(),
                    GCMetrics.getTotFullGCCollectionCount(),
                    GCMetrics.getTotFullGCCollectionTime())
                .serialize())
        .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor);

    for (Map.Entry<String, Supplier<MemoryUsage>> entry :
        HeapMetrics.getMemoryUsageSuppliers().entrySet()) {
      MemoryUsage memoryUsage = entry.getValue().get();

      value
          .append(
              new HeapStatus(
                      entry.getKey(),
                      memoryUsage.getCommitted(),
                      memoryUsage.getInit(),
                      memoryUsage.getMax(),
                      memoryUsage.getUsed())
                  .serialize())
          .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor);
    }

    saveMetricValues(value.toString(), startTime);
  }

  @Override
  public String getMetricsPath(long startTime, String... keysPath) {
    // throw exception if keys.length is not equal to 0
    if (keysPath.length != KEYS_PATH_LENGTH) {
      throw new RuntimeException("keys length should be " + KEYS_PATH_LENGTH);
    }

    return PerformanceAnalyzerMetrics.generatePath(startTime, PerformanceAnalyzerMetrics.sHeapPath);
  }

  public static class HeapStatus extends MetricStatus {
    // GC type like survivor
    private final String type;

    // -2 means this metric is undefined for a memory pool.  For example,
    // The memory pool Eden has no collectionCount metric.
    private static final long UNDEFINED = -2;

    // the total number of collections that have occurred
    private long collectionCount = UNDEFINED;

    // the approximate accumulated collection elapsed time in milliseconds
    private long collectionTime = UNDEFINED;

    // the amount of memory in bytes that is committed for the Java virtual machine to use
    private long committed = UNDEFINED;

    // the amount of memory in bytes that the Java virtual machine initially requests from the
    // operating system for memory management
    private long init = UNDEFINED;

    // the maximum amount of memory in bytes that can be used for memory management
    private long max = UNDEFINED;

    // the amount of used memory in bytes
    private long used = UNDEFINED;

    public HeapStatus(String type, long collectionCount, long collectionTime) {

      this.type = type;
      this.collectionCount = collectionCount;
      this.collectionTime = collectionTime;
    }

    public HeapStatus(String type, long committed, long init, long max, long used) {

      this.type = type;
      this.committed = committed;
      this.init = init;
      this.max = max;
      this.used = used;
    }

    @JsonProperty(HeapDimension.Constants.TYPE_VALUE)
    public String getType() {
      return type;
    }

    @JsonProperty(HeapValue.Constants.COLLECTION_COUNT_VALUE)
    public long getCollectionCount() {
      return collectionCount;
    }

    @JsonProperty(HeapValue.Constants.COLLECTION_TIME_VALUE)
    public long getCollectionTime() {
      return collectionTime;
    }

    @JsonProperty(HeapValue.Constants.COMMITTED_VALUE)
    public long getCommitted() {
      return committed;
    }

    @JsonProperty(HeapValue.Constants.INIT_VALUE)
    public long getInit() {
      return init;
    }

    @JsonProperty(HeapValue.Constants.MAX_VALUE)
    public long getMax() {
      return max;
    }

    @JsonProperty(HeapValue.Constants.USED_VALUE)
    public long getUsed() {
      return used;
    }
  }
}
