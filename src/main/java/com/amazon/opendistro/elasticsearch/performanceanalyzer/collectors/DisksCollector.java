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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.OSMetricsGeneratorFactory;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics_generator.DiskMetricsGenerator;
import java.util.HashMap;
import java.util.Map;

public class DisksCollector extends PerformanceAnalyzerMetricsCollector
    implements MetricsProcessor {

  private static final int sTimeInterval =
      MetricsConfiguration.CONFIG_MAP.get(DisksCollector.class).samplingInterval;
  private StringBuilder value = new StringBuilder();

  public DisksCollector() {
    super(sTimeInterval, "DisksCollector");
  }

  @Override
  public String getMetricsPath(long startTime, String... keysPath) {
    // throw exception if keys.length is not equal to 0
    if (keysPath.length != 0) {
      throw new RuntimeException("keys length should be 0");
    }

    return PerformanceAnalyzerMetrics.generatePath(
        startTime, PerformanceAnalyzerMetrics.sDisksPath);
  }

  @Override
  public void collectMetrics(long startTime) {
    DiskMetricsGenerator diskMetricsGenerator =
        OSMetricsGeneratorFactory.getInstance().getDiskMetricsGenerator();
    diskMetricsGenerator.addSample();

    saveMetricValues(getMetrics(diskMetricsGenerator), startTime);
  }

  private Map<String, DiskMetrics> getMetricsMap(DiskMetricsGenerator diskMetricsGenerator) {

    Map<String, DiskMetrics> map = new HashMap<>();

    for (String disk : diskMetricsGenerator.getAllDisks()) {
      DiskMetrics diskMetrics = new DiskMetrics();
      diskMetrics.name = disk;
      diskMetrics.await = diskMetricsGenerator.getAwait(disk);
      diskMetrics.serviceRate = diskMetricsGenerator.getServiceRate(disk);
      diskMetrics.utilization = diskMetricsGenerator.getDiskUtilization(disk);

      map.put(disk, diskMetrics);
    }

    return map;
  }

  private String getMetrics(DiskMetricsGenerator diskMetricsGenerator) {

    Map<String, DiskMetrics> map = getMetricsMap(diskMetricsGenerator);
    value.setLength(0);
    value
        .append(PerformanceAnalyzerMetrics.getJsonCurrentMilliSeconds())
        .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor);
    for (Map.Entry<String, DiskMetrics> entry : map.entrySet()) {
      value
          .append(entry.getValue().serialize())
          .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor);
    }
    return value.toString();
  }
}
