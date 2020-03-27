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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics_generator.linux;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.DiskMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.hwnet.Disks;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics_generator.DiskMetricsGenerator;
import java.util.Map;
import java.util.Set;

public class LinuxDiskMetricsGenerator implements DiskMetricsGenerator {

  private Map<String, DiskMetrics> diskMetricsMap;

  @Override
  public Set<String> getAllDisks() {
    return diskMetricsMap.keySet();
  }

  @Override
  public double getDiskUtilization(final String disk) {

    return diskMetricsMap.get(disk).utilization;
  }

  @Override
  public double getAwait(final String disk) {

    return diskMetricsMap.get(disk).await;
  }

  @Override
  public double getServiceRate(final String disk) {

    return diskMetricsMap.get(disk).serviceRate;
  }

  @Override
  public void addSample() {
    Disks.addSample();
  }

  public void setDiskMetricsMap(final Map<String, DiskMetrics> map) {

    diskMetricsMap = map;
  }
}
