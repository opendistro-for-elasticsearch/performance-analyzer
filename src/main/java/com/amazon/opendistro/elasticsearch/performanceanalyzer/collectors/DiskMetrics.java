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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.DiskDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.DiskValue;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DiskMetrics extends MetricStatus {
  public String name;

  public double utilization; // fraction, 0-1

  public double await; // ms

  public double serviceRate; // MBps

  public DiskMetrics(String name, double utilization, double await, double serviceRate) {
    super();
    this.name = name;
    this.utilization = utilization;
    this.await = await;
    this.serviceRate = serviceRate;
  }

  public DiskMetrics() {
    super();
  }

  @JsonProperty(DiskDimension.Constants.NAME_VALUE)
  public String getName() {
    return name;
  }

  @JsonProperty(DiskValue.Constants.UTIL_VALUE)
  public double getUtilization() {
    return utilization;
  }

  @JsonProperty(DiskValue.Constants.WAIT_VALUE)
  public double getAwait() {
    return await;
  }

  @JsonProperty(DiskValue.Constants.SRATE_VALUE)
  public double getServiceRate() {
    return serviceRate;
  }
}
