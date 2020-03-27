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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics_generator.SchedMetricsGenerator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.os.ThreadSched;
import java.util.HashMap;
import java.util.Map;

public class LinuxSchedMetricsGenerator implements SchedMetricsGenerator {

  private final Map<String, ThreadSched.SchedMetrics> schedMetricsMap;

  public LinuxSchedMetricsGenerator() {
    schedMetricsMap = new HashMap<>();
  }

  @Override
  public double getAvgRuntime(final String threadId) {

    return schedMetricsMap.get(threadId).avgRuntime;
  }

  @Override
  public double getAvgWaittime(final String threadId) {

    return schedMetricsMap.get(threadId).avgWaittime;
  }

  @Override
  public double getContextSwitchRate(final String threadId) {

    return schedMetricsMap.get(threadId).contextSwitchRate;
  }

  @Override
  public boolean hasSchedMetrics(final String threadId) {

    return schedMetricsMap.containsKey(threadId);
  }

  @Override
  public void addSample() {

    schedMetricsMap.clear();
    ThreadSched.INSTANCE.addSample();
  }

  public void setSchedMetric(final String threadId, final ThreadSched.SchedMetrics schedMetrics) {

    schedMetricsMap.put(threadId, schedMetrics);
  }
}
