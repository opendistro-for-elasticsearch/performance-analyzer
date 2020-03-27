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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics_generator;

public interface DiskIOMetricsGenerator {

  // This method will be called before all following get methods
  // to make sure that all information exists for a thread id
  boolean hasDiskIOMetrics(String threadId);

  // these metrics include page cache activity;
  // only explicit syscalls: NO mmaps (majflts include mmaps)
  double getAvgReadThroughputBps(String threadId);

  double getAvgWriteThroughputBps(String threadId);

  double getAvgTotalThroughputBps(String threadId);

  double getAvgReadSyscallRate(String threadId);

  double getAvgWriteSyscallRate(String threadId);

  double getAvgTotalSyscallRate(String threadId);

  void addSample();
}
