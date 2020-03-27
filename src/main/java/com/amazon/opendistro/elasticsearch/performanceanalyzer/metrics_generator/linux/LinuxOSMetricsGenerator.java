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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.hwnet.Disks;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.hwnet.NetworkE2E;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.hwnet.NetworkInterface;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics_generator.CPUPagingActivityGenerator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics_generator.DiskIOMetricsGenerator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics_generator.DiskMetricsGenerator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics_generator.IPMetricsGenerator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics_generator.OSMetricsGenerator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics_generator.SchedMetricsGenerator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics_generator.TCPMetricsGenerator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.os.OSGlobals;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.os.ThreadCPU;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.os.ThreadDiskIO;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.os.ThreadSched;
import java.util.Set;

public class LinuxOSMetricsGenerator implements OSMetricsGenerator {

  private static OSMetricsGenerator osMetricsGenerator;

  static {
    osMetricsGenerator = new LinuxOSMetricsGenerator();
  }

  public static OSMetricsGenerator getInstance() {

    return osMetricsGenerator;
  }

  @Override
  public String getPid() {

    return OSGlobals.getPid();
  }

  @Override
  public CPUPagingActivityGenerator getPagingActivityGenerator() {

    return ThreadCPU.INSTANCE.getCPUPagingActivity();
  }

  @Override
  public Set<String> getAllThreadIds() {
    return ThreadCPU.INSTANCE.getCPUPagingActivity().getAllThreadIds();
  }

  @Override
  public DiskIOMetricsGenerator getDiskIOMetricsGenerator() {

    return ThreadDiskIO.getIOUtilization();
  }

  @Override
  public SchedMetricsGenerator getSchedMetricsGenerator() {

    return ThreadSched.INSTANCE.getSchedLatency();
  }

  @Override
  public TCPMetricsGenerator getTCPMetricsGenerator() {

    return NetworkE2E.getTCPMetricsHandler();
  }

  @Override
  public IPMetricsGenerator getIPMetricsGenerator() {

    return NetworkInterface.getLinuxIPMetricsGenerator();
  }

  @Override
  public DiskMetricsGenerator getDiskMetricsGenerator() {

    return Disks.getDiskMetricsHandler();
  }
}
