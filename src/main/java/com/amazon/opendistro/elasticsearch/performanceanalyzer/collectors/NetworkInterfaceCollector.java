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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics_generator.IPMetricsGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NetworkInterfaceCollector extends PerformanceAnalyzerMetricsCollector
    implements MetricsProcessor {
  private static final int sTimeInterval =
      MetricsConfiguration.CONFIG_MAP.get(NetworkInterfaceCollector.class).samplingInterval;
  private static final Logger LOG = LogManager.getLogger(NetworkInterfaceCollector.class);
  private StringBuilder ret = new StringBuilder();

  public NetworkInterfaceCollector() {
    super(sTimeInterval, "NetworkInterfaceCollector");
  }

  @Override
  public void collectMetrics(long startTime) {

    IPMetricsGenerator IPMetricsGenerator =
        OSMetricsGeneratorFactory.getInstance().getIPMetricsGenerator();
    IPMetricsGenerator.addSample();

    saveMetricValues(
        getMetrics(IPMetricsGenerator) + PerformanceAnalyzerMetrics.sMetricNewLineDelimitor,
        startTime);
  }

  @Override
  public String getMetricsPath(long startTime, String... keysPath) {
    // throw exception if keys.length is not equal to 0
    if (keysPath.length != 0) {
      throw new RuntimeException("keys length should be 0");
    }

    return PerformanceAnalyzerMetrics.generatePath(startTime, PerformanceAnalyzerMetrics.sIPPath);
  }

  private String getMetrics(IPMetricsGenerator IPMetricsGenerator) {

    ret.setLength(0);
    ret.append(PerformanceAnalyzerMetrics.getJsonCurrentMilliSeconds())
        .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor);

    try {
      NetInterfaceSummary inNetwork =
          new NetInterfaceSummary(
              NetInterfaceSummary.Direction.in,
              IPMetricsGenerator.getInPacketRate4(),
              IPMetricsGenerator.getInDropRate4(),
              IPMetricsGenerator.getInPacketRate6(),
              IPMetricsGenerator.getInDropRate6(),
              IPMetricsGenerator.getInBps());

      NetInterfaceSummary outNetwork =
          new NetInterfaceSummary(
              NetInterfaceSummary.Direction.out,
              IPMetricsGenerator.getOutPacketRate4(),
              IPMetricsGenerator.getOutDropRate4(),
              IPMetricsGenerator.getOutPacketRate6(),
              IPMetricsGenerator.getOutDropRate6(),
              IPMetricsGenerator.getOutBps());

      ret.append(inNetwork.serialize()).append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor);
      ret.append(outNetwork.serialize()).append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor);
    } catch (Exception e) {
      LOG.debug(
          "Exception in NetworkInterfaceCollector: {} with ExceptionCode: {}",
          () -> e.toString(),
          () -> StatExceptionCode.NETWORK_COLLECTION_ERROR.toString());
      StatsCollector.instance().logException(StatExceptionCode.NETWORK_COLLECTION_ERROR);
    }

    return ret.toString();
  }
}
