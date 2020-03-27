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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.IPDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.IPValue;
import com.fasterxml.jackson.annotation.JsonProperty;

// all metrics are per-time-unit
public class NetInterfaceSummary extends MetricStatus {

  public enum Direction {
    in,
    out;
  }

  private Direction direction;
  private double packetRate4;
  private double dropRate4;
  private double packetRate6;
  private double dropRate6;
  private double bps;

  public NetInterfaceSummary(
      Direction direction,
      double packetRate4,
      double dropRate4,
      double packetRate6,
      double dropRate6,
      double bps) {
    this.direction = direction;
    this.packetRate4 = packetRate4;
    this.dropRate4 = dropRate4;
    this.packetRate6 = packetRate6;
    this.dropRate6 = dropRate6;
    this.bps = bps;
  }

  @JsonProperty(IPDimension.Constants.DIRECTION_VALUE)
  public Direction getDirection() {
    return direction;
  }

  @JsonProperty(IPValue.Constants.PACKET_RATE4_VALUE)
  public double getPacketRate4() {
    return packetRate4;
  }

  @JsonProperty(IPValue.Constants.DROP_RATE4_VALUE)
  public double getDropRate4() {
    return dropRate4;
  }

  @JsonProperty(IPValue.Constants.PACKET_RATE6_VALUE)
  public double getPacketRate6() {
    return packetRate6;
  }

  @JsonProperty(IPValue.Constants.DROP_RATE6_VALUE)
  public double getDropRate6() {
    return dropRate6;
  }

  @JsonProperty(IPValue.Constants.THROUGHPUT_VALUE)
  public double getBps() {
    return bps;
  }
}
