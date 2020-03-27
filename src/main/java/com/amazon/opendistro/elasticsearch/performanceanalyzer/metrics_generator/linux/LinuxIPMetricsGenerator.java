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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.NetInterfaceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.hwnet.NetworkInterface;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics_generator.IPMetricsGenerator;

public class LinuxIPMetricsGenerator implements IPMetricsGenerator {

  private NetInterfaceSummary inNetInterfaceSummary;
  private NetInterfaceSummary outNetInterfaceSummary;

  @Override
  public double getInPacketRate4() {

    return inNetInterfaceSummary.getPacketRate4();
  }

  @Override
  public double getOutPacketRate4() {

    return outNetInterfaceSummary.getPacketRate4();
  }

  @Override
  public double getInDropRate4() {

    return inNetInterfaceSummary.getDropRate4();
  }

  @Override
  public double getOutDropRate4() {

    return outNetInterfaceSummary.getDropRate4();
  }

  @Override
  public double getInPacketRate6() {

    return inNetInterfaceSummary.getPacketRate6();
  }

  @Override
  public double getOutPacketRate6() {

    return outNetInterfaceSummary.getPacketRate6();
  }

  @Override
  public double getInDropRate6() {

    return inNetInterfaceSummary.getDropRate6();
  }

  @Override
  public double getOutDropRate6() {

    return outNetInterfaceSummary.getDropRate6();
  }

  @Override
  public double getInBps() {

    return inNetInterfaceSummary.getBps();
  }

  @Override
  public double getOutBps() {

    return outNetInterfaceSummary.getBps();
  }

  @Override
  public void addSample() {

    NetworkInterface.addSample();
  }

  public void setInNetworkInterfaceSummary(final NetInterfaceSummary netInterfaceSummary) {

    this.inNetInterfaceSummary = netInterfaceSummary;
  }

  public void setOutNetworkInterfaceSummary(final NetInterfaceSummary netInterfaceSummary) {

    this.outNetInterfaceSummary = netInterfaceSummary;
  }
}
