/*
 * Copyright <2019> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.performanceanalyzer.metrics_generator.linux;

import com.amazon.opendistro.performanceanalyzer.collectors.NetInterfaceSummary;
import com.amazon.opendistro.performanceanalyzer.hwnet.NetworkInterface;
import com.amazon.opendistro.performanceanalyzer.metrics_generator.IPMetricsGenerator;

public class LinuxIPMetricsGenerator implements IPMetricsGenerator {


    private NetInterfaceSummary inNetInterfaceSummary;
    private NetInterfaceSummary outNetInterfaceSummary;

    @Override
    public double getInPacketRate4() {

        return inNetInterfaceSummary.packetRate4;
    }

    @Override
    public double getOutPacketRate4() {

        return outNetInterfaceSummary.packetRate4;
    }

    @Override
    public double getInDropRate4() {

        return inNetInterfaceSummary.dropRate4;
    }

    @Override
    public double getOutDropRate4() {

        return outNetInterfaceSummary.dropRate4;
    }

    @Override
    public double getInPacketRate6() {

        return inNetInterfaceSummary.packetRate6;
    }

    @Override
    public double getOutPacketRate6() {

        return outNetInterfaceSummary.packetRate6;
    }

    @Override
    public double getInDropRate6() {

        return inNetInterfaceSummary.dropRate6;
    }

    @Override
    public double getOutDropRate6() {

        return outNetInterfaceSummary.dropRate6;
    }

    @Override
    public double getInBps() {

        return inNetInterfaceSummary.bps;
    }

    @Override
    public double getOutBps() {

        return outNetInterfaceSummary.bps;
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
