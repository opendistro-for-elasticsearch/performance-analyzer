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

package com.amazon.opendistro.performanceanalyzer.collectors;


// all metrics are per-time-unit
public class NetInterfaceSummary extends MetricStatus {

    public enum Direction {
        in, out;
    }

    Direction direction;
    public double packetRate4;
    public double dropRate4;
    public double packetRate6;
    public double dropRate6;
    public double bps;

    public NetInterfaceSummary(Direction direction,
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

    public Direction getDirection() {
        return direction;
    }

    public double getPacketRate4() {
        return packetRate4;
    }

    public double getDropRate4() {
        return dropRate4;
    }

    public double getPacketRate6() {
        return packetRate6;
    }

    public double getDropRate6() {
        return dropRate6;
    }

    public double getBps() {
        return bps;
    }
}