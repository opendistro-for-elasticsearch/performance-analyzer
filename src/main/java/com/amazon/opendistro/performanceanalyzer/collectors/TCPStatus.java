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

import com.fasterxml.jackson.annotation.JsonProperty;

public class TCPStatus extends MetricStatus {

    public String dest;

    public int numFlows;

    public double txQ;

    public double rxQ;

    public double curLost;

    public double sndCWND;

    public double SSThresh;

    public TCPStatus(String dest, int numFlows, double txQ, double rxQ,
                     double curLost, double sndCWND, double sSThresh) {
        super();
        this.dest = dest;
        this.numFlows = numFlows;
        this.txQ = txQ;
        this.rxQ = rxQ;
        this.curLost = curLost;
        this.sndCWND = sndCWND;
        this.SSThresh = sSThresh;
    }

    @JsonProperty("dest")
    public String getDest() {
        return dest;
    }

    @JsonProperty("numFlows")
    public int getNumFlows() {
        return numFlows;
    }

    @JsonProperty("txQ")
    public double getTxQ() {
        return txQ;
    }

    @JsonProperty("rxQ")
    public double getRxQ() {
        return rxQ;
    }

    @JsonProperty("curLost")
    public double getCurLost() {
        return curLost;
    }

    @JsonProperty("sndCWND")
    public double getSndCWND() {
        return sndCWND;
    }

    @JsonProperty("SSThresh")
    public double getSSThresh() {
        return SSThresh;
    }
}
