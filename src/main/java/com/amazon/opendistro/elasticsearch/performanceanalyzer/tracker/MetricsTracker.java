/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.tracker;

/**
 *  Class to track previous latency and failure metrics to calculate point in time metrics. These values are updated
 *  everytime collector calls respective stats API.
 */
public class MetricsTracker {
    private double prevTimeTakenInMillis;
    private double prevFailedCount;
    private double prevTotalCount;

    public MetricsTracker(double timeInMillis, double failedCount, double totalCount) {
        this.prevTimeTakenInMillis = timeInMillis;
        this.prevFailedCount = failedCount;
        this.prevTotalCount = totalCount;
    }

    public MetricsTracker() {
    }

    public double getPrevTimeTakenInMillis() {
        return prevTimeTakenInMillis;
    }

    public double getPrevFailedCount() {
        return prevFailedCount;
    }

    public double getPrevTotalCount() {
        return prevTotalCount;
    }
}
