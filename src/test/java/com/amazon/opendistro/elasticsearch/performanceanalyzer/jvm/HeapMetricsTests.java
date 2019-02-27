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


package com.amazon.opendistro.elasticsearch.performanceanalyzer.jvm;

import java.lang.management.MemoryUsage;
import java.util.Map;
import java.util.function.Supplier;

import org.junit.Test;

public class HeapMetricsTests {
    public static void main(String[] args) throws Exception {
        runOnce();
    }

    private static void runOnce() {
        for (Map.Entry<String, Supplier<MemoryUsage>> entry : HeapMetrics.getMemoryUsageSuppliers().entrySet()) {
            MemoryUsage memoryUsage = entry.getValue().get();
            System.out.println(entry.getKey() + "_committed:" + memoryUsage.getCommitted());
            System.out.println(entry.getKey() + "_init" + memoryUsage.getInit());
            System.out.println(entry.getKey() + "_max" + memoryUsage.getMax());
            System.out.println(entry.getKey() + "_used" + memoryUsage.getUsed());
        }
    }

    //- to enhance
    @Test
    public void testMetrics() {

    }
}
