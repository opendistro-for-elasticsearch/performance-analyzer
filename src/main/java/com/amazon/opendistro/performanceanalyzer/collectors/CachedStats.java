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

import java.util.Arrays;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;

import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics;

class CachedStats {
    private static final Set<String> CACHABLE_VALUES = new HashSet<>(Arrays.asList(
            AllMetrics.ShardStatsValue.indexingThrottleTime.name(),
            AllMetrics.ShardStatsValue.queryCacheHitCount.name(),
            AllMetrics.ShardStatsValue.queryCacheMissCount.name(),
            AllMetrics.ShardStatsValue.fieldDataEvictions.name(),
            AllMetrics.ShardStatsValue.requestCacheHitCount.name(),
            AllMetrics.ShardStatsValue.requestCacheMissCount.name(),
            AllMetrics.ShardStatsValue.requestCacheEvictions.name(),
            AllMetrics.ShardStatsValue.refreshCount.name(),
            AllMetrics.ShardStatsValue.refreshTime.name(),
            AllMetrics.ShardStatsValue.flushCount.name(),
            AllMetrics.ShardStatsValue.flushTime.name(),
            AllMetrics.ShardStatsValue.mergeCount.name(),
            AllMetrics.ShardStatsValue.mergeTime.name()
    ));
    private Map<String, Long> cachedValues = new HashMap<>();

    long getValue(String statsName) {
        return cachedValues.getOrDefault(statsName, 0L);
    }

    void putValue(String statsName, long value) {
        cachedValues.put(statsName, value);
    }

    static Set<String> getCachableValues() {
        return CACHABLE_VALUES;
    }
}
