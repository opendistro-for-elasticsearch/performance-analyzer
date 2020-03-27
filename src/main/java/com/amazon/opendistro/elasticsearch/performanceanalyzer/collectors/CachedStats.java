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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ShardStatsValue;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class CachedStats {
  private static final Set<String> CACHABLE_VALUES =
      new HashSet<>(
          Arrays.asList(
              ShardStatsValue.INDEXING_THROTTLE_TIME.toString(),
              ShardStatsValue.CACHE_QUERY_HIT.toString(),
              ShardStatsValue.CACHE_QUERY_MISS.toString(),
              ShardStatsValue.CACHE_FIELDDATA_EVICTION.toString(),
              ShardStatsValue.CACHE_REQUEST_HIT.toString(),
              ShardStatsValue.CACHE_REQUEST_MISS.toString(),
              ShardStatsValue.CACHE_REQUEST_EVICTION.toString(),
              ShardStatsValue.REFRESH_EVENT.toString(),
              ShardStatsValue.REFRESH_TIME.toString(),
              ShardStatsValue.FLUSH_EVENT.toString(),
              ShardStatsValue.FLUSH_TIME.toString(),
              ShardStatsValue.MERGE_EVENT.toString(),
              ShardStatsValue.MERGE_TIME.toString()));
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
