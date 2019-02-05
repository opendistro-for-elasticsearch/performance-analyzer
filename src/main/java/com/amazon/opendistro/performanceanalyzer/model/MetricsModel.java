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

package com.amazon.opendistro.performanceanalyzer.model;

import java.util.HashMap;
import java.util.Map;

import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.AggregatedOSDimension;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.CircuitBreakerDimension;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.DiskDimension;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.EmptyDimension;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.HeapDimension;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.HttpOnlyDimension;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.IPDimension;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.LatencyDimension;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.ShardStatsDerivedDimension;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.TCPDimension;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.ThreadPoolDimension;

public class MetricsModel {

    public static final Map<String, MetricAttributes> ALL_METRICS = new HashMap<>();

    static {
        // OS Metrics
        ALL_METRICS.put("cpu", new MetricAttributes("cores", AggregatedOSDimension.values()));
        ALL_METRICS.put("paging_majflt", new MetricAttributes("count", AggregatedOSDimension.values()));
        ALL_METRICS.put("paging_minflt", new MetricAttributes("count", AggregatedOSDimension.values()));
        ALL_METRICS.put("rss", new MetricAttributes("count", AggregatedOSDimension.values()));
        ALL_METRICS.put("runtime", new MetricAttributes("s", AggregatedOSDimension.values()));
        ALL_METRICS.put("waittime", new MetricAttributes("s", AggregatedOSDimension.values()));
        ALL_METRICS.put("ctxrate", new MetricAttributes("count", AggregatedOSDimension.values()));
        ALL_METRICS.put("heap_usage", new MetricAttributes("bps", AggregatedOSDimension.values()));
        ALL_METRICS.put("avgReadThroughputBps", new MetricAttributes("bps", AggregatedOSDimension.values()));
        ALL_METRICS.put("avgWriteThroughputBps", new MetricAttributes("bps", AggregatedOSDimension.values()));
        ALL_METRICS.put("avgTotalThroughputBps", new MetricAttributes("bps", AggregatedOSDimension.values()));
        ALL_METRICS.put("avgReadSyscallRate", new MetricAttributes("sread/s", AggregatedOSDimension.values()));
        ALL_METRICS.put("avgWriteSyscallRate", new MetricAttributes("swrite/s", AggregatedOSDimension.values()));
        ALL_METRICS.put("avgTotalSyscallRate", new MetricAttributes("scall/s", AggregatedOSDimension.values()));
        ALL_METRICS.put("avgBlockedTime", new MetricAttributes("s", AggregatedOSDimension.values()));
        ALL_METRICS.put("blockedCount", new MetricAttributes("count", AggregatedOSDimension.values()));

        // Elasticsearch Metrics
        // todo: "latency" needs a dimension enum.
        ALL_METRICS.put("latency", new MetricAttributes("ms", LatencyDimension.values()));
        ALL_METRICS.put("itemCount", new MetricAttributes("count", HttpOnlyDimension.values()));
        ALL_METRICS.put("count", new MetricAttributes("count", HttpOnlyDimension.values()));

        // Circuit Breaker
        ALL_METRICS.put("estimated", new MetricAttributes("b", CircuitBreakerDimension.values()));
        ALL_METRICS.put("limitConfigured", new MetricAttributes("b", CircuitBreakerDimension.values()));
        ALL_METRICS.put("tripped", new MetricAttributes("count", CircuitBreakerDimension.values()));

        // Heap Metrics
        ALL_METRICS.put("collectionCount", new MetricAttributes("count", HeapDimension.values()));
        ALL_METRICS.put("collectionTime", new MetricAttributes("ms", HeapDimension.values()));
        ALL_METRICS.put("committed", new MetricAttributes("b", HeapDimension.values()));
        ALL_METRICS.put("init", new MetricAttributes("b", HeapDimension.values()));
        ALL_METRICS.put("max", new MetricAttributes("b", HeapDimension.values()));
        ALL_METRICS.put("used", new MetricAttributes("b", HeapDimension.values()));

        // Disk Metrics
        ALL_METRICS.put("srate", new MetricAttributes("mb/s", DiskDimension.values()));
        ALL_METRICS.put("util", new MetricAttributes("%", DiskDimension.values()));
        ALL_METRICS.put("wait", new MetricAttributes("ms", DiskDimension.values()));

        // TCP Metrics
        ALL_METRICS.put("curLost", new MetricAttributes("count", TCPDimension.values()));
        ALL_METRICS.put("numFlows", new MetricAttributes("count", TCPDimension.values()));
        ALL_METRICS.put("rxQ", new MetricAttributes("count", TCPDimension.values()));
        ALL_METRICS.put("sndCWND", new MetricAttributes("b", TCPDimension.values()));
        ALL_METRICS.put("SSThresh", new MetricAttributes("b", TCPDimension.values()));
        ALL_METRICS.put("txQ", new MetricAttributes("count", TCPDimension.values()));

        // IP Metrics
        ALL_METRICS.put("bps", new MetricAttributes("bps", IPDimension.values()));
        ALL_METRICS.put("dropRate4", new MetricAttributes("count", IPDimension.values()));
        ALL_METRICS.put("dropRate6", new MetricAttributes("count", IPDimension.values()));
        ALL_METRICS.put("packetRate4", new MetricAttributes("count", IPDimension.values()));
        ALL_METRICS.put("packetRate6", new MetricAttributes("count", IPDimension.values()));

        // Thread Pool Metrics
        ALL_METRICS.put("queueSize", new MetricAttributes("b", ThreadPoolDimension.values()));
        ALL_METRICS.put("rejected", new MetricAttributes("count", ThreadPoolDimension.values()));
        ALL_METRICS.put("threadsActive", new MetricAttributes("count", ThreadPoolDimension.values()));
        ALL_METRICS.put("threadsCount", new MetricAttributes("count", ThreadPoolDimension.values()));

        // Shard Stats Metrics
        ALL_METRICS.put("indexingThrottleTime", new MetricAttributes("ms", ShardStatsDerivedDimension.values()));
        ALL_METRICS.put("queryCacheHitCount", new MetricAttributes("count", ShardStatsDerivedDimension.values()));
        ALL_METRICS.put("queryCacheMissCount", new MetricAttributes("count", ShardStatsDerivedDimension.values()));
        ALL_METRICS.put("queryCacheInBytes", new MetricAttributes("b", ShardStatsDerivedDimension.values()));
        ALL_METRICS.put("fieldDataEvictions", new MetricAttributes("count", ShardStatsDerivedDimension.values()));
        ALL_METRICS.put("fieldDataInBytes", new MetricAttributes("b", ShardStatsDerivedDimension.values()));
        ALL_METRICS.put("requestCacheHitCount", new MetricAttributes("count", ShardStatsDerivedDimension.values()));
        ALL_METRICS.put("requestCacheMissCount", new MetricAttributes("count", ShardStatsDerivedDimension.values()));
        ALL_METRICS.put("requestCacheEvictions", new MetricAttributes("count", ShardStatsDerivedDimension.values()));
        ALL_METRICS.put("requestCacheInBytes", new MetricAttributes("b", ShardStatsDerivedDimension.values()));
        ALL_METRICS.put("refreshCount", new MetricAttributes("count", ShardStatsDerivedDimension.values()));
        ALL_METRICS.put("refreshTime", new MetricAttributes("ms", ShardStatsDerivedDimension.values()));
        ALL_METRICS.put("flushCount", new MetricAttributes("count", ShardStatsDerivedDimension.values()));
        ALL_METRICS.put("flushTime", new MetricAttributes("ms", ShardStatsDerivedDimension.values()));
        ALL_METRICS.put("mergeCount", new MetricAttributes("count", ShardStatsDerivedDimension.values()));
        ALL_METRICS.put("mergeTime", new MetricAttributes("ms", ShardStatsDerivedDimension.values()));
        ALL_METRICS.put("mergeCurrent", new MetricAttributes("count", ShardStatsDerivedDimension.values()));
        ALL_METRICS.put("indexBufferBytes", new MetricAttributes("b", ShardStatsDerivedDimension.values()));
        ALL_METRICS.put("segmentCount", new MetricAttributes("count", ShardStatsDerivedDimension.values()));
        ALL_METRICS.put("segmentsMemory", new MetricAttributes("b", ShardStatsDerivedDimension.values()));
        ALL_METRICS.put("termsMemory", new MetricAttributes("b", ShardStatsDerivedDimension.values()));
        ALL_METRICS.put("storedFieldsMemory", new MetricAttributes("b", ShardStatsDerivedDimension.values()));
        ALL_METRICS.put("termVectorsMemory", new MetricAttributes("b", ShardStatsDerivedDimension.values()));
        ALL_METRICS.put("normsMemory", new MetricAttributes("b", ShardStatsDerivedDimension.values()));
        ALL_METRICS.put("pointsMemory", new MetricAttributes("b", ShardStatsDerivedDimension.values()));
        ALL_METRICS.put("docValuesMemory", new MetricAttributes("b", ShardStatsDerivedDimension.values()));
        ALL_METRICS.put("indexWriterMemory", new MetricAttributes("b", ShardStatsDerivedDimension.values()));
        ALL_METRICS.put("versionMapMemory", new MetricAttributes("b", ShardStatsDerivedDimension.values()));

        // Master Metrics
        ALL_METRICS.put("pendingTasksCount", new MetricAttributes("count", EmptyDimension.values()));
    }

}
