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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.model;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.AggregatedOSDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CircuitBreakerDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CircuitBreakerValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CommonMetric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.DiskDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.DiskValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.EmptyDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.HeapDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.HeapValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.HttpMetric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.HttpOnlyDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.IPDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.IPValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.LatencyDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.MasterPendingValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.MetricUnits;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.OSMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ShardBulkMetric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ShardOperationMetric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ShardStatsDerivedDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ShardStatsValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.TCPDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.TCPValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ThreadPoolDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ThreadPoolValue;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MetricsModel {

  public static final Map<String, MetricAttributes> ALL_METRICS;

  static {
    Map<String, MetricAttributes> allMetricsInitializer = new HashMap<>();
    // OS Metrics
    allMetricsInitializer.put(
        OSMetrics.CPU_UTILIZATION.toString(),
        new MetricAttributes(MetricUnits.CORES.toString(), AggregatedOSDimension.values()));
    allMetricsInitializer.put(
        OSMetrics.PAGING_MAJ_FLT_RATE.toString(),
        new MetricAttributes(MetricUnits.COUNT_PER_SEC.toString(), AggregatedOSDimension.values()));
    allMetricsInitializer.put(
        OSMetrics.PAGING_MIN_FLT_RATE.toString(),
        new MetricAttributes(MetricUnits.COUNT_PER_SEC.toString(), AggregatedOSDimension.values()));
    allMetricsInitializer.put(
        OSMetrics.PAGING_RSS.toString(),
        new MetricAttributes(MetricUnits.PAGES.toString(), AggregatedOSDimension.values()));
    allMetricsInitializer.put(
        OSMetrics.SCHED_RUNTIME.toString(),
        new MetricAttributes(
            MetricUnits.SEC_PER_CONTEXT_SWITCH.toString(), AggregatedOSDimension.values()));
    allMetricsInitializer.put(
        OSMetrics.SCHED_WAITTIME.toString(),
        new MetricAttributes(
            MetricUnits.SEC_PER_CONTEXT_SWITCH.toString(), AggregatedOSDimension.values()));
    allMetricsInitializer.put(
        OSMetrics.SCHED_CTX_RATE.toString(),
        new MetricAttributes(MetricUnits.COUNT_PER_SEC.toString(), AggregatedOSDimension.values()));
    allMetricsInitializer.put(
        OSMetrics.HEAP_ALLOC_RATE.toString(),
        new MetricAttributes(MetricUnits.BYTE_PER_SEC.toString(), AggregatedOSDimension.values()));
    allMetricsInitializer.put(
        OSMetrics.IO_READ_THROUGHPUT.toString(),
        new MetricAttributes(MetricUnits.BYTE_PER_SEC.toString(), AggregatedOSDimension.values()));
    allMetricsInitializer.put(
        OSMetrics.IO_WRITE_THROUGHPUT.toString(),
        new MetricAttributes(MetricUnits.BYTE_PER_SEC.toString(), AggregatedOSDimension.values()));
    allMetricsInitializer.put(
        OSMetrics.IO_TOT_THROUGHPUT.toString(),
        new MetricAttributes(MetricUnits.BYTE_PER_SEC.toString(), AggregatedOSDimension.values()));
    allMetricsInitializer.put(
        OSMetrics.IO_READ_SYSCALL_RATE.toString(),
        new MetricAttributes(MetricUnits.COUNT_PER_SEC.toString(), AggregatedOSDimension.values()));
    allMetricsInitializer.put(
        OSMetrics.IO_WRITE_SYSCALL_RATE.toString(),
        new MetricAttributes(MetricUnits.COUNT_PER_SEC.toString(), AggregatedOSDimension.values()));
    allMetricsInitializer.put(
        OSMetrics.IO_TOTAL_SYSCALL_RATE.toString(),
        new MetricAttributes(MetricUnits.COUNT_PER_SEC.toString(), AggregatedOSDimension.values()));
    allMetricsInitializer.put(
        OSMetrics.THREAD_BLOCKED_TIME.toString(),
        new MetricAttributes(MetricUnits.SEC_PER_EVENT.toString(), AggregatedOSDimension.values()));
    allMetricsInitializer.put(
        OSMetrics.THREAD_BLOCKED_EVENT.toString(),
        new MetricAttributes(MetricUnits.COUNT.toString(), AggregatedOSDimension.values()));

    // Latency Metric
    allMetricsInitializer.put(
        CommonMetric.LATENCY.toString(),
        new MetricAttributes(MetricUnits.MILLISECOND.toString(), LatencyDimension.values()));

    allMetricsInitializer.put(
        ShardOperationMetric.SHARD_OP_COUNT.toString(),
        new MetricAttributes(MetricUnits.COUNT.toString(), AggregatedOSDimension.values()));
    allMetricsInitializer.put(
        ShardBulkMetric.DOC_COUNT.toString(),
        new MetricAttributes(MetricUnits.COUNT.toString(), AggregatedOSDimension.values()));

    // HTTP Metrics
    allMetricsInitializer.put(
        HttpMetric.HTTP_REQUEST_DOCS.toString(),
        new MetricAttributes(MetricUnits.COUNT.toString(), HttpOnlyDimension.values()));
    allMetricsInitializer.put(
        HttpMetric.HTTP_TOTAL_REQUESTS.toString(),
        new MetricAttributes(MetricUnits.COUNT.toString(), HttpOnlyDimension.values()));

    // Circuit Breaker Metrics
    allMetricsInitializer.put(
        CircuitBreakerValue.CB_ESTIMATED_SIZE.toString(),
        new MetricAttributes(MetricUnits.BYTE.toString(), CircuitBreakerDimension.values()));
    allMetricsInitializer.put(
        CircuitBreakerValue.CB_CONFIGURED_SIZE.toString(),
        new MetricAttributes(MetricUnits.BYTE.toString(), CircuitBreakerDimension.values()));
    allMetricsInitializer.put(
        CircuitBreakerValue.CB_TRIPPED_EVENTS.toString(),
        new MetricAttributes(MetricUnits.COUNT.toString(), CircuitBreakerDimension.values()));

    // Heap Metrics
    allMetricsInitializer.put(
        HeapValue.GC_COLLECTION_EVENT.toString(),
        new MetricAttributes(MetricUnits.COUNT.toString(), HeapDimension.values()));
    allMetricsInitializer.put(
        HeapValue.GC_COLLECTION_TIME.toString(),
        new MetricAttributes(MetricUnits.MILLISECOND.toString(), HeapDimension.values()));
    allMetricsInitializer.put(
        HeapValue.HEAP_COMMITTED.toString(),
        new MetricAttributes(MetricUnits.BYTE.toString(), HeapDimension.values()));
    allMetricsInitializer.put(
        HeapValue.HEAP_INIT.toString(),
        new MetricAttributes(MetricUnits.BYTE.toString(), HeapDimension.values()));
    allMetricsInitializer.put(
        HeapValue.HEAP_MAX.toString(),
        new MetricAttributes(MetricUnits.BYTE.toString(), HeapDimension.values()));
    allMetricsInitializer.put(
        HeapValue.HEAP_USED.toString(),
        new MetricAttributes(MetricUnits.BYTE.toString(), HeapDimension.values()));

    // Disk Metrics
    allMetricsInitializer.put(
        DiskValue.DISK_UTILIZATION.toString(),
        new MetricAttributes(MetricUnits.PERCENT.toString(), DiskDimension.values()));
    allMetricsInitializer.put(
        DiskValue.DISK_WAITTIME.toString(),
        new MetricAttributes(MetricUnits.MILLISECOND.toString(), DiskDimension.values()));
    allMetricsInitializer.put(
        DiskValue.DISK_SERVICE_RATE.toString(),
        new MetricAttributes(MetricUnits.MEGABYTE_PER_SEC.toString(), DiskDimension.values()));

    // TCP Metrics
    allMetricsInitializer.put(
        TCPValue.Net_TCP_NUM_FLOWS.toString(),
        new MetricAttributes(MetricUnits.COUNT.toString(), TCPDimension.values()));
    allMetricsInitializer.put(
        TCPValue.Net_TCP_TXQ.toString(),
        new MetricAttributes(MetricUnits.SEGMENT_PER_FLOW.toString(), TCPDimension.values()));
    allMetricsInitializer.put(
        TCPValue.Net_TCP_RXQ.toString(),
        new MetricAttributes(MetricUnits.SEGMENT_PER_FLOW.toString(), TCPDimension.values()));
    allMetricsInitializer.put(
        TCPValue.Net_TCP_LOST.toString(),
        new MetricAttributes(MetricUnits.SEGMENT_PER_FLOW.toString(), TCPDimension.values()));
    allMetricsInitializer.put(
        TCPValue.Net_TCP_SEND_CWND.toString(),
        new MetricAttributes(MetricUnits.BYTE_PER_FLOW.toString(), TCPDimension.values()));
    allMetricsInitializer.put(
        TCPValue.Net_TCP_SSTHRESH.toString(),
        new MetricAttributes(MetricUnits.BYTE_PER_FLOW.toString(), TCPDimension.values()));

    // IP Metrics
    allMetricsInitializer.put(
        IPValue.NET_PACKET_RATE4.toString(),
        new MetricAttributes(MetricUnits.PACKET_PER_SEC.toString(), IPDimension.values()));
    allMetricsInitializer.put(
        IPValue.NET_PACKET_DROP_RATE4.toString(),
        new MetricAttributes(MetricUnits.PACKET_PER_SEC.toString(), IPDimension.values()));
    allMetricsInitializer.put(
        IPValue.NET_PACKET_RATE6.toString(),
        new MetricAttributes(MetricUnits.PACKET_PER_SEC.toString(), IPDimension.values()));
    allMetricsInitializer.put(
        IPValue.NET_PACKET_DROP_RATE6.toString(),
        new MetricAttributes(MetricUnits.PACKET_PER_SEC.toString(), IPDimension.values()));
    allMetricsInitializer.put(
        IPValue.NET_THROUGHPUT.toString(),
        new MetricAttributes(MetricUnits.BYTE_PER_SEC.toString(), IPDimension.values()));

    // Thread Pool Metrics
    allMetricsInitializer.put(
        ThreadPoolValue.THREADPOOL_QUEUE_SIZE.toString(),
        new MetricAttributes(MetricUnits.COUNT.toString(), ThreadPoolDimension.values()));
    allMetricsInitializer.put(
        ThreadPoolValue.THREADPOOL_REJECTED_REQS.toString(),
        new MetricAttributes(MetricUnits.COUNT.toString(), ThreadPoolDimension.values()));
    allMetricsInitializer.put(
        ThreadPoolValue.THREADPOOL_TOTAL_THREADS.toString(),
        new MetricAttributes(MetricUnits.COUNT.toString(), ThreadPoolDimension.values()));
    allMetricsInitializer.put(
        ThreadPoolValue.THREADPOOL_ACTIVE_THREADS.toString(),
        new MetricAttributes(MetricUnits.COUNT.toString(), ThreadPoolDimension.values()));

    // Shard Stats Metrics
    allMetricsInitializer.put(
        ShardStatsValue.INDEXING_THROTTLE_TIME.toString(),
        new MetricAttributes(
            MetricUnits.MILLISECOND.toString(), ShardStatsDerivedDimension.values()));
    allMetricsInitializer.put(
        ShardStatsValue.CACHE_QUERY_HIT.toString(),
        new MetricAttributes(MetricUnits.COUNT.toString(), ShardStatsDerivedDimension.values()));
    allMetricsInitializer.put(
        ShardStatsValue.CACHE_QUERY_MISS.toString(),
        new MetricAttributes(MetricUnits.COUNT.toString(), ShardStatsDerivedDimension.values()));
    allMetricsInitializer.put(
        ShardStatsValue.CACHE_QUERY_SIZE.toString(),
        new MetricAttributes(MetricUnits.BYTE.toString(), ShardStatsDerivedDimension.values()));
    allMetricsInitializer.put(
        ShardStatsValue.CACHE_FIELDDATA_EVICTION.toString(),
        new MetricAttributes(MetricUnits.COUNT.toString(), ShardStatsDerivedDimension.values()));
    allMetricsInitializer.put(
        ShardStatsValue.CACHE_FIELDDATA_SIZE.toString(),
        new MetricAttributes(MetricUnits.BYTE.toString(), ShardStatsDerivedDimension.values()));
    allMetricsInitializer.put(
        ShardStatsValue.CACHE_REQUEST_HIT.toString(),
        new MetricAttributes(MetricUnits.COUNT.toString(), ShardStatsDerivedDimension.values()));
    allMetricsInitializer.put(
        ShardStatsValue.CACHE_REQUEST_MISS.toString(),
        new MetricAttributes(MetricUnits.COUNT.toString(), ShardStatsDerivedDimension.values()));
    allMetricsInitializer.put(
        ShardStatsValue.CACHE_REQUEST_EVICTION.toString(),
        new MetricAttributes(MetricUnits.COUNT.toString(), ShardStatsDerivedDimension.values()));
    allMetricsInitializer.put(
        ShardStatsValue.CACHE_REQUEST_SIZE.toString(),
        new MetricAttributes(MetricUnits.BYTE.toString(), ShardStatsDerivedDimension.values()));
    allMetricsInitializer.put(
        ShardStatsValue.REFRESH_EVENT.toString(),
        new MetricAttributes(MetricUnits.COUNT.toString(), ShardStatsDerivedDimension.values()));
    allMetricsInitializer.put(
        ShardStatsValue.REFRESH_TIME.toString(),
        new MetricAttributes(
            MetricUnits.MILLISECOND.toString(), ShardStatsDerivedDimension.values()));
    allMetricsInitializer.put(
        ShardStatsValue.FLUSH_EVENT.toString(),
        new MetricAttributes(MetricUnits.COUNT.toString(), ShardStatsDerivedDimension.values()));
    allMetricsInitializer.put(
        ShardStatsValue.FLUSH_TIME.toString(),
        new MetricAttributes(
            MetricUnits.MILLISECOND.toString(), ShardStatsDerivedDimension.values()));
    allMetricsInitializer.put(
        ShardStatsValue.MERGE_EVENT.toString(),
        new MetricAttributes(MetricUnits.COUNT.toString(), ShardStatsDerivedDimension.values()));
    allMetricsInitializer.put(
        ShardStatsValue.MERGE_TIME.toString(),
        new MetricAttributes(
            MetricUnits.MILLISECOND.toString(), ShardStatsDerivedDimension.values()));
    allMetricsInitializer.put(
        ShardStatsValue.MERGE_CURRENT_EVENT.toString(),
        new MetricAttributes(MetricUnits.COUNT.toString(), ShardStatsDerivedDimension.values()));
    allMetricsInitializer.put(
        ShardStatsValue.INDEXING_BUFFER.toString(),
        new MetricAttributes(MetricUnits.BYTE.toString(), ShardStatsDerivedDimension.values()));
    allMetricsInitializer.put(
        ShardStatsValue.SEGMENTS_TOTAL.toString(),
        new MetricAttributes(MetricUnits.COUNT.toString(), ShardStatsDerivedDimension.values()));
    allMetricsInitializer.put(
        ShardStatsValue.SEGMENTS_MEMORY.toString(),
        new MetricAttributes(MetricUnits.BYTE.toString(), ShardStatsDerivedDimension.values()));
    allMetricsInitializer.put(
        ShardStatsValue.TERMS_MEMORY.toString(),
        new MetricAttributes(MetricUnits.BYTE.toString(), ShardStatsDerivedDimension.values()));
    allMetricsInitializer.put(
        ShardStatsValue.STORED_FIELDS_MEMORY.toString(),
        new MetricAttributes(MetricUnits.BYTE.toString(), ShardStatsDerivedDimension.values()));
    allMetricsInitializer.put(
        ShardStatsValue.TERM_VECTOR_MEMORY.toString(),
        new MetricAttributes(MetricUnits.BYTE.toString(), ShardStatsDerivedDimension.values()));
    allMetricsInitializer.put(
        ShardStatsValue.NORMS_MEMORY.toString(),
        new MetricAttributes(MetricUnits.BYTE.toString(), ShardStatsDerivedDimension.values()));
    allMetricsInitializer.put(
        ShardStatsValue.POINTS_MEMORY.toString(),
        new MetricAttributes(MetricUnits.BYTE.toString(), ShardStatsDerivedDimension.values()));
    allMetricsInitializer.put(
        ShardStatsValue.DOC_VALUES_MEMORY.toString(),
        new MetricAttributes(MetricUnits.BYTE.toString(), ShardStatsDerivedDimension.values()));
    allMetricsInitializer.put(
        ShardStatsValue.INDEX_WRITER_MEMORY.toString(),
        new MetricAttributes(MetricUnits.BYTE.toString(), ShardStatsDerivedDimension.values()));
    allMetricsInitializer.put(
        ShardStatsValue.VERSION_MAP_MEMORY.toString(),
        new MetricAttributes(MetricUnits.BYTE.toString(), ShardStatsDerivedDimension.values()));
    allMetricsInitializer.put(
        ShardStatsValue.BITSET_MEMORY.toString(),
        new MetricAttributes(MetricUnits.BYTE.toString(), ShardStatsDerivedDimension.values()));

    // Master Metrics
    allMetricsInitializer.put(
        MasterPendingValue.MASTER_PENDING_QUEUE_SIZE.toString(),
        new MetricAttributes(MetricUnits.COUNT.toString(), EmptyDimension.values()));

    allMetricsInitializer.put(
        AllMetrics.MasterMetricValues.MASTER_TASK_QUEUE_TIME.toString(),
        new MetricAttributes(
            MetricUnits.MILLISECOND.toString(), AllMetrics.MasterMetricDimensions.values()));

    allMetricsInitializer.put(
        AllMetrics.MasterMetricValues.MASTER_TASK_RUN_TIME.toString(),
        new MetricAttributes(
            MetricUnits.MILLISECOND.toString(), AllMetrics.MasterMetricDimensions.values()));


    ALL_METRICS = Collections.unmodifiableMap(allMetricsInitializer);
  }
}
