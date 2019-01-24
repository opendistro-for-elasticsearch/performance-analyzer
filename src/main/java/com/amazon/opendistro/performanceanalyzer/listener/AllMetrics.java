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

package com.amazon.opendistro.performanceanalyzer.metrics;

/**
 * Contract between reader and writer.  Writer write using the same values of
 * these enums as json keys (See all MetricStatus's subclasses in
 * com.amazon.opendistro.performanceanalyzer.collectors), while reader creates db tables using these
 *  keys as column names and extract values using these keys. You should
 *  make sure the the field names in the MetricStatus's subclasses and enum
 *  names match. Also, when you change anything, modify JsonKeyTest accordingly.
 * We use camelCase instead of the usual capital case for enum members because
 * they have better readability for the above use cases.
 *
 */
public class AllMetrics {
    // metric name (not complete, only metrics use the json format and contains
    // numeric values. Will add more when needed)
    public enum MetricName {
        circuit_breaker, heap_metrics, disk_metrics, tcp_metrics, ip_metrics,
        thread_pool, shard_stats, master_pending
    }

    // we don't store node details as a metric on reader side database.  We
    // use the information as part of http response.
    public enum NodeDetailColumns {
        ID, hostAddress
    }

    // contents of metrics
    public enum GCType {
        totYoungGC, totFullGC, Surivor, PermGen, OldGen, Eden, NonHeap, Heap
    }

    // column names of database table
    public enum CircuitBreakerDimension implements MetricDimension {
        type
    }

    // cannot use limit as it is a keyword in sql
    public enum CircuitBreakerValue implements MetricValue {
        estimated, tripped, limitConfigured
    }

    public enum HeapDimension implements MetricDimension {
        type
    }

    public enum HeapValue implements MetricValue {
        collectionCount, collectionTime, committed, init, max, used
    }

    public enum DiskDimension implements MetricDimension {
        name
    }

    public enum DiskValue implements MetricValue {
        util, wait, srate
    }

    public enum TCPDimension implements MetricDimension {
        dest
    }

    public enum TCPValue implements MetricValue {
        numFlows, txQ, rxQ, curLost, sndCWND, SSThresh
    }

    public enum IPDimension implements MetricDimension {
        direction
    }

    public enum IPValue implements MetricValue {
        packetRate4, dropRate4, packetRate6, dropRate6, bps
    }

    public enum ThreadPoolDimension implements MetricDimension {
        type
    }

    public enum ThreadPoolValue implements MetricValue {
        queueSize, rejected, threadsCount, threadsActive
    }

    // extra dimension values come from other places (e.g., file path) instead
    // of metric files themselves
    public enum ShardStatsDerivedDimension implements MetricDimension {
        indexName, shardID
    }

    public enum ShardStatsValue implements MetricValue {
        indexingThrottleTime, queryCacheHitCount, queryCacheMissCount, queryCacheInBytes, fieldDataEvictions,
        fieldDataInBytes, requestCacheHitCount, requestCacheMissCount, requestCacheEvictions, requestCacheInBytes,
        refreshCount, refreshTime, flushCount, flushTime, mergeCount, mergeTime, mergeCurrent, indexBufferBytes,
        segmentCount, segmentsMemory, termsMemory, storedFieldsMemory, termVectorsMemory, normsMemory, pointsMemory,
        docValuesMemory, indexWriterMemory, versionMapMemory, bitsetMemory
    }

    public enum MasterPendingValue implements MetricValue {
        pendingTasksCount
    }

    public enum Master_Metrics {
        ID, priority, startTime, type, metadata, ageInMillis, finishTime
    }

    public enum OS_Metrics {
        cpu, paging_majflt, paging_minflt, rss, runtime, waittime, ctxrate, heap_usage, avgReadThroughputBps,
        avgWriteThroughputBps, avgTotalThroughputBps, avgReadSyscallRate, avgWriteSyscallRate, avgTotalSyscallRate,
        avgBlockedTime, blockedCount
    }

    public enum Http_Metrics {
        startTime, indices, itemCount, finishTime, exception, status, latency, count
    }

    public enum ShardSearch_Metrics {
        startTime, indexName, shardId, finishTime, failed
    }

//    public enum SearchReducer_Metrics {
//        startTime, hasSuggest, hasProfileResults, finishTime, hasAggs, indexNames
//    }

    public enum ShardBulk_Metrics {
        startTime, itemCount, indexName, shardId, primary, finishTime, exception, failed
    }

    public enum Dimensions implements MetricDimension {
        index, operation, role, shard
    }

    public enum MasterDimensions implements MetricDimension {
        noDimension
    }
}
