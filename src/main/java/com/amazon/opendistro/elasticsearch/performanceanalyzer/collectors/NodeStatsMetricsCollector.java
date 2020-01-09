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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors;

import java.lang.reflect.Field;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PerformanceAnalyzerController;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.NodeIndicesStats;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ESResources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ShardStatsValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatExceptionCode;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

@SuppressWarnings("unchecked")
public class NodeStatsMetricsCollector extends PerformanceAnalyzerMetricsCollector implements MetricsProcessor {
    public static final int SAMPLING_TIME_INTERVAL = MetricsConfiguration.CONFIG_MAP.get(NodeStatsMetricsCollector.class).samplingInterval;
    private static final int KEYS_PATH_LENGTH = 2;
    private static final Logger LOG = LogManager.getLogger(NodeStatsMetricsCollector.class);
    private HashMap<String, IndexShard> currentShards;
    private Iterator<HashMap.Entry<String, IndexShard>> currentShardsIter;
    private final PerformanceAnalyzerController controller;


    public NodeStatsMetricsCollector(final PerformanceAnalyzerController controller) {
        super(SAMPLING_TIME_INTERVAL, "NodeStatsMetrics");
        currentShards = new HashMap<>();
        currentShardsIter = currentShards.entrySet().iterator();
        this.controller = controller;
    }

    private String getUniqueShardIdKey(ShardId shardId) {
        return "[" + shardId.getIndex().getUUID() + "][" + shardId.getId() + "]";
    }

    private void populateCurrentShards() {
        currentShards.clear();
        Iterator<IndexService> indexServices = ESResources.INSTANCE.getIndicesService().iterator();
        while (indexServices.hasNext()) {
            Iterator<IndexShard> indexShards = indexServices.next().iterator();
            while (indexShards.hasNext()) {
                IndexShard shard = indexShards.next();
                currentShards.put(getUniqueShardIdKey(shard.shardId()), shard);
            }
        }
        currentShardsIter = currentShards.entrySet().iterator();
    }

    /**
     * This function is copied directly from IndicesService.java in elastic search as the original function is not public
     * we need to collect stats per shard based instead of calling the stat() function to fetch all at once(which increases
     * cpu usage on data nodes dramatically).
     */
    private IndexShardStats indexShardStats(final IndicesService indicesService, final IndexShard indexShard,
                                            final CommonStatsFlags flags) {
        if (indexShard.routingEntry() == null) {
            return null;
        }

        return new IndexShardStats(
                indexShard.shardId(),
                new ShardStats[]{
                        new ShardStats(
                                indexShard.routingEntry(),
                                indexShard.shardPath(),
                                new CommonStats(indicesService.getIndicesQueryCache(), indexShard, flags),
                                null,
                                null,
                                null)
                });
    }

    private static final EnumSet<IndexShardState> CAN_WRITE_INDEX_BUFFER_STATES = EnumSet.of(
            IndexShardState.RECOVERING, IndexShardState.POST_RECOVERY, IndexShardState.STARTED);

    private Map<String, ValueCalculator> valueCalculators = new HashMap<String, ValueCalculator>() { {
        put(ShardStatsValue.INDEXING_THROTTLE_TIME.toString(),
                (shardStats) -> shardStats.getStats().getIndexing().getTotal().getThrottleTime().millis());

        put(ShardStatsValue.CACHE_QUERY_HIT.toString(), (shardStats) -> shardStats.getStats().getQueryCache().getHitCount());
        put(ShardStatsValue.CACHE_QUERY_MISS.toString(), (shardStats) -> shardStats.getStats().getQueryCache().getMissCount());
        put(ShardStatsValue.CACHE_QUERY_SIZE.toString(), (shardStats) -> shardStats.getStats().getQueryCache().getMemorySizeInBytes());

        put(ShardStatsValue.CACHE_FIELDDATA_EVICTION.toString(), (shardStats) -> shardStats.getStats().getFieldData().getEvictions());
        put(ShardStatsValue.CACHE_FIELDDATA_SIZE.toString(), (shardStats) -> shardStats.getStats().getFieldData().getMemorySizeInBytes());

        put(ShardStatsValue.CACHE_REQUEST_HIT.toString(), (shardStats) -> shardStats.getStats().getRequestCache().getHitCount());
        put(ShardStatsValue.CACHE_REQUEST_MISS.toString(), (shardStats) -> shardStats.getStats().getRequestCache().getMissCount());
        put(ShardStatsValue.CACHE_REQUEST_EVICTION.toString(), (shardStats) -> shardStats.getStats().getRequestCache().getEvictions());
        put(ShardStatsValue.CACHE_REQUEST_SIZE.toString(), (shardStats) -> shardStats.getStats().getRequestCache().getMemorySizeInBytes());

        put(ShardStatsValue.REFRESH_EVENT.toString(), (shardStats) -> shardStats.getStats().getRefresh().getTotal());
        put(ShardStatsValue.REFRESH_TIME.toString(), (shardStats) -> shardStats.getStats().getRefresh().getTotalTimeInMillis());

        put(ShardStatsValue.FLUSH_EVENT.toString(), (shardStats) -> shardStats.getStats().getFlush().getTotal());
        put(ShardStatsValue.FLUSH_TIME.toString(), (shardStats) -> shardStats.getStats().getFlush().getTotalTimeInMillis());

        put(ShardStatsValue.MERGE_EVENT.toString(), (shardStats) -> shardStats.getStats().getMerge().getTotal());
        put(ShardStatsValue.MERGE_TIME.toString(), (shardStats) -> shardStats.getStats().getMerge().getTotalTimeInMillis());
        put(ShardStatsValue.MERGE_CURRENT_EVENT.toString(), (shardStats) -> shardStats.getStats().getMerge().getCurrent());

        put(ShardStatsValue.SEGMENTS_TOTAL.toString(), (shardStats) -> shardStats.getStats().getSegments().getCount());
        put(ShardStatsValue.SEGMENTS_MEMORY.toString(), (shardStats) -> shardStats.getStats().getSegments().getMemoryInBytes());
        put(ShardStatsValue.TERMS_MEMORY.toString(), (shardStats) -> shardStats.getStats().getSegments().getTermsMemoryInBytes());
        put(ShardStatsValue.STORED_FIELDS_MEMORY.toString(),
                (shardStats) -> shardStats.getStats().getSegments().getStoredFieldsMemoryInBytes());
        put(ShardStatsValue.TERM_VECTOR_MEMORY.toString(),
                (shardStats) -> shardStats.getStats().getSegments().getTermVectorsMemoryInBytes());
        put(ShardStatsValue.NORMS_MEMORY.toString(), (shardStats) -> shardStats.getStats().getSegments().getNormsMemoryInBytes());
        put(ShardStatsValue.POINTS_MEMORY.toString(), (shardStats) -> shardStats.getStats().getSegments().getPointsMemoryInBytes());
        put(ShardStatsValue.DOC_VALUES_MEMORY.toString(), (shardStats) -> shardStats.getStats().getSegments().getDocValuesMemoryInBytes());
        put(ShardStatsValue.INDEX_WRITER_MEMORY.toString(),
                (shardStats) -> shardStats.getStats().getSegments().getIndexWriterMemoryInBytes());
            put(ShardStatsValue.VERSION_MAP_MEMORY.toString(),
                    (shardStats) -> shardStats.getStats().getSegments()
                            .getVersionMapMemoryInBytes());
            put(ShardStatsValue.BITSET_MEMORY.toString(), (shardStats) -> shardStats.getStats().getSegments().getBitsetMemoryInBytes());

        put(ShardStatsValue.INDEXING_BUFFER.toString(), (shardStats) -> getIndexBufferBytes(shardStats));

    } };

    private long getIndexBufferBytes(ShardStats shardStats) {
        IndexShard shard = currentShards.get(getUniqueShardIdKey(shardStats.getShardRouting().shardId()));

        if (shard == null) {
            return 0;
        }

        return CAN_WRITE_INDEX_BUFFER_STATES.contains(shard.state()) ? shard.getWritingBytes() + shard.getIndexBufferRAMBytesUsed() : 0;
    }


    @Override
    public String getMetricsPath(long startTime, String... keysPath) {
        // throw exception if keysPath.length is not equal to 2 (Keys should be Index Name, and ShardId)
        if (keysPath.length != KEYS_PATH_LENGTH) {
            throw new RuntimeException("keys length should be " + KEYS_PATH_LENGTH);
        }


        return PerformanceAnalyzerMetrics.generatePath(startTime, PerformanceAnalyzerMetrics.sIndicesPath, keysPath[0], keysPath[1]);
    }

    @Override
    public void collectMetrics(long startTime) {
        IndicesService indicesService = ESResources.INSTANCE.getIndicesService();

        if (indicesService == null) {
            return;
        }
        
        try {
            //reach the end of current shardId list. retrieve new shard list from IndexService
            if (!currentShardsIter.hasNext()) {
                populateCurrentShards();
            }
            for(int i = 0; i < controller.getNodeStatsShardsPerCollection(); i++){
                if (!currentShardsIter.hasNext()) {
                    break;
                }
                IndexShard currentIndexShard = currentShardsIter.next().getValue();
                IndexShardStats currentIndexShardStats = this.indexShardStats(indicesService, currentIndexShard, CommonStatsFlags.ALL);
                for (ShardStats shardStats : currentIndexShardStats.getShards()) {
                    StringBuilder value = new StringBuilder();

                    value.append(PerformanceAnalyzerMetrics.getJsonCurrentMilliSeconds());
                    //- go through the list of metrics to be collected and emit
                    value.append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor)
                            .append(new NodeStatsMetricsStatus(shardStats).serialize());

                    saveMetricValues(value.toString(), startTime, currentIndexShardStats.getShardId().getIndexName(),
                            String.valueOf(currentIndexShardStats.getShardId().id()));
                }
            }
        } catch (Exception ex) {
            LOG.debug("Exception in Collecting NodesStats Metrics: {} for startTime {} with ExceptionCode: {}",
                      () -> ex.toString(), () -> startTime, () -> StatExceptionCode.NODESTATS_COLLECTION_ERROR.toString());
            StatsCollector.instance().logException(StatExceptionCode.NODESTATS_COLLECTION_ERROR);
        }
    }

    //- Separated to have a unit test; and catch any code changes around this field
    Field getNodeIndicesStatsByShardField() throws Exception {
        Field field = NodeIndicesStats.class.getDeclaredField("statsByShard");
        field.setAccessible(true);
        return field;
    }

    public class NodeStatsMetricsStatus extends MetricStatus {

        @JsonIgnore
        private ShardStats shardStats;

        private final long indexingThrottleTime;
        private final long queryCacheHitCount;
        private final long queryCacheMissCount;
        private final long queryCacheInBytes;
        private final long fieldDataEvictions;
        private final long fieldDataInBytes;
        private final long requestCacheHitCount;
        private final long requestCacheMissCount;
        private final long requestCacheEvictions;
        private final long requestCacheInBytes;
        private final long refreshCount;
        private final long refreshTime;
        private final long flushCount;
        private final long flushTime;
        private final long mergeCount;
        private final long mergeTime;
        private final long mergeCurrent;
        private final long indexBufferBytes;
        private final long segmentCount;
        private final long segmentsMemory;
        private final long termsMemory;
        private final long storedFieldsMemory;
        private final long termVectorsMemory;
        private final long normsMemory;
        private final long pointsMemory;
        private final long docValuesMemory;
        private final long indexWriterMemory;
        private final long versionMapMemory;
        private final long bitsetMemory;

        public NodeStatsMetricsStatus(ShardStats shardStats) {
            super();
            this.shardStats = shardStats;

            this.indexingThrottleTime = calculate(
                    ShardStatsValue.INDEXING_THROTTLE_TIME);
            this.queryCacheHitCount = calculate(
                    ShardStatsValue.CACHE_QUERY_HIT);
            this.queryCacheMissCount = calculate(
                    ShardStatsValue.CACHE_QUERY_MISS);
            this.queryCacheInBytes = calculate(
                    ShardStatsValue.CACHE_QUERY_SIZE);
            this.fieldDataEvictions = calculate(
                    ShardStatsValue.CACHE_FIELDDATA_EVICTION);
            this.fieldDataInBytes = calculate(ShardStatsValue.CACHE_FIELDDATA_SIZE);
            this.requestCacheHitCount = calculate(
                    ShardStatsValue.CACHE_REQUEST_HIT);
            this.requestCacheMissCount = calculate(
                    ShardStatsValue.CACHE_REQUEST_MISS);
            this.requestCacheEvictions = calculate(
                    ShardStatsValue.CACHE_REQUEST_EVICTION);
            this.requestCacheInBytes = calculate(
                    ShardStatsValue.CACHE_REQUEST_SIZE);
            this.refreshCount = calculate(ShardStatsValue.REFRESH_EVENT);
            this.refreshTime = calculate(ShardStatsValue.REFRESH_TIME);
            this.flushCount = calculate(ShardStatsValue.FLUSH_EVENT);
            this.flushTime = calculate(ShardStatsValue.FLUSH_TIME);
            this.mergeCount = calculate(ShardStatsValue.MERGE_EVENT);
            this.mergeTime = calculate(ShardStatsValue.MERGE_TIME);
            this.mergeCurrent = calculate(ShardStatsValue.MERGE_CURRENT_EVENT);
            this.indexBufferBytes = calculate(ShardStatsValue.INDEXING_BUFFER);
            this.segmentCount = calculate(ShardStatsValue.SEGMENTS_TOTAL);
            this.segmentsMemory = calculate(ShardStatsValue.SEGMENTS_MEMORY);
            this.termsMemory = calculate(ShardStatsValue.TERMS_MEMORY);
            this.storedFieldsMemory = calculate(
                    ShardStatsValue.STORED_FIELDS_MEMORY);
            this.termVectorsMemory = calculate(ShardStatsValue.TERMS_MEMORY);
            this.normsMemory = calculate(ShardStatsValue.NORMS_MEMORY);
            this.pointsMemory = calculate(ShardStatsValue.POINTS_MEMORY);
            this.docValuesMemory = calculate(ShardStatsValue.DOC_VALUES_MEMORY);
            this.indexWriterMemory = calculate(
                    ShardStatsValue.INDEX_WRITER_MEMORY);
            this.versionMapMemory = calculate(ShardStatsValue.VERSION_MAP_MEMORY);
            this.bitsetMemory = calculate(ShardStatsValue.BITSET_MEMORY);
        }

        @SuppressWarnings("checkstyle:parameternumber")
        public NodeStatsMetricsStatus(long indexingThrottleTime,
                long queryCacheHitCount, long queryCacheMissCount,
                long queryCacheInBytes, long fieldDataEvictions,
                long fieldDataInBytes, long requestCacheHitCount,
                long requestCacheMissCount, long requestCacheEvictions,
                long requestCacheInBytes, long refreshCount, long refreshTime,
                long flushCount, long flushTime, long mergeCount,
                long mergeTime, long mergeCurrent, long indexBufferBytes,
                long segmentCount, long segmentsMemory, long termsMemory,
                long storedFieldsMemory, long termVectorsMemory,
                long normsMemory, long pointsMemory, long docValuesMemory,
                long indexWriterMemory, long versionMapMemory,
                long bitsetMemory) {
            super();
            this.shardStats = null;
            this.indexingThrottleTime = indexingThrottleTime;
            this.queryCacheHitCount = queryCacheHitCount;
            this.queryCacheMissCount = queryCacheMissCount;
            this.queryCacheInBytes = queryCacheInBytes;
            this.fieldDataEvictions = fieldDataEvictions;
            this.fieldDataInBytes = fieldDataInBytes;
            this.requestCacheHitCount = requestCacheHitCount;
            this.requestCacheMissCount = requestCacheMissCount;
            this.requestCacheEvictions = requestCacheEvictions;
            this.requestCacheInBytes = requestCacheInBytes;
            this.refreshCount = refreshCount;
            this.refreshTime = refreshTime;
            this.flushCount = flushCount;
            this.flushTime = flushTime;
            this.mergeCount = mergeCount;
            this.mergeTime = mergeTime;
            this.mergeCurrent = mergeCurrent;
            this.indexBufferBytes = indexBufferBytes;
            this.segmentCount = segmentCount;
            this.segmentsMemory = segmentsMemory;
            this.termsMemory = termsMemory;
            this.storedFieldsMemory = storedFieldsMemory;
            this.termVectorsMemory = termVectorsMemory;
            this.normsMemory = normsMemory;
            this.pointsMemory = pointsMemory;
            this.docValuesMemory = docValuesMemory;
            this.indexWriterMemory = indexWriterMemory;
            this.versionMapMemory = versionMapMemory;
            this.bitsetMemory = bitsetMemory;
        }


        private long calculate(ShardStatsValue nodeMetric) {
            return valueCalculators.get(nodeMetric.toString()).calculateValue(shardStats);
        }

        @JsonIgnore
        public ShardStats getShardStats() {
            return shardStats;
        }

        @JsonProperty(ShardStatsValue.Constants.INDEXING_THROTTLE_TIME_VALUE)
        public long getIndexingThrottleTime() {
            return indexingThrottleTime;
        }

        @JsonProperty(ShardStatsValue.Constants.QUEY_CACHE_HIT_COUNT_VALUE)
        public long getQueryCacheHitCount() {
            return queryCacheHitCount;
        }

        @JsonProperty(ShardStatsValue.Constants.QUERY_CACHE_MISS_COUNT_VALUE)
        public long getQueryCacheMissCount() {
            return queryCacheMissCount;
        }

        @JsonProperty(ShardStatsValue.Constants.QUERY_CACHE_IN_BYTES_VALUE)
        public long getQueryCacheInBytes() {
            return queryCacheInBytes;
        }

        @JsonProperty(ShardStatsValue.Constants.FIELDDATA_EVICTION_VALUE)
        public long getFieldDataEvictions() {
            return fieldDataEvictions;
        }

        @JsonProperty(ShardStatsValue.Constants.FIELD_DATA_IN_BYTES_VALUE)
        public long getFieldDataInBytes() {
            return fieldDataInBytes;
        }

        @JsonProperty(ShardStatsValue.Constants.REQUEST_CACHE_HIT_COUNT_VALUE)
        public long getRequestCacheHitCount() {
            return requestCacheHitCount;
        }

        @JsonProperty(ShardStatsValue.Constants.REQUEST_CACHE_MISS_COUNT_VALUE)
        public long getRequestCacheMissCount() {
            return requestCacheMissCount;
        }

        @JsonProperty(ShardStatsValue.Constants.REQUEST_CACHE_EVICTION_VALUE)
        public long getRequestCacheEvictions() {
            return requestCacheEvictions;
        }

        @JsonProperty(ShardStatsValue.Constants.REQUEST_CACHE_IN_BYTES_VALUE)
        public long getRequestCacheInBytes() {
            return requestCacheInBytes;
        }

        @JsonProperty(ShardStatsValue.Constants.REFRESH_COUNT_VALUE)
        public long getRefreshCount() {
            return refreshCount;
        }

        @JsonProperty(ShardStatsValue.Constants.REFRESH_TIME_VALUE)
        public long getRefreshTime() {
            return refreshTime;
        }

        @JsonProperty(ShardStatsValue.Constants.FLUSH_COUNT_VALUE)
        public long getFlushCount() {
            return flushCount;
        }

        @JsonProperty(ShardStatsValue.Constants.FLUSH_TIME_VALUE)
        public long getFlushTime() {
            return flushTime;
        }

        @JsonProperty(ShardStatsValue.Constants.MERGE_COUNT_VALUE)
        public long getMergeCount() {
            return mergeCount;
        }

        @JsonProperty(ShardStatsValue.Constants.MERGE_TIME_VALUE)
        public long getMergeTime() {
            return mergeTime;
        }

        @JsonProperty(ShardStatsValue.Constants.MERGE_CURRENT_VALUE)
        public long getMergeCurrent() {
            return mergeCurrent;
        }

        @JsonProperty(ShardStatsValue.Constants.INDEX_BUFFER_BYTES_VALUE)
        public long getIndexBufferBytes() {
            return indexBufferBytes;
        }

        @JsonProperty(ShardStatsValue.Constants.SEGMENTS_COUNT_VALUE)
        public long getSegmentCount() {
            return segmentCount;
        }

        @JsonProperty(ShardStatsValue.Constants.SEGMENTS_MEMORY_VALUE)
        public long getSegmentsMemory() {
            return segmentsMemory;
        }

        @JsonProperty(ShardStatsValue.Constants.TERMS_MEMORY_VALUE)
        public long getTermsMemory() {
            return termsMemory;
        }

        @JsonProperty(ShardStatsValue.Constants.STORED_FIELDS_MEMORY_VALUE)
        public long getStoredFieldsMemory() {
            return storedFieldsMemory;
        }

        @JsonProperty(ShardStatsValue.Constants.TERM_VECTOR_MEMORY_VALUE)
        public long getTermVectorsMemory() {
            return termVectorsMemory;
        }

        @JsonProperty(ShardStatsValue.Constants.NORMS_MEMORY_VALUE)
        public long getNormsMemory() {
            return normsMemory;
        }

        @JsonProperty(ShardStatsValue.Constants.POINTS_MEMORY_VALUE)
        public long getPointsMemory() {
            return pointsMemory;
        }

        @JsonProperty(ShardStatsValue.Constants.DOC_VALUES_MEMORY_VALUE)
        public long getDocValuesMemory() {
            return docValuesMemory;
        }

        @JsonProperty(ShardStatsValue.Constants.INDEX_WRITER_MEMORY_VALUE)
        public long getIndexWriterMemory() {
            return indexWriterMemory;
        }

        @JsonProperty(ShardStatsValue.Constants.VERSION_MAP_MEMORY_VALUE)
        public long getVersionMapMemory() {
            return versionMapMemory;
        }

        @JsonProperty(ShardStatsValue.Constants.BITSET_MEMORY_VALUE)
        public long getBitsetMemory() {
            return bitsetMemory;
        }
    }
}
