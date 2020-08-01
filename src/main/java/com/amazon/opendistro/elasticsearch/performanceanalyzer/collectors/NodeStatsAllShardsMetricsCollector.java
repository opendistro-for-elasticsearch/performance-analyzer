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
import java.util.HashMap;
import java.util.Map;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PerformanceAnalyzerController;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.NodeIndicesStats;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.ESResources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ShardStatsValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

@SuppressWarnings("unchecked")
public class NodeStatsAllShardsMetricsCollector extends PerformanceAnalyzerMetricsCollector implements MetricsProcessor {
    public static final int SAMPLING_TIME_INTERVAL = MetricsConfiguration.CONFIG_MAP.get(
            NodeStatsAllShardsMetricsCollector.class).samplingInterval;
    private static final int KEYS_PATH_LENGTH = 2;
    private static final Logger LOG = LogManager.getLogger(NodeStatsAllShardsMetricsCollector.class);
    private HashMap<String, IndexShard> currentShards;
    private final PerformanceAnalyzerController controller;


    public NodeStatsAllShardsMetricsCollector(final PerformanceAnalyzerController controller) {
        super(SAMPLING_TIME_INTERVAL, "NodeStatsMetrics");
        currentShards = new HashMap<>();
        this.controller = controller;
    }

    private String getUniqueShardIdKey(ShardId shardId) {
        return "[" + shardId.getIndex().getUUID() + "][" + shardId.getId() + "]";
    }

    private void populateCurrentShards() {
        currentShards.clear();
        currentShards = NodeStatsUtils.getShards();
    }

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

    } };

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
            populateCurrentShards();
            // Metrics populated for all shards in every collection.
            for (HashMap.Entry currentShard : currentShards.entrySet() ){
                IndexShard currentIndexShard = (IndexShard)currentShard.getValue();
                IndexShardStats currentIndexShardStats = NodeStatsUtils.indexShardStats(indicesService,
                        currentIndexShard, new CommonStatsFlags(CommonStatsFlags.Flag.QueryCache,
                                CommonStatsFlags.Flag.FieldData,
                                CommonStatsFlags.Flag.RequestCache));
                for (ShardStats shardStats : currentIndexShardStats.getShards()) {
                    StringBuilder value = new StringBuilder();

                    value.append(PerformanceAnalyzerMetrics.getJsonCurrentMilliSeconds());
                    // Populate the result with cache specific metrics only.
                    value.append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor)
                            .append(new NodeStatsMetricsAllShardsPerCollectionStatus(shardStats).serialize());
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

    public class NodeStatsMetricsAllShardsPerCollectionStatus extends MetricStatus {

        @JsonIgnore
        private ShardStats shardStats;

        private final long queryCacheHitCount;
        private final long queryCacheMissCount;
        private final long queryCacheInBytes;
        private final long fieldDataEvictions;
        private final long fieldDataInBytes;
        private final long requestCacheHitCount;
        private final long requestCacheMissCount;
        private final long requestCacheEvictions;
        private final long requestCacheInBytes;

        public NodeStatsMetricsAllShardsPerCollectionStatus(ShardStats shardStats) {
            super();
            this.shardStats = shardStats;

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
        }

        @SuppressWarnings("checkstyle:parameternumber")
        public NodeStatsMetricsAllShardsPerCollectionStatus(long queryCacheHitCount, long queryCacheMissCount,
                                                            long queryCacheInBytes, long fieldDataEvictions,
                                                            long fieldDataInBytes, long requestCacheHitCount,
                                                            long requestCacheMissCount, long requestCacheEvictions,
                                                            long requestCacheInBytes) {
            super();
            this.shardStats = null;

            this.queryCacheHitCount = queryCacheHitCount;
            this.queryCacheMissCount = queryCacheMissCount;
            this.queryCacheInBytes = queryCacheInBytes;
            this.fieldDataEvictions = fieldDataEvictions;
            this.fieldDataInBytes = fieldDataInBytes;
            this.requestCacheHitCount = requestCacheHitCount;
            this.requestCacheMissCount = requestCacheMissCount;
            this.requestCacheEvictions = requestCacheEvictions;
            this.requestCacheInBytes = requestCacheInBytes;
        }


        private long calculate(ShardStatsValue nodeMetric) {
            return valueCalculators.get(nodeMetric.toString()).calculateValue(shardStats);
        }

        @JsonIgnore
        public ShardStats getShardStats() {
            return shardStats;
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

    }
}
