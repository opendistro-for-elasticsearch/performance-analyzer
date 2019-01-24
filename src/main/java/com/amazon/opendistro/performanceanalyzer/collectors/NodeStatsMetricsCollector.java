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

import java.lang.reflect.Field;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.List;
import java.util.Iterator;

import com.amazon.opendistro.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.performanceanalyzer.ESResources;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.performanceanalyzer.metrics.MetricsConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.IndexShardStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.NodeIndicesStats;

import com.amazon.opendistro.performanceanalyzer.metrics.MetricsProcessor;
import com.fasterxml.jackson.annotation.JsonIgnore;

@SuppressWarnings("unchecked")
public class NodeStatsMetricsCollector extends PerformanceAnalyzerMetricsCollector implements MetricsProcessor {
    public static final int SAMPLING_TIME_INTERVAL = MetricsConfiguration.CONFIG_MAP.get(NodeStatsMetricsCollector.class).samplingInterval;
    private static final int KEYS_PATH_LENGTH = 2;
    private static final Logger LOG = LogManager.getLogger(NodeStatsMetricsCollector.class);
    private HashMap<ShardId, CachedStats> cachedInfo;
    private static HashMap<ShardId, IndexShard> currentShards;

    public NodeStatsMetricsCollector() {
        super(SAMPLING_TIME_INTERVAL, "NodeStatsMetrics");
        cachedInfo = new HashMap<>();
        currentShards = new HashMap<>();
    }

    private static void populateCurrentShards() {
        currentShards.clear();
        Iterator<IndexService> indexServices = ESResources.INSTANCE.getIndicesService().iterator();
        while (indexServices.hasNext()) {
            Iterator<IndexShard> indexShards = indexServices.next().iterator();
            while (indexShards.hasNext()) {
                IndexShard shard = indexShards.next();
                currentShards.put(shard.shardId(), shard);
            }
        }
    }

    private static final EnumSet<IndexShardState> CAN_WRITE_INDEX_BUFFER_STATES = EnumSet.of(
            IndexShardState.RECOVERING, IndexShardState.POST_RECOVERY, IndexShardState.STARTED);

    private Map<String, ValueCalculator> valueCalculators = new HashMap<String, ValueCalculator>() { {
        put(AllMetrics.ShardStatsValue.indexingThrottleTime.name(),
                (shardStats) -> shardStats.getStats().getIndexing().getTotal().getThrottleTime().millis());

        put(AllMetrics.ShardStatsValue.queryCacheHitCount.name(), (shardStats) -> shardStats.getStats().getQueryCache().getHitCount());
        put(AllMetrics.ShardStatsValue.queryCacheMissCount.name(), (shardStats) -> shardStats.getStats().getQueryCache().getMissCount());
        put(AllMetrics.ShardStatsValue.queryCacheInBytes.name(),
                (shardStats) -> shardStats.getStats().getQueryCache().getMemorySizeInBytes());

        put(AllMetrics.ShardStatsValue.fieldDataEvictions.name(), (shardStats) -> shardStats.getStats().getFieldData().getEvictions());
        put(AllMetrics.ShardStatsValue.fieldDataInBytes.name(),
                (shardStats) -> shardStats.getStats().getFieldData().getMemorySizeInBytes());

        put(AllMetrics.ShardStatsValue.requestCacheHitCount.name(),
                (shardStats) -> shardStats.getStats().getRequestCache().getHitCount());
        put(AllMetrics.ShardStatsValue.requestCacheMissCount.name(),
                (shardStats) -> shardStats.getStats().getRequestCache().getMissCount());
        put(AllMetrics.ShardStatsValue.requestCacheEvictions.name(),
                (shardStats) -> shardStats.getStats().getRequestCache().getEvictions());
        put(AllMetrics.ShardStatsValue.requestCacheInBytes.name(),
                (shardStats) -> shardStats.getStats().getRequestCache().getMemorySizeInBytes());

        put(AllMetrics.ShardStatsValue.refreshCount.name(), (shardStats) -> shardStats.getStats().getRefresh().getTotal());
        put(AllMetrics.ShardStatsValue.refreshTime.name(), (shardStats) -> shardStats.getStats().getRefresh().getTotalTimeInMillis());

        put(AllMetrics.ShardStatsValue.flushCount.name(), (shardStats) -> shardStats.getStats().getFlush().getTotal());
        put(AllMetrics.ShardStatsValue.flushTime.name(), (shardStats) -> shardStats.getStats().getFlush().getTotalTimeInMillis());

        put(AllMetrics.ShardStatsValue.mergeCount.name(), (shardStats) -> shardStats.getStats().getMerge().getTotal());
        put(AllMetrics.ShardStatsValue.mergeTime.name(), (shardStats) -> shardStats.getStats().getMerge().getTotalTimeInMillis());
        put(AllMetrics.ShardStatsValue.mergeCurrent.name(), (shardStats) -> shardStats.getStats().getMerge().getCurrent());

        put(AllMetrics.ShardStatsValue.segmentCount.name(), (shardStats) -> shardStats.getStats().getSegments().getCount());
        put(AllMetrics.ShardStatsValue.segmentsMemory.name(), (shardStats) -> shardStats.getStats().getSegments().getMemoryInBytes());
        put(AllMetrics.ShardStatsValue.termsMemory.name(),
                (shardStats) -> shardStats.getStats().getSegments().getTermsMemoryInBytes());
        put(AllMetrics.ShardStatsValue.storedFieldsMemory.name(),
                (shardStats) -> shardStats.getStats().getSegments().getStoredFieldsMemoryInBytes());
        put(AllMetrics.ShardStatsValue.termVectorsMemory.name(),
                (shardStats) -> shardStats.getStats().getSegments().getTermVectorsMemoryInBytes());
        put(AllMetrics.ShardStatsValue.normsMemory.name(),
                (shardStats) -> shardStats.getStats().getSegments().getNormsMemoryInBytes());
        put(AllMetrics.ShardStatsValue.pointsMemory.name(),
                (shardStats) -> shardStats.getStats().getSegments().getPointsMemoryInBytes());
        put(AllMetrics.ShardStatsValue.docValuesMemory.name(),
                (shardStats) -> shardStats.getStats().getSegments().getDocValuesMemoryInBytes());
        put(AllMetrics.ShardStatsValue.indexWriterMemory.name(),
                (shardStats) -> shardStats.getStats().getSegments().getIndexWriterMemoryInBytes());
        put(AllMetrics.ShardStatsValue.versionMapMemory.name(),
                (shardStats) -> shardStats.getStats().getSegments().getVersionMapMemoryInBytes());
        put(AllMetrics.ShardStatsValue.bitsetMemory.name(), (shardStats) -> shardStats.getStats().getSegments().getBitsetMemoryInBytes());

        put(AllMetrics.ShardStatsValue.indexBufferBytes.name(), (shardStats) -> getIndexBufferBytes(shardStats));

    } };

    private static long getIndexBufferBytes(ShardStats shardStats) {
        IndexShard shard = currentShards.get(shardStats.getShardRouting().shardId());

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

        NodeIndicesStats nodeIndicesStats = indicesService.stats(true, CommonStatsFlags.ALL);

        HashSet<ShardId> currentShards = new HashSet<>();

        try {
            populateCurrentShards();
            Map<Index, List<IndexShardStats>> statsByShard =
                    (Map<Index, List<IndexShardStats>>) getNodeIndicesStatsByShardField().get(nodeIndicesStats);

            for (List<IndexShardStats> shardStatsList : statsByShard.values()) {
                for (IndexShardStats indexShardStats : shardStatsList) {
                    for (ShardStats shardStats : indexShardStats.getShards()) {
                        currentShards.add(indexShardStats.getShardId());

                        if (!cachedInfo.containsKey(indexShardStats.getShardId())) {
                            cachedInfo.put(indexShardStats.getShardId(), new CachedStats());
                        }

                        CachedStats prevStats = cachedInfo.get(indexShardStats.getShardId());

                        StringBuilder value = new StringBuilder();

                        value.append(PerformanceAnalyzerMetrics.getJsonCurrentMilliSeconds());
                        //- go through the list of metrics to be collected and emit
                        value.append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor)
                                .append(new NodeStatsMetricsStatus(shardStats,
                                        prevStats).serialize());

                        saveMetricValues(value.toString(), startTime, indexShardStats.getShardId().getIndexName(),
                                String.valueOf(indexShardStats.getShardId().id()));
                    }
                }
            }
        } catch (Exception ex) {
            LOG.debug("Exception in Collecting NodesStats Metrics: {} for startTime {}", () -> ex.toString(), () -> startTime);
        }

        //- remove from Cache if IndexName/ShardId is not present anymore...this will keep the Sanity of Cache
        for (Iterator<Map.Entry<ShardId, CachedStats>> it = cachedInfo.entrySet().iterator(); it.hasNext();) {
            Map.Entry<ShardId, CachedStats> entry = it.next();
            if (!currentShards.contains(entry.getKey())) {
                it.remove();
            }
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
        ShardStats shardStats;

        @JsonIgnore
        CachedStats prevStats;

        public final long indexingThrottleTime;
        public final long queryCacheHitCount;
        public final long queryCacheMissCount;
        public final long queryCacheInBytes;
        public final long fieldDataEvictions;
        public final long fieldDataInBytes;
        public final long requestCacheHitCount;
        public final long requestCacheMissCount;
        public final long requestCacheEvictions;
        public final long requestCacheInBytes;
        public final long refreshCount;
        public final long refreshTime;
        public final long flushCount;
        public final long flushTime;
        public final long mergeCount;
        public final long mergeTime;
        public final long mergeCurrent;
        public final long indexBufferBytes;
        public final long segmentCount;
        public final long segmentsMemory;
        public final long termsMemory;
        public final long storedFieldsMemory;
        public final long termVectorsMemory;
        public final long normsMemory;
        public final long pointsMemory;
        public final long docValuesMemory;
        public final long indexWriterMemory;
        public final long versionMapMemory;
        public final long bitsetMemory;

        public NodeStatsMetricsStatus(ShardStats shardStats,
                CachedStats prevStats) {
            super();
            this.shardStats = shardStats;
            this.prevStats = prevStats;

            this.indexingThrottleTime = calculate(
                    AllMetrics.ShardStatsValue.indexingThrottleTime);
            this.queryCacheHitCount = calculate(
                    AllMetrics.ShardStatsValue.queryCacheHitCount);
            this.queryCacheMissCount = calculate(
                    AllMetrics.ShardStatsValue.queryCacheMissCount);
            this.queryCacheInBytes = calculate(
                    AllMetrics.ShardStatsValue.queryCacheInBytes);
            this.fieldDataEvictions = calculate(
                    AllMetrics.ShardStatsValue.fieldDataEvictions);
            this.fieldDataInBytes = calculate(AllMetrics.ShardStatsValue.fieldDataInBytes);
            this.requestCacheHitCount = calculate(
                    AllMetrics.ShardStatsValue.requestCacheHitCount);
            this.requestCacheMissCount = calculate(
                    AllMetrics.ShardStatsValue.requestCacheMissCount);
            this.requestCacheEvictions = calculate(
                    AllMetrics.ShardStatsValue.requestCacheEvictions);
            this.requestCacheInBytes = calculate(
                    AllMetrics.ShardStatsValue.requestCacheInBytes);
            this.refreshCount = calculate(AllMetrics.ShardStatsValue.refreshCount);
            this.refreshTime = calculate(AllMetrics.ShardStatsValue.refreshTime);
            this.flushCount = calculate(AllMetrics.ShardStatsValue.flushCount);
            this.flushTime = calculate(AllMetrics.ShardStatsValue.flushTime);
            this.mergeCount = calculate(AllMetrics.ShardStatsValue.mergeCount);
            this.mergeTime = calculate(AllMetrics.ShardStatsValue.mergeTime);
            this.mergeCurrent = calculate(AllMetrics.ShardStatsValue.mergeCurrent);
            this.indexBufferBytes = calculate(AllMetrics.ShardStatsValue.indexBufferBytes);
            this.segmentCount = calculate(AllMetrics.ShardStatsValue.segmentCount);
            this.segmentsMemory = calculate(AllMetrics.ShardStatsValue.segmentsMemory);
            this.termsMemory = calculate(AllMetrics.ShardStatsValue.termsMemory);
            this.storedFieldsMemory = calculate(
                    AllMetrics.ShardStatsValue.storedFieldsMemory);
            this.termVectorsMemory = calculate(AllMetrics.ShardStatsValue.termsMemory);
            this.normsMemory = calculate(AllMetrics.ShardStatsValue.normsMemory);
            this.pointsMemory = calculate(AllMetrics.ShardStatsValue.pointsMemory);
            this.docValuesMemory = calculate(AllMetrics.ShardStatsValue.docValuesMemory);
            this.indexWriterMemory = calculate(
                    AllMetrics.ShardStatsValue.indexWriterMemory);
            this.versionMapMemory = calculate(AllMetrics.ShardStatsValue.versionMapMemory);
            this.bitsetMemory = calculate(AllMetrics.ShardStatsValue.bitsetMemory);
        }

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
            this.prevStats = null;
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


        private long calculate(AllMetrics.ShardStatsValue nodeMetric) {
            return valueCalculators.get(nodeMetric.name()).calculate(
                    shardStats, prevStats, nodeMetric.name());
        }
    }
}
