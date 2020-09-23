package com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ESResources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.jooq.tools.StringUtils;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics.SHARD_PRIMARY;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics.SHARD_REPLICA;

public class ShardStatsCollector extends PerformanceAnalyzerMetricsCollector implements MetricsProcessor {
    public static final int SAMPLING_TIME_INTERVAL = MetricsConfiguration.CONFIG_MAP.get(ShardStatsCollector.class).samplingInterval;
    private static final Logger LOG = LogManager.getLogger(ShardStatsCollector.class);
    private static final int KEYS_PATH_LENGTH = 0;
    private StringBuilder value;

    public ShardStatsCollector() {
        super(SAMPLING_TIME_INTERVAL, "ShardsStatsCollector");
        value = new StringBuilder();
    }

    @Override
    void collectMetrics( long startTime) {
        if (ESResources.INSTANCE.getClusterService() == null) {
            return;
        }
        ClusterState clusterState = ESResources.INSTANCE.getClusterService().state();

        try {
            value.setLength(0);
            value.append(PerformanceAnalyzerMetrics.getJsonCurrentMilliSeconds())
                    .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor);
            for(ShardRouting shard : clusterState.routingTable().allShards()) {
                String nodeName = StringUtils.EMPTY;
                if (shard.assignedToNode()) {
                    nodeName = clusterState.nodes().get(shard.currentNodeId()).getName();
                }
                value
                        .append(new ShardStats(
                                shard.getIndexName(),
                                shard.getId(),
                                shard.primary()?SHARD_PRIMARY:SHARD_REPLICA,
                                nodeName,
                                shard.state() == ShardRoutingState.STARTED?1:0,
                                shard.state() == ShardRoutingState.INITIALIZING?1:0,
                                shard.state() == ShardRoutingState.UNASSIGNED?1:0)
                                .serialize())
                        .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor);
            }
            saveMetricValues(value.toString(), startTime);

        } catch (Exception ex) {
            LOG.debug("Exception in Collecting Shard Metrics: {} for startTime {}", () -> ex.toString(),
                    () -> startTime);
        }
    }

    @Override
    public String getMetricsPath(long startTime, String... keysPath) {
        if (keysPath.length != KEYS_PATH_LENGTH) {
            throw new RuntimeException("keys length should be " + KEYS_PATH_LENGTH);
        }

        return PerformanceAnalyzerMetrics.generatePath(startTime, PerformanceAnalyzerMetrics.sShardStatePath);
    }

    public static class ShardStats extends MetricStatus {

        private final String indexName;
        private final int shardId;
        private final String shardType;
        private final String nodeName;
        private final int activeShardState;
        private final int initializingShardState;
        private final int unassignedShardState;

        public ShardStats(String indexName, int shardId, String shardType, String nodeName, int activeShardState,
                               int initializingShardState, int unassignedShardState) {
            this.indexName = indexName;
            this.shardId = shardId;
            this.shardType = shardType;
            this.nodeName = nodeName;
            this.activeShardState = activeShardState;
            this.initializingShardState = initializingShardState;
            this.unassignedShardState = unassignedShardState;
        }

        @JsonProperty(AllMetrics.CommonDimension.Constants.INDEX_NAME_VALUE)
        public String getIndexName() {
            return indexName;
        }

        @JsonProperty(AllMetrics.CommonDimension.Constants.SHARDID_VALUE)
        public int getShardId() {
            return shardId;
        }

        @JsonProperty(AllMetrics.ShardStateDimension.Constants.SHARD_TYPE)
        public String getShardType() {
            return shardType;
        }

        @JsonProperty(AllMetrics.ShardStateDimension.Constants.NODE_NAME)
        public String  getNodeName() {
            return nodeName;
        }

        @JsonProperty(AllMetrics.ShardStateValue.Constants.SHARD_STATE_ACTIVE)
        public int getActiveShardState() {
            return activeShardState;
        }

        @JsonProperty(AllMetrics.ShardStateValue.Constants.SHARD_STATE_INITIALIZING)
        public int getInitializingShardState() {
            return initializingShardState;
        }

        @JsonProperty(AllMetrics.ShardStateValue.Constants.SHARD_STATE_UNASSIGNED)
        public int getUnassignedShardState() {
            return unassignedShardState;
        }
    }

}
