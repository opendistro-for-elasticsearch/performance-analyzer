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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ESResources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.ExceptionsAndErrors;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.WriterMetrics;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.jooq.tools.StringUtils;
import org.jooq.tools.json.JSONObject;

import java.util.List;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ShardType.SHARD_PRIMARY;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ShardType.SHARD_REPLICA;

public class ShardStateCollector extends PerformanceAnalyzerMetricsCollector implements MetricsProcessor {
    public static final int SAMPLING_TIME_INTERVAL = MetricsConfiguration.CONFIG_MAP.get(ShardStateCollector.class).samplingInterval;
    private static final Logger LOG = LogManager.getLogger(ShardStateCollector.class);
    private static final int KEYS_PATH_LENGTH = 0;
    private StringBuilder value;

    public ShardStateCollector() {
        super(SAMPLING_TIME_INTERVAL, "ShardsStateCollector");
        value = new StringBuilder();
    }

    @Override
    void collectMetrics( long startTime) {
        long mCurrT = System.currentTimeMillis();
        if (ESResources.INSTANCE.getClusterService() == null) {
            return;
        }
        ClusterState clusterState = ESResources.INSTANCE.getClusterService().state();
        boolean inActiveShard = false;
        try {
            value.setLength(0);
            value.append(PerformanceAnalyzerMetrics.getJsonCurrentMilliSeconds())
            .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor);
            RoutingTable routingTable = clusterState.routingTable();
            String[] indices = routingTable.indicesRouting().keys().toArray(String.class);
            for (String index : indices) {
                List<ShardRouting> allShardsIndex = routingTable.allShards(index);
                value.append(createJsonObject(AllMetrics.ShardStateDimension.INDEX_NAME.toString(), index));
                for (ShardRouting shard :allShardsIndex) {
                    String nodeName = StringUtils.EMPTY;
                    if (shard.assignedToNode()) {
                        nodeName = clusterState.nodes().get(shard.currentNodeId()).getName();
                    }
                    if (shard.state() != ShardRoutingState.STARTED) {
                        inActiveShard = true;
                        value
                                .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor)
                                .append(new ShardStateMetrics(
                                        shard.getId(),
                                        shard.primary() ? SHARD_PRIMARY.toString() : SHARD_REPLICA.toString(),
                                        nodeName,
                                        shard.state().name())
                                        .serialize());

                    }
                }
            }
            value.append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor);
            if(inActiveShard) {
                saveMetricValues(value.toString(), startTime);
            }
            PerformanceAnalyzerApp.WRITER_METRICS_AGGREGATOR.updateStat(
                    WriterMetrics.SHARD_STATE_COLLECTOR_EXECUTION_TIME, "",
                    System.currentTimeMillis() - mCurrT);
        } catch (Exception ex) {
            PerformanceAnalyzerApp.ERRORS_AND_EXCEPTIONS_AGGREGATOR.updateStat(
                    ExceptionsAndErrors.SHARD_STATE_COLLECTOR_ERROR, "", 1);
            LOG.debug("Exception in Collecting Shard Metrics: {} for startTime {}", () -> ex.toString(),
                    () -> startTime);
        }
    }

    @SuppressWarnings("unchecked")
    private String createJsonObject(String key, String value) {
        JSONObject json = new JSONObject();
        json.put(key,value);
        return json.toString();
    }

    @Override
    public String getMetricsPath(long startTime, String... keysPath) {
        if (keysPath.length != KEYS_PATH_LENGTH) {
            throw new RuntimeException("keys length should be " + KEYS_PATH_LENGTH);
        }

        return PerformanceAnalyzerMetrics.generatePath(startTime, PerformanceAnalyzerMetrics.sShardStatePath);
    }

    public static class ShardStateMetrics extends MetricStatus {

        private final int shardId;
        private final String shardType;
        private final String nodeName;
        private final String shardState;

        public ShardStateMetrics(int shardId, String shardType, String nodeName, String shardState) {
            this.shardId = shardId;
            this.shardType = shardType;
            this.nodeName = nodeName;
            this.shardState = shardState;
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

        @JsonProperty(AllMetrics.ShardStateValue.Constants.SHARD_STATE)
        public String getShardState() {
            return shardState;
        }
    }

}
