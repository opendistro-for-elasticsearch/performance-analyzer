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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ESResources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.NodeDetailColumns;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.NodeRole;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.node.DiscoveryNode;

import java.util.Iterator;

public class NodeDetailsCollector extends PerformanceAnalyzerMetricsCollector implements MetricsProcessor {
    public static final int SAMPLING_TIME_INTERVAL = MetricsConfiguration.CONFIG_MAP.get(NodeDetailsCollector.class).samplingInterval;
    private static final Logger LOG = LogManager.getLogger(NodeDetailsCollector.class);
    private static final int KEYS_PATH_LENGTH = 0;

    public NodeDetailsCollector() {
        super(SAMPLING_TIME_INTERVAL, "NodeDetails");
    }

    @Override
    public void collectMetrics(long startTime) {
        if (ESResources.INSTANCE.getClusterService() == null
                || ESResources.INSTANCE.getClusterService()
                                       .state() == null
                || ESResources.INSTANCE.getClusterService()
                                       .state()
                                       .nodes() == null) {
            return;
        }

        StringBuilder value = new StringBuilder();
        value.append(PerformanceAnalyzerMetrics.getJsonCurrentMilliSeconds())
             .append(
                     PerformanceAnalyzerMetrics.sMetricNewLineDelimitor);

        Iterator<DiscoveryNode> discoveryNodeIterator = ESResources.INSTANCE.getClusterService()
                                                                            .state()
                                                                            .nodes()
                                                                            .iterator();
        addMetricsToStringBuilder(ESResources.INSTANCE.getClusterService()
                                                      .state()
                                                      .nodes()
                                                      .getLocalNode(), value, "");
        String localNodeID = ESResources.INSTANCE.getClusterService()
                                                 .state()
                                                 .nodes()
                                                 .getLocalNode()
                                                 .getId();

        while (discoveryNodeIterator.hasNext()) {
            addMetricsToStringBuilder(discoveryNodeIterator.next(), value, localNodeID);
        }
        saveMetricValues(value.toString(), startTime);
    }

    private void addMetricsToStringBuilder(DiscoveryNode discoveryNode,
                                           StringBuilder value, String localNodeID) {
        if (!discoveryNode.getId()
                          .equals(localNodeID)) {
            value.append(new NodeDetailsStatus(discoveryNode.getId(),
                                               discoveryNode.getHostAddress(), getNodeRole(discoveryNode)).serialize())
                 .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor);
        }
    }

    private String getNodeRole(final DiscoveryNode node) {
        final NodeRole role = node.isDataNode() ? NodeRole.DATA : node.isMasterNode() ? NodeRole.MASTER : NodeRole.UNKNOWN;
        return role.toString();
    }

    @Override
    public String getMetricsPath(long startTime, String... keysPath) {
        // throw exception if keys.length is not equal to 0
        if (keysPath.length != KEYS_PATH_LENGTH) {
            throw new RuntimeException("keys length should be " + KEYS_PATH_LENGTH);
        }

        return PerformanceAnalyzerMetrics.generatePath(startTime, PerformanceAnalyzerMetrics.sNodesPath);
    }

    public static class NodeDetailsStatus extends MetricStatus {
        private String id;

        private String hostAddress;

        private String role;

        public NodeDetailsStatus(String id, String hostAddress, String role) {
            super();
            this.id = id;
            this.hostAddress = hostAddress;
            this.role = role;
        }

        @JsonProperty(NodeDetailColumns.Constants.ID_VALUE)
        public String getID() {
            return id;
        }

        @JsonProperty(NodeDetailColumns.Constants.HOST_ADDRESS_VALUE)
        public String getHostAddress() {
            return hostAddress;
        }

        @JsonProperty(NodeDetailColumns.Constants.ROLE_VALUE)
        public String getRole() {
            return role;
        }
    }
}

