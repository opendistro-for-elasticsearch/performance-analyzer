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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides.ConfigOverridesWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.NodeDetailColumns;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.NodeRole;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;

import java.io.IOException;
import java.util.Iterator;

public class NodeDetailsCollector extends PerformanceAnalyzerMetricsCollector implements MetricsProcessor {
    public static final int SAMPLING_TIME_INTERVAL = MetricsConfiguration.CONFIG_MAP.get(NodeDetailsCollector.class).samplingInterval;
    private static final Logger LOG = LogManager.getLogger(NodeDetailsCollector.class);
    private static final int KEYS_PATH_LENGTH = 0;
    private final ConfigOverridesWrapper configOverridesWrapper;

    public NodeDetailsCollector() {
        this(null);
    }

    public NodeDetailsCollector(final ConfigOverridesWrapper configOverridesWrapper) {
        super(SAMPLING_TIME_INTERVAL, "NodeDetails");
        this.configOverridesWrapper = configOverridesWrapper;
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

        // We add the config overrides in line#2 because we don't know how many lines
        // follow that belong to actual node details, and the reader also has no way to
        // know this information in advance unless we add the number of nodes as
        // additional metadata in the file.
        try {
            String rcaOverrides = configOverridesWrapper.serialize(configOverridesWrapper.getCurrentClusterConfigOverrides());
            value.append(rcaOverrides);
        } catch (IOException ioe) {
            LOG.error("Unable to serialize rca config overrides.", ioe);
        }
        value.append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor);
        
        // line#3 denotes when the timestamp when the config override happened.
        value.append(configOverridesWrapper.getLastUpdatedTimestamp());
        value.append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor);

        DiscoveryNodes discoveryNodes = ESResources.INSTANCE.getClusterService().state().nodes();

        DiscoveryNode masterNode = discoveryNodes.getMasterNode();

        Iterator<DiscoveryNode> discoveryNodeIterator = discoveryNodes.iterator();
        addMetricsToStringBuilder(discoveryNodes.getLocalNode(), value, "", masterNode);
        String localNodeID = discoveryNodes.getLocalNode().getId();

        while (discoveryNodeIterator.hasNext()) {
            addMetricsToStringBuilder(discoveryNodeIterator.next(), value, localNodeID, masterNode);
        }
        saveMetricValues(value.toString(), startTime);
    }

    private void addMetricsToStringBuilder(DiscoveryNode discoveryNode,
                                           StringBuilder value, String localNodeID, DiscoveryNode masterNode) {
        if (!discoveryNode.getId()
                          .equals(localNodeID)) {
            boolean isMasterNode = discoveryNode.equals(masterNode);
            value.append(new NodeDetailsStatus(discoveryNode.getId(),
                                               discoveryNode.getHostAddress(), getNodeRole(discoveryNode), isMasterNode).serialize())
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

        private boolean isMasterNode;

        public NodeDetailsStatus(String id, String hostAddress, String role, boolean isMasterNode) {
            super();
            this.id = id;
            this.hostAddress = hostAddress;
            this.role = role;
            this.isMasterNode = isMasterNode;
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

        @JsonProperty(NodeDetailColumns.Constants.IS_MASTER_NODE)
        public boolean getIsMasterNode() {
            return isMasterNode;
        }
    }
}

