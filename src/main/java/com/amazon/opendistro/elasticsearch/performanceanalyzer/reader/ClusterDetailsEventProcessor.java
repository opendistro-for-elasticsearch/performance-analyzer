/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.reader;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.JsonConverter;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ClusterDetailsEventProcessor implements EventProcessor {
  private static final Logger LOG = LogManager.getLogger(ClusterDetailsEventProcessor.class);
  /**
   * keep a volatile immutable list to make the read/write to this list thread safe.
   */
  private static volatile ImmutableList<NodeDetails> nodesDetails = null;

  @Override
  public void initializeProcessing(long startTime, long endTime) {}

  @Override
  public void finalizeProcessing() {}

  @Override
  public void processEvent(Event event) {
    String[] lines = event.value.split(System.lineSeparator());
    if (lines.length < 2) {
      // We expect at-least 2 lines as the first line is always timestamp
      // and there must be at least one ElasticSearch node in a cluster.
      LOG.error(
          "ClusterDetails contain less items than expected. " + "Expected 2, found: {}",
          event.value);
      return;
    }
    // An example node_metrics data is something like this for a two node cluster:
    // {"current_time":1566414001749}
    // {"ID":"4sqG_APMQuaQwEW17_6zwg","HOST_ADDRESS":"10.212.73.121"}
    // {"ID":"OVH94mKXT5ibeqvDoAyTeg","HOST_ADDRESS":"10.212.78.83"}
    //
    // The line 0 is timestamp that can be skipped. So we allocated size of
    // the array is one less than the list.
    final List<NodeDetails> tmpNodesDetails = new ArrayList<>();

    // Just to keep track of duplicate node ids.
    Set<String> ids = new HashSet<>();

    for (int i = 1; i < lines.length; ++i) {
      NodeDetails nodeDetails = new NodeDetails(lines[i]);

      // Include nodeIds we haven't seen so far.
      if (ids.add(nodeDetails.getId())) {
        tmpNodesDetails.add(nodeDetails);
      } else {
        LOG.info("node id {}, logged twice.", nodeDetails.getId());
      }
    }
    setNodesDetails(tmpNodesDetails);
  }

  @Override
  public boolean shouldProcessEvent(Event event) {
    return event.key.contains(PerformanceAnalyzerMetrics.sNodesPath);
  }

  @Override
  public void commitBatchIfRequired() {

  }

  public static void setNodesDetails(List<NodeDetails> nodesDetails) {
    ClusterDetailsEventProcessor.nodesDetails = ImmutableList.copyOf(nodesDetails);
  }

  public static List<NodeDetails> getNodesDetails() {
    if (nodesDetails != null) {
      return nodesDetails.asList();
    } else {
      return Collections.emptyList();
    }
  }

  public static List<NodeDetails> getDataNodesDetails() {
    List<NodeDetails> allNodes = getNodesDetails();
    if (allNodes.size() > 0) {
      return allNodes.stream()
          .filter(p -> p.getRole().equals(AllMetrics.NodeRole.DATA.toString()))
          .collect(Collectors.toList());
    } else {
      return Collections.emptyList();
    }
  }

  public static NodeDetails getCurrentNodeDetails() {
    List<NodeDetails> allNodes = getNodesDetails();
    if (allNodes.size() > 0) {
      return allNodes.get(0);
    } else {
      return null;
    }
  }

  public static class NodeDetails {

    private String id;
    private String hostAddress;
    private String role;
    private Boolean isMasterNode;

    NodeDetails(String stringifiedMetrics) {
      Map<String, Object> map = JsonConverter
          .createMapFrom(stringifiedMetrics);
      id = (String) map.get(AllMetrics.NodeDetailColumns.ID.toString());
      hostAddress = (String) map.get(AllMetrics.NodeDetailColumns.HOST_ADDRESS.toString());
      role = (String) map.get(AllMetrics.NodeDetailColumns.ROLE.toString());
      Object isMasterNodeObject = map.get(AllMetrics.NodeDetailColumns.IS_MASTER_NODE.toString());
      isMasterNode = isMasterNodeObject != null ? (Boolean) isMasterNodeObject : null;
    }

    @Override
    public String toString() {
      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append("{")
          .append("id:")
          .append(id)
          .append(" hostAddress:")
          .append(hostAddress)
          .append(" role:")
          .append(role)
          .append(" isMasterNode:")
          .append(isMasterNode)
          .append("}");
      return stringBuilder.toString();
    }

    public String getId() {
      return id;
    }

    public String getHostAddress() {
      return hostAddress;
    }

    public String getRole() {
      return role;
    }
  }
}
