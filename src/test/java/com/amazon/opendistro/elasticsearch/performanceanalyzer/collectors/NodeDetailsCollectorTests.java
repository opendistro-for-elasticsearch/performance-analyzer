/*
 * Copyright <2020> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import static org.mockito.MockitoAnnotations.initMocks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ESResources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.NodeDetailsCollector.NodeDetailsStatus;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides.ConfigOverridesWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.NodeRole;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.TestUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.paranamer.ParanamerModule;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class NodeDetailsCollectorTests extends ESTestCase {
  private static final String NODE_ID = "testNode";
  private NodeDetailsCollector collector;
  private ThreadPool threadPool;
  private long startTimeInMills = 1153721339;

  @Mock
  private ConfigOverridesWrapper configOverrides;

  @Before
  public void init() {
    initMocks(this);

    DiscoveryNode testNode = new DiscoveryNode(NODE_ID, ESTestCase.buildNewFakeTransportAddress(), Collections
        .emptyMap(),
        DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);

    threadPool = new TestThreadPool("test");
    ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool, testNode);
    ESResources.INSTANCE.setClusterService(clusterService);

    MetricsConfiguration.CONFIG_MAP.put(NodeDetailsCollector.class, MetricsConfiguration.cdefault);
    collector = new NodeDetailsCollector(configOverrides);
  }

  @After
  public void tearDown() throws Exception {
    threadPool.shutdownNow();
    super.tearDown();
  }

  @Test
  public void testGetMetricsPath() {
    String expectedPath = PluginSettings.instance().getMetricsLocation()
        + PerformanceAnalyzerMetrics.getTimeInterval(startTimeInMills)+ "/" + PerformanceAnalyzerMetrics.sNodesPath;
    String actualPath = collector.getMetricsPath(startTimeInMills);
    assertEquals(expectedPath, actualPath);

    try {
      collector.getMetricsPath(startTimeInMills, "nodesPath");
      assertTrue("Negative scenario test: Should have been a RuntimeException", true);
    } catch (RuntimeException ex) {
      //- expecting exception...1 values passed; 0 expected
    }
  }

  @Test
  public void testCollectMetrics() throws IOException {
    long startTimeInMills = 1153721339;
    collector.collectMetrics(startTimeInMills);
    NodeDetailsStatus nodeDetailsStatus = readMetrics();

    assertEquals(NODE_ID, nodeDetailsStatus.getID());
    assertEquals("0.0.0.0", nodeDetailsStatus.getHostAddress());
    assertEquals(NodeRole.DATA.role(), nodeDetailsStatus.getRole());
    assertTrue(nodeDetailsStatus.getIsMasterNode());
  }

  private NodeDetailsStatus readMetrics() throws IOException {
    List<Event> metrics = TestUtil.readEvents();
    assert metrics.size() == 1;
    ObjectMapper objectMapper = new ObjectMapper().registerModule(new ParanamerModule());
    String[] jsonStrs = metrics.get(0).value.split("\n");
    assert jsonStrs.length == 4;
    return objectMapper.readValue(jsonStrs[3], NodeDetailsStatus.class);
  }
}
