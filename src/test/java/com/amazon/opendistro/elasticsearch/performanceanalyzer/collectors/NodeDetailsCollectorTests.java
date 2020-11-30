package com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ESResources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.NodeDetailsCollector.NodeDetailsStatus;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides.ConfigOverridesWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
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
import org.mockito.Mockito;

public class NodeDetailsCollectorTests extends ESTestCase {
  private static final String NODE_ID = "testNode";
  private NodeDetailsCollector collector;
  private ConfigOverridesWrapper configOverrides;
  private ThreadPool threadPool;

  @Before
  public void init() {
    DiscoveryNode testNode = new DiscoveryNode(NODE_ID, ESTestCase.buildNewFakeTransportAddress(), Collections
        .emptyMap(),
        DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);

    threadPool = new TestThreadPool("test");
    ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool, testNode);
    ESResources.INSTANCE.setClusterService(clusterService);

    MetricsConfiguration.CONFIG_MAP.put(NodeDetailsCollector.class, MetricsConfiguration.cdefault);
    configOverrides = Mockito.mock(ConfigOverridesWrapper.class);
    collector = new NodeDetailsCollector(configOverrides);
  }

  @After
  public void tearDown() throws Exception {
    threadPool.shutdownNow();
    super.tearDown();
  }

  @Test
  public void testCollectMetrics() throws IOException {
    long startTimeInMills = 1153721339;
    collector.collectMetrics(startTimeInMills);
    NodeDetailsStatus nodeDetailsStatus = readMetrics();

    assertEquals(NODE_ID, nodeDetailsStatus.getID());
    assertEquals("0.0.0.0", nodeDetailsStatus.getHostAddress());
    assertEquals("DATA", nodeDetailsStatus.getRole());
    assertEquals(true, nodeDetailsStatus.getIsMasterNode());
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