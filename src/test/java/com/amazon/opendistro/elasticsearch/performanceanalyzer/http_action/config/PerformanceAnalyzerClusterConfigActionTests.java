/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.http_action.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.MockitoAnnotations.initMocks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PerformanceAnalyzerController;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.ClusterSettingsManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.handler.NodeStatsSettingHandler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.handler.PerformanceAnalyzerClusterSettingHandler;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler.Route;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.usage.UsageService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class PerformanceAnalyzerClusterConfigActionTests {
  private PerformanceAnalyzerClusterConfigAction configAction;
  private RestController restController;
  private ThreadPool threadPool;
  private NodeClient nodeClient;
  private CircuitBreakerService circuitBreakerService;
  private ClusterSettings clusterSettings;
  private PerformanceAnalyzerClusterSettingHandler clusterSettingHandler;
  private NodeStatsSettingHandler nodeStatsSettingHandler;

  @Mock private PerformanceAnalyzerController controller;
  @Mock private ClusterSettingsManager clusterSettingsManager;

  @Before
  public void init() {
    initMocks(this);

    clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    circuitBreakerService = new HierarchyCircuitBreakerService(Settings.EMPTY, new ArrayList<BreakerSettings>(), clusterSettings);
    UsageService usageService = new UsageService();
    threadPool = new TestThreadPool("test");
    nodeClient = new NodeClient(Settings.EMPTY, threadPool);
    restController = new RestController(Collections.emptySet(), null, nodeClient, circuitBreakerService, usageService);
    clusterSettingHandler = new PerformanceAnalyzerClusterSettingHandler(controller, clusterSettingsManager);
    nodeStatsSettingHandler = new NodeStatsSettingHandler(controller, clusterSettingsManager);
    configAction = new PerformanceAnalyzerClusterConfigAction(Settings.EMPTY, restController, clusterSettingHandler, nodeStatsSettingHandler);
    restController.registerHandler(configAction);
  }

  @After
  public void tearDown() throws Exception {
    threadPool.shutdownNow();
  }

  @Test
  public void testRoutes() {
    List<Route> routes = configAction.routes();
    assertEquals(8, routes.size());
  }

  @Test
  public void testGetName() {
    assertEquals(PerformanceAnalyzerClusterConfigAction.class.getSimpleName(), configAction.getName());
  }

  @Test
  public void testUpdateRcaSetting() throws IOException {
    test(PerformanceAnalyzerClusterConfigAction.RCA_CLUSTER_CONFIG_PATH);
  }

  @Test
  public void testUpdateLoggingSetting() throws IOException {
    test(PerformanceAnalyzerClusterConfigAction.LOGGING_CLUSTER_CONFIG_PATH);
  }

  @Test
  public void testUpdateBatchMetricsSetting() throws IOException {
    test(PerformanceAnalyzerClusterConfigAction.BATCH_METRICS_CLUSTER_CONFIG_PATH);
  }

  @Test
  public void testUpdatePerformanceAnalyzerSetting() throws IOException {
    test(PerformanceAnalyzerClusterConfigAction.PA_CLUSTER_CONFIG_PATH);
  }

  private void test(String requestPath) throws IOException {
    final FakeRestRequest fakeRestRequest = buildRequest(requestPath);
    final FakeRestChannel channel = new FakeRestChannel(fakeRestRequest, true, 10);
    restController.dispatchRequest(fakeRestRequest, channel, new ThreadContext(Settings.EMPTY));
    assertEquals(RestStatus.OK, channel.capturedResponse().status());

    String responseStr = channel.capturedResponse().content().utf8ToString();
    assertTrue(responseStr.contains(PerformanceAnalyzerClusterConfigAction.CURRENT));
    assertTrue(responseStr.contains(PerformanceAnalyzerClusterConfigAction.SHARDS_PER_COLLECTION));
    assertTrue(responseStr.contains(PerformanceAnalyzerClusterConfigAction.BATCH_METRICS_RETENTION_PERIOD_MINUTES));
  }

  private FakeRestRequest buildRequest(String requestPath) throws IOException {
    final XContentBuilder builder = XContentFactory.jsonBuilder()
        .startObject()
        .field(PerformanceAnalyzerClusterConfigAction.ENABLED, true)
        .field(PerformanceAnalyzerClusterConfigAction.SHARDS_PER_COLLECTION, 1)
        .endObject();

    return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withMethod(RestRequest.Method.POST)
            .withPath(requestPath)
            .withContent(BytesReference.bytes(builder), builder.contentType())
            .build();
  }
}
