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
import org.mockito.Mockito;

public class PerformanceAnalyzerConfigActionTests {
  private PerformanceAnalyzerConfigAction configAction;
  private RestController restController;
  private ThreadPool threadPool;
  private NodeClient nodeClient;
  private CircuitBreakerService circuitBreakerService;
  private ClusterSettings clusterSettings;

  @Mock private PerformanceAnalyzerController controller;

  @Before
  public void init() {
    initMocks(this);

    clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    circuitBreakerService = new HierarchyCircuitBreakerService(Settings.EMPTY, new ArrayList<BreakerSettings>(), clusterSettings);
    UsageService usageService = new UsageService();
    threadPool = new TestThreadPool("test");
    nodeClient = new NodeClient(Settings.EMPTY, threadPool);
    restController = new RestController(Collections.emptySet(), null, nodeClient, circuitBreakerService, usageService);
    configAction = new PerformanceAnalyzerConfigAction(restController, controller);
    restController.registerHandler(configAction);

    PerformanceAnalyzerConfigAction.setInstance(configAction);
    assertEquals(configAction, PerformanceAnalyzerConfigAction.getInstance());

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
    assertEquals(PerformanceAnalyzerConfigAction.PERFORMANCE_ANALYZER_CONFIG_ACTION, configAction.getName());
  }

  @Test
  public void testUpdateRcaState_ShouldEnable_paEnabled() throws IOException {
    test(PerformanceAnalyzerConfigAction.RCA_CONFIG_PATH, true, true);
  }

  @Test
  public void testUpdateRcaState_ShouldEnable_paDisabled() throws IOException {
    test(PerformanceAnalyzerConfigAction.RCA_CONFIG_PATH, true, false);
  }
  @Test
  public void testUpdateRcaState_ShouldDisable_paEnabled() throws IOException {
    test(PerformanceAnalyzerConfigAction.RCA_CONFIG_PATH, false, true);
  }

  @Test
  public void testUpdateRcaState_ShouldDisable_paDisabled() throws IOException {
    test(PerformanceAnalyzerConfigAction.RCA_CONFIG_PATH, false, false);
  }

  @Test
  public void testUpdateLoggingState_ShouldEnable_paEnabled() throws IOException {
    test(PerformanceAnalyzerConfigAction.LOGGING_CONFIG_PATH, true, true);
  }

  @Test
  public void testUpdateLoggingState_ShouldEnable_paDisabled() throws IOException {
    test(PerformanceAnalyzerConfigAction.LOGGING_CONFIG_PATH, true, false);
  }
  @Test
  public void testUpdateLoggingState_ShouldDisable_paEnabled() throws IOException {
    test(PerformanceAnalyzerConfigAction.LOGGING_CONFIG_PATH, false, true);
  }

  @Test
  public void testUpdateLoggingState_ShouldDisable_paDisabled() throws IOException {
    test(PerformanceAnalyzerConfigAction.LOGGING_CONFIG_PATH, false, false);
  }

  @Test
  public void testUpdateBatchMetricsState_ShouldEnable_paEnabled() throws IOException {
    test(PerformanceAnalyzerConfigAction.BATCH_METRICS_CONFIG_PATH, true, true);
  }

  @Test
  public void testUpdateBatchMetricsState_ShouldEnable_paDisabled() throws IOException {
    test(PerformanceAnalyzerConfigAction.BATCH_METRICS_CONFIG_PATH, true, false);
  }
  @Test
  public void testUpdateBatchMetricsState_ShouldDisable_paEnabled() throws IOException {
    test(PerformanceAnalyzerConfigAction.BATCH_METRICS_CONFIG_PATH, false, true);
  }

  @Test
  public void testUpdateBatchMetricsState_ShouldDisable_paDisabled() throws IOException {
    test(PerformanceAnalyzerConfigAction.BATCH_METRICS_CONFIG_PATH, false, false);
  }

  @Test
  public void testUpdatePerformanceAnalyzerState_ShouldEnable_paEnabled() throws IOException {
    test(PerformanceAnalyzerConfigAction.PA_CONFIG_PATH, true, true);
  }

  @Test
  public void testUpdatePerformanceAnalyzerState_ShouldDisable_paEnabled() throws IOException {
    test(PerformanceAnalyzerConfigAction.PA_CONFIG_PATH, false, true);
  }

  @Test
  public void testUpdatePerformanceAnalyzerState_ShouldDisable_paDisabled() throws IOException {
    test(PerformanceAnalyzerConfigAction.PA_CONFIG_PATH, false, false);
  }

  private void test(String requestPath, boolean shouldEnable, boolean paEnabled) throws IOException {
    final FakeRestRequest fakeRestRequest = buildRequest(requestPath, shouldEnable);
    final FakeRestChannel channel = new FakeRestChannel(fakeRestRequest, true, 10);
    Mockito.when(controller.isPerformanceAnalyzerEnabled()).thenReturn(paEnabled);
    restController.dispatchRequest(fakeRestRequest, channel, new ThreadContext(Settings.EMPTY));
    boolean testWithError = shouldEnable && !paEnabled;
    if (testWithError) {
      assertEquals(RestStatus.BAD_REQUEST, channel.capturedResponse().status());
    } else {
      assertEquals(RestStatus.OK, channel.capturedResponse().status());
      String responseStr = channel.capturedResponse().content().utf8ToString();
      assertTrue(responseStr.contains(PerformanceAnalyzerConfigAction.PA_ENABLED));
      assertTrue(responseStr.contains(PerformanceAnalyzerConfigAction.RCA_ENABLED));
      assertTrue(responseStr.contains(PerformanceAnalyzerConfigAction.PA_LOGGING_ENABLED));
      assertTrue(responseStr.contains(PerformanceAnalyzerConfigAction.SHARDS_PER_COLLECTION));
      assertTrue(responseStr.contains(PerformanceAnalyzerConfigAction.BATCH_METRICS_ENABLED));
      assertTrue(responseStr.contains(PerformanceAnalyzerConfigAction.BATCH_METRICS_RETENTION_PERIOD_MINUTES));
    }
  }

  private FakeRestRequest buildRequest(String requestPath, boolean shouldEnable) throws IOException {
    final XContentBuilder builder = XContentFactory.jsonBuilder()
        .startObject()
        .field(PerformanceAnalyzerConfigAction.ENABLED, shouldEnable)
        .field(PerformanceAnalyzerConfigAction.SHARDS_PER_COLLECTION, 1)
        .endObject();

    return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
        .withMethod(RestRequest.Method.POST)
        .withPath(requestPath)
        .withContent(BytesReference.bytes(builder), builder.contentType())
        .build();
  }



}
