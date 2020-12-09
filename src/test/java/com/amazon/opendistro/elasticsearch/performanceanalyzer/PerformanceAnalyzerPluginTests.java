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

package com.amazon.opendistro.elasticsearch.performanceanalyzer;

import static org.mockito.MockitoAnnotations.initMocks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.action.PerformanceAnalyzerActionFilter;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.PerformanceAnalyzerClusterSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.http_action.config.PerformanceAnalyzerClusterConfigAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.http_action.config.PerformanceAnalyzerConfigAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.http_action.config.PerformanceAnalyzerOverridesClusterConfigAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.http_action.config.PerformanceAnalyzerResourceProvider;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.transport.PerformanceAnalyzerTransportInterceptor;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.plugins.ActionPlugin.ActionHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.usage.UsageService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

@ThreadLeakScope(Scope.NONE)
public class PerformanceAnalyzerPluginTests extends ESTestCase {
  private PerformanceAnalyzerPlugin plugin;
  private Settings settings;
  private RestController restController;
  private ThreadPool threadPool;
  private NodeClient nodeClient;
  private Environment environment;
  private CircuitBreakerService circuitBreakerService;
  private ClusterService clusterService;
  private ClusterSettings clusterSettings;

  @Mock
  private Discovery discovery;

  @Before
  public void setup() {
    initMocks(this);

    settings = Settings.builder().put("path.home", "./").build();
    plugin = new PerformanceAnalyzerPlugin(settings, Paths.get("build/tmp/junit_metrics"));
    clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    circuitBreakerService = new HierarchyCircuitBreakerService(settings, new ArrayList<BreakerSettings>(), clusterSettings);
    UsageService usageService = new UsageService();
    threadPool = new TestThreadPool("test");
    nodeClient = new NodeClient(settings, threadPool);
    environment = TestEnvironment.newEnvironment(settings);
    clusterService = new ClusterService(settings, clusterSettings, threadPool);
    restController = new RestController(Collections.emptySet(), null, nodeClient, circuitBreakerService, usageService);
  }

  @After
  public void tearDown() throws Exception {
    threadPool.shutdownNow();
    super.tearDown();
  }

  @Test
  public void testGetActionFilters() {
    List<ActionFilter> list = plugin.getActionFilters();
    assertEquals(1, list.size());
    assertEquals(PerformanceAnalyzerActionFilter.class, list.get(0).getClass());
  }

  @Test
  public void testGetActions() {
    List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> list = plugin.getActions();
    assertEquals(1, list.size());
    assertEquals(ActionHandler.class, list.get(0).getClass());
  }

  @Test
  public void testGetTransportInterceptors() {
    List<TransportInterceptor> list = plugin.getTransportInterceptors(null, null);
    assertEquals(1, list.size());
    assertEquals(PerformanceAnalyzerTransportInterceptor.class, list.get(0).getClass());
  }

  @Test
  public void testGetRestHandlers() {
    List<RestHandler> handlers = plugin.getRestHandlers(settings, restController, null,
        null, null, null, null);
    assertEquals(4, handlers.size());
    assertEquals(PerformanceAnalyzerConfigAction.class, handlers.get(0).getClass());
    assertEquals(PerformanceAnalyzerClusterConfigAction.class, handlers.get(1).getClass());
    assertEquals(PerformanceAnalyzerResourceProvider.class, handlers.get(2).getClass());
    assertEquals(PerformanceAnalyzerOverridesClusterConfigAction.class, handlers.get(3).getClass());
  }

  @Test
  public void testCreateComponents() {
    Collection<Object> components = plugin.createComponents(
        nodeClient, clusterService, threadPool, null,null,
        null, environment, null,null, null,null);
    assertEquals(1, components.size());
    assertEquals(settings, ESResources.INSTANCE.getSettings());
    assertEquals(threadPool, ESResources.INSTANCE.getThreadPool());
    assertEquals(environment, ESResources.INSTANCE.getEnvironment());
    assertEquals(nodeClient, ESResources.INSTANCE.getClient());
  }

  @Test
  public void testGetTransports() {
    Map<String, Supplier<Transport>> map = plugin.getTransports(settings, threadPool, null, circuitBreakerService, null, null);
    assertEquals(0, map.size());
    assertEquals(settings, ESResources.INSTANCE.getSettings());
    assertEquals(circuitBreakerService, ESResources.INSTANCE.getCircuitBreakerService());
  }

  @Test
  public void testGetSettings() {
    List<Setting<?>> list = plugin.getSettings();
    assertEquals(3, list.size());
    assertEquals(PerformanceAnalyzerClusterSettings.COMPOSITE_PA_SETTING, list.get(0));
    assertEquals(PerformanceAnalyzerClusterSettings.PA_NODE_STATS_SETTING, list.get(1));
    assertEquals(PerformanceAnalyzerClusterSettings.CONFIG_OVERRIDES_SETTING, list.get(2));
  }
}
