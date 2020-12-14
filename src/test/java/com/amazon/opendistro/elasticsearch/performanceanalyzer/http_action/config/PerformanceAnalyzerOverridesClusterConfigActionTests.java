package com.amazon.opendistro.elasticsearch.performanceanalyzer.http_action.config;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.http_action.config.PerformanceAnalyzerOverridesClusterConfigAction.PA_CONFIG_OVERRIDES_PATH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.MockitoAnnotations.initMocks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.ConfigOverridesTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides.ConfigOverridesWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.handler.ConfigOverridesClusterSettingHandler;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.indices.breaker.BreakerSettings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler.Route;
import org.elasticsearch.rest.RestRequest.Method;
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

public class PerformanceAnalyzerOverridesClusterConfigActionTests {
  private PerformanceAnalyzerOverridesClusterConfigAction configAction;
  private RestController restController;
  private ThreadPool threadPool;
  private NodeClient nodeClient;
  private CircuitBreakerService circuitBreakerService;
  private ClusterSettings clusterSettings;

  @Mock private ConfigOverridesClusterSettingHandler configOverridesClusterSettingHandler;
  @Mock private ConfigOverridesWrapper overridesWrapper;

  @Before
  public void init() {
    initMocks(this);

    clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    circuitBreakerService = new HierarchyCircuitBreakerService(Settings.EMPTY, new ArrayList<BreakerSettings>(), clusterSettings);
    UsageService usageService = new UsageService();
    threadPool = new TestThreadPool("test");
    nodeClient = new NodeClient(Settings.EMPTY, threadPool);
    restController = new RestController(Collections.emptySet(), null, nodeClient, circuitBreakerService, usageService);
    configAction = new PerformanceAnalyzerOverridesClusterConfigAction(Settings.EMPTY, restController, configOverridesClusterSettingHandler, overridesWrapper);
    restController.registerHandler(configAction);
  }

  @After
  public void tearDown() throws Exception {
    threadPool.shutdownNow();
  }

  @Test
  public void testRoutes() {
    List<Route> routes = configAction.routes();
    assertEquals(2, routes.size());
  }

  @Test
  public void testGetName() {
    assertEquals(PerformanceAnalyzerOverridesClusterConfigAction.class.getSimpleName(), configAction.getName());
  }

  @Test
  public void testWithGetMethod() throws IOException {
    final FakeRestRequest fakeRestRequest = buildRequest(
        PA_CONFIG_OVERRIDES_PATH,
        Method.GET,
        ConfigOverridesTestHelper.getValidConfigOverridesJson());
    final FakeRestChannel channel = new FakeRestChannel(fakeRestRequest, true, 10);
    restController.dispatchRequest(fakeRestRequest, channel, new ThreadContext(Settings.EMPTY));
    assertEquals(RestStatus.OK, channel.capturedResponse().status());
  }

  @Test
  public void testWithPostMethod() throws IOException {
    final FakeRestRequest fakeRestRequest = buildRequest(
        PA_CONFIG_OVERRIDES_PATH,
        Method.POST,
        ConfigOverridesTestHelper.getValidConfigOverridesJson());
    final FakeRestChannel channel = new FakeRestChannel(fakeRestRequest, true, 10);
    restController.dispatchRequest(fakeRestRequest, channel, new ThreadContext(Settings.EMPTY));
    assertEquals(RestStatus.OK, channel.capturedResponse().status());
    String responseStr = channel.capturedResponse().content().utf8ToString();
    assertTrue(responseStr.contains(PerformanceAnalyzerOverridesClusterConfigAction.OVERRIDE_TRIGGERED_FIELD));
  }

  @Test
  public void testWithUnsupportedMethod() throws IOException {
    final FakeRestRequest fakeRestRequest = buildRequest(
        PA_CONFIG_OVERRIDES_PATH,
        Method.PUT,
        ConfigOverridesTestHelper.getValidConfigOverridesJson());
    final FakeRestChannel channel = new FakeRestChannel(fakeRestRequest, true, 10);
    restController.dispatchRequest(fakeRestRequest, channel, new ThreadContext(Settings.EMPTY));
    assertEquals(RestStatus.METHOD_NOT_ALLOWED, channel.capturedResponse().status());
  }

  @Test
  public void testWithInvalidOverrides() throws IOException {
    final FakeRestRequest fakeRestRequest = buildRequest(
        PA_CONFIG_OVERRIDES_PATH,
        Method.POST,
        ConfigOverridesTestHelper.getInvalidConfigOverridesJson());
    final FakeRestChannel channel = new FakeRestChannel(fakeRestRequest, true, 10);
    restController.dispatchRequest(fakeRestRequest, channel, new ThreadContext(Settings.EMPTY));
    assertEquals(RestStatus.BAD_REQUEST, channel.capturedResponse().status());
  }

  private FakeRestRequest buildRequest(String requestPath, Method requestMethod, String configOverridesJson) throws IOException {
    ByteBuffer byteBuffer = ByteBuffer.wrap(configOverridesJson.getBytes());
    return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
        .withMethod(requestMethod)
        .withPath(requestPath)
        .withContent(BytesReference.fromByteBuffer(byteBuffer), XContentType.JSON)
        .build();
  }
}
