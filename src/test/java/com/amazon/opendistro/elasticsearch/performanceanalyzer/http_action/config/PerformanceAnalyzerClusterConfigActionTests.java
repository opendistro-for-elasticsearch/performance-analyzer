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
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.handler.NodeStatsSettingHandler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.handler.PerformanceAnalyzerClusterSettingHandler;
import java.io.IOException;
import java.util.List;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.RestHandler.Route;
import org.elasticsearch.rest.RestRequest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

public class PerformanceAnalyzerClusterConfigActionTests {
  private PerformanceAnalyzerClusterConfigAction configAction;

  @Mock
  private PerformanceAnalyzerClusterSettingHandler clusterSettingHandler;

  @Mock
  private NodeStatsSettingHandler nodeStatsSettingHandler;

  @Mock
  private NodeClient client;

  @Mock
  private RestRequest restRequest;

  @Before
  public void init() {
    initMocks(this);
    configAction = new PerformanceAnalyzerClusterConfigAction(null, null, clusterSettingHandler, nodeStatsSettingHandler);
  }

  @Test
  public void testPrepareRequest() throws IOException {
    Mockito.when(restRequest.method()).thenReturn(RestRequest.Method.POST);
    final XContentBuilder builder = XContentFactory.jsonBuilder()
        .startObject()
        .field(PerformanceAnalyzerClusterConfigAction.ENABLED, true)
        .field(PerformanceAnalyzerClusterConfigAction.SHARDS_PER_COLLECTION, 1)
        .endObject();
    Mockito.when(restRequest.content()).thenReturn(BytesReference.bytes(builder));

    //verify updateRcaSetting
    Mockito.when(restRequest.path()).thenReturn(PerformanceAnalyzerClusterConfigAction.RCA_CLUSTER_CONFIG_PATH);
    configAction.prepareRequest(restRequest, client);
    verify(clusterSettingHandler).updateRcaSetting(true);
    verify(nodeStatsSettingHandler).updateNodeStatsSetting(1);

    //verify updateLogginSetting
    Mockito.when(restRequest.path()).thenReturn(PerformanceAnalyzerClusterConfigAction.LOGGING_CLUSTER_CONFIG_PATH);
    configAction.prepareRequest(restRequest, client);
    verify(clusterSettingHandler).updateLoggingSetting(true);

    //verify updateBatchMetricsSetting
    Mockito.when(restRequest.path()).thenReturn(PerformanceAnalyzerClusterConfigAction.BATCH_METRICS_CLUSTER_CONFIG_PATH);
    configAction.prepareRequest(restRequest, client);
    verify(clusterSettingHandler).updateBatchMetricsSetting(true);

    //verify updatePerformanceAnalyzerSetting
    Mockito.when(restRequest.path()).thenReturn(null);
    configAction.prepareRequest(restRequest, client);
    verify(clusterSettingHandler).updatePerformanceAnalyzerSetting(true);
  }

  @Test
  public void testRoutes() {
    List<Route> routes = configAction.routes();
    assertEquals(8, routes.size());
  }
}
