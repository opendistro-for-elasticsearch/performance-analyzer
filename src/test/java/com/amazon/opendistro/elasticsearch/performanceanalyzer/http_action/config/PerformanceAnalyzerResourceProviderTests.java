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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.http_action.config;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.MockitoAnnotations.initMocks;


@RunWith(PowerMockRunner.class)
@PrepareForTest({PluginSettings.class})
@SuppressStaticInitializationFor({"PluginSettings"})
public class PerformanceAnalyzerResourceProviderTests {
  @Mock
  RestController mockRestController;
  PerformanceAnalyzerResourceProvider performanceAnalyzerRp;

  @Before
  public void setup() {
    initMocks(this);
    initPerformanceAnalyzerResourceProvider(false);
  }

  private void initPerformanceAnalyzerResourceProvider(boolean isHttpsEnabled) {
    PluginSettings config = Mockito.mock(PluginSettings.class);
    Mockito.when(config.getHttpsEnabled()).thenReturn(isHttpsEnabled);

    PowerMockito.mockStatic(PluginSettings.class);
    PowerMockito.when(PluginSettings.instance()).thenReturn(config);

    performanceAnalyzerRp = new PerformanceAnalyzerResourceProvider(Settings.EMPTY, mockRestController);
    performanceAnalyzerRp.setPortNumber("9650");
  }

  private RestRequest generateRestRequest(String requestUri, String redirectEndpoint) {
    RestRequest request = new RestRequest(null, new HashMap<>(), requestUri, Collections.emptyMap(), null, null) {
      @Override
      public Method method() {
        return Method.GET;
      }

      @Override
      public String uri() {
        return requestUri;
      }

      @Override
      public boolean hasContent() {
        return false;
      }

      @Override
      public BytesReference content() {
        return null;
      }
    };
    request.params().put("redirectEndpoint", redirectEndpoint);
    return request;
  }


  private void assertAgentUriWithMetricsRedirection(final String protocolScheme) throws IOException {
    initPerformanceAnalyzerResourceProvider(protocolScheme.equals("https://"));

    String requestURI = protocolScheme + "localhost:9200/_opendistro/_performanceanalyzer/_agent/metrics" +
            "?metrics=Latency,CPU_Utilization&agg=avg,max&dim=ShardID&nodes=all";
    String expectedResponseURI = protocolScheme + "localhost:9650/_opendistro/_performanceanalyzer/metrics" +
            "?metrics=Latency,CPU_Utilization&agg=avg,max&dim=ShardID&nodes=all";

    RestRequest restRequest = generateRestRequest(requestURI, "metrics");
    URL actualResponseURI = performanceAnalyzerRp.getAgentUri(restRequest);
    assertEquals(new URL(expectedResponseURI), actualResponseURI);
  }


  private void assertAgentUriWithRcaRedirection(final String protocolScheme) throws IOException {
    initPerformanceAnalyzerResourceProvider(protocolScheme.equals("https://"));

    String requestUri = protocolScheme + "localhost:9200/_opendistro/_performanceanalyzer/_agent/rca" +
            "?rca=highShardCPU&startTime=2019-10-11";
    String expectedResponseUri = protocolScheme + "localhost:9650/_opendistro/_performanceanalyzer/rca" +
            "?rca=highShardCPU&startTime=2019-10-11";

    RestRequest restRequest = generateRestRequest(requestUri, "rca");
    URL actualResponseURI = performanceAnalyzerRp.getAgentUri(restRequest);
    assertEquals(new URL(expectedResponseUri), actualResponseURI);
  }


  private void assertAgentUriWithBatchRedirection(final String protocolScheme) throws IOException {
    initPerformanceAnalyzerResourceProvider(protocolScheme.equals("https://"));

    String requestUri = protocolScheme + "localhost:9200/_opendistro/_performanceanalyzer/_agent/batch" +
            "?metrics=CPU_Utilization,IO_TotThroughput&starttime=1594412650000&endtime=1594412665000&samplingperiod=5";
    String expectedResponseUri = protocolScheme + "localhost:9650/_opendistro/_performanceanalyzer/batch" +
            "?metrics=CPU_Utilization,IO_TotThroughput&starttime=1594412650000&endtime=1594412665000&samplingperiod=5";

    RestRequest restRequest = generateRestRequest(requestUri, "batch");
    URL actualResponseURI = performanceAnalyzerRp.getAgentUri(restRequest);
    assertEquals(new URL(expectedResponseUri), actualResponseURI);
  }

  private void assertAgentUriWithActionsRedirection(final String protocolScheme) throws IOException {
    initPerformanceAnalyzerResourceProvider(protocolScheme.equals("https://"));

    String requestUri = protocolScheme + "localhost:9200/_opendistro/_performanceanalyzer/_agent/actions";
    String expectedResponseUri = protocolScheme + "localhost:9650/_opendistro/_performanceanalyzer/actions";

    RestRequest restRequest = generateRestRequest(requestUri, "actions");
    URL actualResponseURI = performanceAnalyzerRp.getAgentUri(restRequest);
    assertEquals(new URL(expectedResponseUri), actualResponseURI);
  }


  @Test
  public void testGetAgentUri_WithHttp_WithMetricRedirection() throws Exception {
    assertAgentUriWithMetricsRedirection("http://");
  }

  @Test
  public void testGetAgentUri_WithHttps_WithMetricRedirection() throws Exception {
    assertAgentUriWithRcaRedirection("https://");
  }

  @Test
  public void testGetAgentUri_WithHttp_WithRcaRedirection() throws Exception {
    assertAgentUriWithRcaRedirection("http://");
  }

  @Test
  public void testGetAgentUri_WithHttps_WithRcaRedirection() throws Exception {
    assertAgentUriWithRcaRedirection("https://");
  }

  @Test
  public void testGetAgentUri_WithHttp_WithBatchRedirection() throws Exception {
    assertAgentUriWithBatchRedirection("http://");
  }

  @Test
  public void testGetAgentUri_WithHttps_WithBatchRedirection() throws Exception {
    assertAgentUriWithBatchRedirection("https://");
  }

  @Test
  public void testGetAgentUri_WithHttp_WithActionsRedirection() throws Exception {
    assertAgentUriWithActionsRedirection("http://");
  }

  @Test
  public void testGetAgentUri_WithHttps_WithActionsRedirection() throws Exception {
    assertAgentUriWithActionsRedirection("https://");
  }

  @Test
  public void testGetAgentUri_WithHttp_WithUnsupportedRedirection() throws Exception {
    String requestUri = "http://localhost:9200/_opendistro/_performanceanalyzer/_agent/invalid";
    RestRequest request = generateRestRequest(requestUri, "invalid");
    URL finalURI = performanceAnalyzerRp.getAgentUri(request);
    assertNull(finalURI);
  }
}