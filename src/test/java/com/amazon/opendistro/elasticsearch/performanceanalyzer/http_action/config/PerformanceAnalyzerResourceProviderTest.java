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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.MockitoAnnotations.initMocks;

import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;


public class PerformanceAnalyzerResourceProviderTest {
  @Mock
  RestController mockRestController;
  PerformanceAnalyzerResourceProvider performanceAnalyzerRp ;

  @Before
  public void setup(){
    initMocks(this);
    performanceAnalyzerRp = new PerformanceAnalyzerResourceProvider(Settings.EMPTY, mockRestController);
  }

  @Test
  public void getAgentUriTest() throws Exception {
    //Case1 : Positive Scenario
    String requestUri1 = "http://localhost:9200/_opendistro/_performanceanalyzer/"
        + "_agent/metrics?metrics=Latency,CPU_Utilization&agg=avg,max&dim=ShardID&nodes=all";
    String expectedResponseUri = "http://localhost:9600/_opendistro/_performanceanalyzer/"
        + "metrics?metrics=Latency,CPU_Utilization&agg=avg,max&dim=ShardID&nodes=all";

    RestRequest request = new RestRequest(null, new HashMap<>(), requestUri1, Collections.emptyMap(), null, null) {
      @Override
      public Method method() {
        return Method.GET;
      }

      @Override
      public String uri() {
        return requestUri1;
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
    request.params().put("redirectEndpoint", "metrics");
    URL finalURI = performanceAnalyzerRp.getAgentUri(request);
    assertEquals(finalURI.toString(), new URL(expectedResponseUri).toString());

    //Case2 : Negative Scenario
    String requestUri2 = "http://localhost:9200/_opendistro/_performanceanalyzer/_agent/garbage";
    request = new RestRequest(null, new HashMap<>(), requestUri1, Collections.emptyMap(), null, null) {
      @Override
      public Method method() {
        return Method.GET;
      }

      @Override
      public String uri() {
        return requestUri2;
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
    request.params().put("redirectEndpoint", "garbage");
    finalURI = performanceAnalyzerRp.getAgentUri(request);
    assertNull(finalURI);
  }
}