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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.ScheduledMetricCollectorsExecutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;


public class PerformanceAnalyzerResourceProvider extends BaseRestHandler {
  private static final Logger LOG = LogManager.getLogger(PerformanceAnalyzerResourceProvider.class);
  private static PerformanceAnalyzerResourceProvider instance = null;
  private static final int HTTP_CLIENT_CONNECTION_TIMEOUT_MILLIS = 200;
  private static final String AGENT_PATH = "/_opendistro/_performanceanalyzer/_agent/";
  private static final String REDIRECT_BASE_PATH = "http://localhost:9600/_opendistro/_performanceanalyzer/";
  private static final String RCA = "rca";
  private static final String METRICS = "metrics";

  @Inject
  public PerformanceAnalyzerResourceProvider(Settings settings, RestController controller) {
    super(settings);
    controller.registerHandler(org.elasticsearch.rest.RestRequest.Method.GET, AGENT_PATH + "{redirectEndpoint}", this);
  }

  public String getName() {
    return "PerformanceAnalyzer_ResourceProvider";
  }

  @Override
  protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
    StringBuilder response = new StringBuilder();
    String inputLine;
    URL url = getAgentUri(request);
    // 'url' is null if no correct mapping for input uri is found
    if (url == null) {
      return channel -> {
        RestResponse finalResponse = new BytesRestResponse(RestStatus.NOT_FOUND, "");
        channel.sendResponse(finalResponse);
      };
    } else {
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setConnectTimeout(HTTP_CLIENT_CONNECTION_TIMEOUT_MILLIS);

      //Build Response in buffer
      try ( BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
        while ((inputLine = in.readLine()) != null) {
          response.append(inputLine);
        }
        LOG.debug("Response received - {}", response);
      } catch (Exception ex) {
        LOG.error("Error receiving response for Request Uri {} - {}", request.uri(), ex);
        return channel -> {
          channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR,"Something went wrong"));
        };
      }

      return channel -> {
        try {
          RestResponse finalResponse = new BytesRestResponse(RestStatus.OK, String.valueOf(response));
          LOG.debug("finalResponse: {}", finalResponse);
          Map<String, List<String>> map = conn.getHeaderFields();
          for (Map.Entry<String, List<String>> entry : map.entrySet()) {
            finalResponse.addHeader(entry.getKey(), entry.getValue().toString());
          }
          //Send Response back to callee
          channel.sendResponse(finalResponse);
        } catch (Exception ex) {
          LOG.error("Error sending response", ex);
          channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR,"Something went wrong"));
        }
      };
    }
  }

  /**
   * Get Agent URI mapping
   * @param request : RestRequest as input with valid URI
   * @return URI of target path
   * @throws IOException
   */
  public URL getAgentUri(RestRequest request) throws IOException{
    String redirectEndpoint = request.param("redirectEndpoint");
    String uri = "";
    // Need to register all params in ES request else es throws illegal_argument_exception
    for (String key : request.params().keySet()){
      request.param(key);
    }

    // Add Handler whenever add new redirectAgent path
    switch (redirectEndpoint){
      case METRICS:
      case RCA:
        uri = String.format(REDIRECT_BASE_PATH + request.uri().split(AGENT_PATH)[1]);
        break;

      default:
        LOG.error("Endpoint path Unexpected for: ", redirectEndpoint);
        return null;
    }
    return new URL(uri);
  }
}