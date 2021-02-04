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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.integ_test;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerIntegTestBase;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.integ_test.json.JsonResponseNode;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.junit.Assert;

public class MetricCollectorIntegTestBase extends PerformanceAnalyzerIntegTestBase {

  private List<String> nodeIDs;

  protected List<JsonResponseNode> readMetric(String endpoint) throws Exception {
    String jsonString;
    //read metric from local node
    Request request = new Request("GET", endpoint);
    Response resp = paClient.performRequest(request);
    Assert.assertEquals(HttpStatus.SC_OK, resp.getStatusLine().getStatusCode());
    jsonString = EntityUtils.toString(resp.getEntity());
    JsonObject jsonObject = new JsonParser().parse(jsonString).getAsJsonObject();
    return parseJsonResponse(jsonObject);
  }

  protected void initNodes() throws Exception {
    final Request request = new Request("GET", "/_cat/nodes?full_id&h=id");
    final Response response = adminClient().performRequest(request);
    nodeIDs = new ArrayList<>();
    if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
      try (BufferedReader responseReader = new BufferedReader(
          new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8))) {
        String line;
        while ((line = responseReader.readLine()) != null) {
          nodeIDs.add(line);
        }
      }
    }
  }

  protected List<String> getNodeIDs() {
    return nodeIDs;
  }

  private List<JsonResponseNode> parseJsonResponse(JsonObject jsonObject) throws JsonParseException {
    List<JsonResponseNode> responseNodeList = new ArrayList<>();
    jsonObject.entrySet().forEach(n -> {
      JsonResponseNode responseNode = new Gson().fromJson(n.getValue(), JsonResponseNode.class);
      responseNodeList.add(responseNode);
    });
    return responseNodeList;
  }
}
