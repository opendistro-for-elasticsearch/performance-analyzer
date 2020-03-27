/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MetricsRestUtil {

  private static final String WEBSERVICE_BIND_HOST_NAME = "webservice-bind-host";
  private static final Logger LOG = LogManager.getLogger(MetricsRestUtil.class);
  private static final int INCOMING_QUEUE_LENGTH = 1;
  private static final String QUERY_URL = "/_opendistro/_performanceanalyzer/metrics";

  public String nodeJsonBuilder(ConcurrentHashMap<String, String> nodeResponses) {
    StringBuilder outputJson = new StringBuilder();
    outputJson.append("{");
    Set<String> nodeSet = nodeResponses.keySet();
    String[] nodes = nodeSet.toArray(new String[nodeSet.size()]);
    if (nodes.length > 0) {
      outputJson.append("\"");
      outputJson.append(nodes[0]);
      outputJson.append("\": ");
      outputJson.append(nodeResponses.get(nodes[0]));
    }

    for (int i = 1; i < nodes.length; i++) {
      outputJson.append(", \"");
      outputJson.append(nodes[i]);
      outputJson.append("\" :");
      outputJson.append(nodeResponses.get(nodes[i]));
    }

    outputJson.append("}");
    return outputJson.toString();
  }

  public List<String> parseArrayParam(Map<String, String> params, String name, boolean optional) throws InvalidParameterException {
    if (!optional) {
      if (!params.containsKey(name) || params.get(name).isEmpty()) {
        throw new InvalidParameterException(String.format("%s parameter needs to be set", name));
      }
    }

    if (params.containsKey(name) && !params.get(name).isEmpty()) {
      return Arrays.asList(params.get(name).split(","));
    }
    return new ArrayList<>();
  }
}
