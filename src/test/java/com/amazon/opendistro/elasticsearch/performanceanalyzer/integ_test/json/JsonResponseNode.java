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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.integ_test.json;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.integ_test.json.JsonResponseData;
import com.google.gson.annotations.SerializedName;

public class JsonResponseNode {
  private static final String DATA = "data";
  private static final String TIMESTAMP = "timestamp";
  @SerializedName(DATA)
  private JsonResponseData data;
  @SerializedName(TIMESTAMP)
  private long timestamp;

  public JsonResponseNode(JsonResponseData data, long timestamp) {
    this.data = data;
    this.timestamp = timestamp;
  }

  public JsonResponseData getData() {
    return data;
  }

  public long getTimestamp() {
    return timestamp;
  }
}
