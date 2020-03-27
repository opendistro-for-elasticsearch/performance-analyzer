/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.jooq.Field;
import org.jooq.impl.DSL;

public class Dimensions {
  // Dimension is a key, value
  private Map<String, String> dimensions;

  public Dimensions() {
    this.dimensions = new HashMap<>();
  }

  public void put(String key, String value) {
    this.dimensions.put(key, value);
  }

  public String get(String key) {
    return this.dimensions.get(key);
  }

  public Map<Field<String>, String> getFieldMap() {
    Map<Field<String>, String> fieldMap = new HashMap<Field<String>, String>();
    for (Map.Entry<String, String> entry : dimensions.entrySet()) {
      fieldMap.put(DSL.field(DSL.name(entry.getKey()), String.class), entry.getValue());
    }
    return fieldMap;
  }

  public Set<String> getDimensionNames() {
    return this.dimensions.keySet();
  }
}
