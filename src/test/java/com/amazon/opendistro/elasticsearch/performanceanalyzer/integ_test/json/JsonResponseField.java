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

import com.google.gson.annotations.SerializedName;

/**
 *   "fields": [
 *      {
 *        "name": "CPU_Utilization",
 *        "type": "DOUBLE"
 *      }
 *   ]
 */
public class JsonResponseField {
  private static final String NAME = "name";
  private static final String TYPE = "type";
  @SerializedName(NAME)
  private String name;
  @SerializedName(TYPE)
  private String type;

  public JsonResponseField(String name, String type) {
    this.name = name;
    this.type = type;
  }

  public String getName() {
    return this.name;
  }

  public String getType() {
    return this.type;
  }

  //SQLite data type
  public enum Type {
    VARCHAR(Constants.VARCHAR),
    DOUBLE(Constants.DOUBLE);

    private final String value;

    Type(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }

    public static class Constants {
      public static final String VARCHAR = "VARCHAR";
      public static final String DOUBLE = "DOUBLE";
    }
  }
}
