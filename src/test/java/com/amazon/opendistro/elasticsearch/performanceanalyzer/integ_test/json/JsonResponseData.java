/*
 * Copyright 2020-2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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


import com.amazon.opendistro.elasticsearch.performanceanalyzer.integ_test.json.JsonResponseField.Type.Constants;
import com.google.gson.annotations.SerializedName;

/**
 * spotless:off
 *
 * "data": {
 *   "fields": [
 *      {
 *        "name": "CPU_Utilization",
 *        "type": "DOUBLE"
 *      }
 *   ],
 *   "records": [
 *       [
 *         0.005275218803760752
 *       ]
 *    ]
 *  }
 *
 *  spotless:on
 */
public class JsonResponseData {
  private static final String FIELDS = "fields";
  private static final String RECORDS = "records";

  @SerializedName(FIELDS)
  private JsonResponseField[] fields;

  @SerializedName(RECORDS)
  private String[][] records;

  public JsonResponseData(JsonResponseField[] fields, String[][] records) {
    this.fields = fields;
    this.records = records;
  }

  public int getFieldDimensionSize() {
    return fields.length;
  }

  public int getRecordSize() {
    return records.length;
  }

  public JsonResponseField getField(int index) throws IndexOutOfBoundsException {
    return fields[index];
  }

  public String getRecord(int index, String fieldName) throws Exception {
    for (int i = 0; i < getFieldDimensionSize(); i++) {
      if (fieldName.equals(fields[i].getName())) {
        return records[index][i];
      }
    }
    throw new IllegalArgumentException();
  }

  public Double getRecordAsDouble(int index, String fieldName) throws Exception {
    String recordStr = getRecord(index, fieldName);
    JsonResponseField field = getField(index);
    if (!field.getType().equals(Constants.DOUBLE)) {
      throw new IllegalArgumentException();
    }
    return Double.parseDouble(recordStr);
  }
}
