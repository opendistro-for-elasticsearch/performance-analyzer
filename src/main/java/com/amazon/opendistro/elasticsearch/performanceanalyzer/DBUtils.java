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

package com.amazon.opendistro.elasticsearch.performanceanalyzer;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SelectHavingStep;
import org.jooq.TableLike;
import org.jooq.impl.DSL;

@SuppressWarnings("unchecked")
public class DBUtils {
  public static boolean checkIfTableExists(DSLContext create, String tableName) {
    Result<Record> res =
        create
            .select()
            .from(DSL.table("sqlite_master"))
            .where(DSL.field("type").eq("table").and(DSL.field("name").eq(tableName)))
            .fetch();
    return (res.size() > 0);
  }

  public static Result<Record> fetchTables(DSLContext create) {
    return create
        .select()
        .from(DSL.table("sqlite_master"))
        .where(DSL.field("type").eq("table"))
        .fetch();
  }

  public static List<Field<String>> getStringFieldsFromList(Collection<String> fieldNames) {
    return fieldNames.stream()
        .map(s -> DSL.field(DSL.name(s), String.class))
        .collect(Collectors.toList());
  }

  /**
   * Same implementation as getStringFieldsFromList, but return a list allowing other kinds of
   * fields other than String field.
   *
   * @param fieldNames a collection of field names
   * @return a list of org.jooq.Field objects
   */
  public static List<Field<?>> getFieldsFromList(Collection<String> fieldNames) {
    return fieldNames.stream()
        .map(s -> DSL.field(DSL.name(s), String.class))
        .collect(Collectors.toList());
  }

  public static List<Field<Double>> getDoubleFieldsFromList(Collection<String> fieldNames) {
    return fieldNames.stream()
        .map(s -> DSL.field(DSL.name(s), Double.class))
        .collect(Collectors.toList());
  }

  public static List<Field<?>> getStringFieldsFromTable(
      Collection<String> fieldNames, TableLike<Record> table) {
    return fieldNames.stream().map(s -> table.field(s, String.class)).collect(Collectors.toList());
  }

  public static List<Field<?>> getSelectFieldsForMetricName(
      String metricName, List<String> metrics, List<String> dimensions) {
    List<Field<?>> selectFields = DBUtils.getFieldsFromList(dimensions);
    for (String metric : metrics) {
      if (metric.equals(metricName)) {
        selectFields.add(DSL.field(metric, Double.class).as(metric));
      } else {
        selectFields.add(DSL.val(null, Double.class).as(metric));
      }
    }
    return selectFields;
  }

  /**
   * Get records by field and return as a set.
   *
   * @param table table select
   * @param field field
   * @param condition select condition
   * @param create db connection
   * @return records set
   */
  public static Set<String> getRecordSetByField(
      SelectHavingStep<Record> table, Field field, Condition condition, final DSLContext create) {
    Result<Record> records = create.select(field).from(table).where(condition).fetch();

    Set<String> res = new HashSet<>();
    for (int i = 0; i < records.size(); i++) {
      res.add(records.get(i).get(0).toString());
    }
    return res;
  }

  public static String getAggFieldName(String fieldName, String aggName) {
    return aggName + "_" + fieldName;
  }

  public static Map<String, Field<Double>> getDoubleFieldMapFromList(
      Collection<String> fieldNames) {
    return fieldNames.stream()
        .collect(Collectors.toMap(s -> s, s -> DSL.field(DSL.name(s), Double.class)));
  }
}
