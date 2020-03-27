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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.reader;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.OSMetrics;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.BatchBindStep;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.SelectField;
import org.jooq.SelectHavingStep;
import org.jooq.impl.DSL;

@SuppressWarnings("serial")
public class OSMetricsSnapshot implements Removable {
  private static final Logger LOG = LogManager.getLogger(OSMetricsSnapshot.class);

  private final DSLContext create;
  private final String tableName;
  private Set<String> dimensionColumns;
  private static final String LAST_UPDATE_TIME_FIELD = "lastUpdateTime";

  private static final LinkedHashSet<String> METRIC_COLUMNS;

  public enum Fields {
    tid,
    tName,
    weight
  }

  static {
    METRIC_COLUMNS = new LinkedHashSet<>();
    for (OSMetrics metric : OSMetrics.values()) {
      METRIC_COLUMNS.add(metric.toString());
    }
  }

  public DSLContext getDSLContext() {
    return create;
  }

  public OSMetricsSnapshot(Connection conn, String tableNamePrefix, Long windowEndTime) {
    this.tableName = tableNamePrefix + windowEndTime;
    this.create = DSL.using(conn, SQLDialect.SQLITE);

    this.dimensionColumns =
        new LinkedHashSet<String>() {
          {
            this.add(Fields.tid.toString());
            this.add(Fields.tName.toString());
          }
        };

    LOG.debug("Creating a new os snapshot table - {}", tableName);
    create.createTable(this.tableName).columns(getFields()).execute();
  }

  public OSMetricsSnapshot(Connection conn, Long windowEndTime) {
    this(conn, "os_", windowEndTime);
  }

  public void putMetric(
      Map<String, Double> metrics, Map<String, String> dimensions, long updateTime) {
    Map<Field<?>, String> dimensionMap = new HashMap<Field<?>, String>();
    Map<Field<?>, Double> metricMap = new HashMap<Field<?>, Double>();
    Map<Field<?>, Long> updateTimeMap = new HashMap<Field<?>, Long>();

    for (Map.Entry<String, String> dimension : dimensions.entrySet()) {
      dimensionMap.put(DSL.field(DSL.name(dimension.getKey()), String.class), dimension.getValue());
    }

    for (Map.Entry<String, Double> metricName : metrics.entrySet()) {
      metricMap.put(DSL.field(DSL.name(metricName.getKey()), Double.class), metricName.getValue());
    }

    updateTimeMap.put(DSL.field(LAST_UPDATE_TIME_FIELD, Long.class), updateTime);

    create
        .insertInto(DSL.table(this.tableName))
        .set(metricMap)
        .set(dimensionMap)
        .set(updateTimeMap)
        .execute();
  }

  public void putMetric(Map<String, Double> metrics, String tid, String tName) {
    Map<Field<?>, Double> metricMap = new HashMap<Field<?>, Double>();

    for (Map.Entry<String, Double> metricName : metrics.entrySet()) {
      metricMap.put(DSL.field(DSL.name(metricName.getKey()), Double.class), metricName.getValue());
    }

    create
        .insertInto(DSL.table(this.tableName))
        .set(DSL.field(Fields.tid.toString()), tid)
        .set(DSL.field(Fields.tName.toString()), tName)
        .set(metricMap)
        .execute();
  }

  public BatchBindStep startBatchPut() {
    List<Object> dummyValues = new ArrayList<>();
    for (int i = 0; i < dimensionColumns.size(); i++) {
      dummyValues.add(null);
    }
    for (int i = 0; i < METRIC_COLUMNS.size(); i++) {
      dummyValues.add(null);
    }
    // last update time column
    dummyValues.add(null);
    return create.batch(create.insertInto(DSL.table(this.tableName)).values(dummyValues));
  }

  public void deleteByTid(List<String> tids) {
    create
        .delete(DSL.table(this.tableName))
        .where(DSL.field(Fields.tid.name(), String.class).in(tids))
        .execute();
  }

  public List<Field<?>> getMetricColumnFields() {
    return OSMetricsSnapshot.METRIC_COLUMNS.stream()
                                           .map(s -> DSL.field(s, Double.class))
                                           .collect(Collectors.toList());
  }

  public String getTableName() {
    return this.tableName;
  }

  public Result<Record> fetchAll() {
    return create.select().from(DSL.table(this.tableName)).fetch();
  }

  public Result<Record> fetchNegative() {
    return create
        .select()
        .from(DSL.table(this.tableName))
        .where(DSL.field(OSMetrics.CPU_UTILIZATION.toString()).lt(0L))
        .fetch();
  }

  public SelectHavingStep<Record> selectAll() {
    return create.select(getFields()).from(this.tableName);
  }

  @Override
  public void remove() {
    LOG.debug("Dropping {}", this.tableName);
    create.dropTable(DSL.table(this.tableName)).execute();
  }

  public void logSnap() {
    LOG.debug(() -> getDebugSnap());
  }

  public Result<?> getDebugSnap() {
    return create
        .select(
            DSL.field(Fields.tid.toString()).as(Fields.tid.toString()),
            DSL.field(Fields.tName.toString()).as(Fields.tName.toString()),
            DSL.field(OSMetrics.CPU_UTILIZATION.toString()),
            DSL.field(OSMetrics.PAGING_MIN_FLT_RATE.toString()))
        .from(this.tableName)
        .where(DSL.field(OSMetrics.CPU_UTILIZATION.toString(), Double.class).ne(0d))
        .fetch();
  }

  public Result<Record> getOSMetrics() {
    List<SelectField<?>> fields = new ArrayList<SelectField<?>>();
    fields.add(DSL.field(Fields.tid.toString()).as(Fields.tid.toString()));
    fields.add(DSL.field(Fields.tName.toString()).as(Fields.tName.toString()));
    for (String metricColumn : METRIC_COLUMNS) {
      fields.add(DSL.field(metricColumn, Double.class).as(metricColumn));
    }
    return create.select(fields).from(this.tableName).fetch();
  }

  public Map<String, Long> getLastUpdateTimePerTid() {
    List<SelectField<?>> fields = new ArrayList<SelectField<?>>();
    fields.add(DSL.field(Fields.tid.name()).as(Fields.tid.name()));
    fields.add(DSL.field(LAST_UPDATE_TIME_FIELD).as(LAST_UPDATE_TIME_FIELD));
    Result<Record> ret = create.select(fields).from(this.tableName).fetch();

    Map<String, Long> lastUpdateTimePerTid = new HashMap<>();
    for (int i = 0; i < ret.size(); i++) {
      lastUpdateTimePerTid.put(
          ret.get(i).get(Fields.tid.name()).toString(),
          Long.parseLong(ret.get(i).get(LAST_UPDATE_TIME_FIELD).toString()));
    }
    return lastUpdateTimePerTid;
  }

  /**
   * Given metrics in two windows calculates a new window which overlaps with the given windows.
   * |------leftWindow-------|-------rightWindow--------| leftLastUpdateTime rightLastUpdateTime
   *
   * <p>a b |-----alignedWindow-----|
   *
   * <p>leftWeight = leftLastUpdateTime - a rightWeight = b - (rightLastUpdateTime - simpleInterval)
   *
   * <p>This method assumes that both left/right windows are greater than or equal to 5 seconds.
   *
   * @param leftWindow a snapshot of the left window metrics
   * @param rightWindow a snapshot of the right window metrics
   * @param alignedWindow aligned window combinging left and right window
   * @param a aligned window start time.
   * @param b aligned window end time.
   */
  public static void alignWindow(
      OSMetricsSnapshot leftWindow,
      OSMetricsSnapshot rightWindow,
      String alignedWindow,
      long a,
      long b) {
    DSLContext create = leftWindow.getDSLContext();

    String leftPrefix = "l_";
    String rightPrefix = "r_";

    SelectHavingStep<Record> alignWindow =
        selectAlignWindow(
            create, leftWindow.tableName, rightWindow.tableName, leftPrefix, rightPrefix);

    create
        .insertInto(DSL.table(alignedWindow))
        .select(
            selectFieldsHasLeftAndRight(create, leftPrefix, rightPrefix, a, b, alignWindow)
                .unionAll(selectFieldsHasLeftOnly(create, leftPrefix, rightPrefix, alignWindow))
                .unionAll(selectFieldsHasRightOnly(create, leftPrefix, rightPrefix, alignWindow)))
        .execute();
  }

  /**
   * Select records that exists in both left window and right window. Calc result by its weight.
   *
   * <p>MetricValue = ((l_updateTime - a) * l_Metric + (b - l_updateTime) * r_metric) / 5
   *
   * <p>Example: alignWindow: |tid|l_lastModifiTime|l_cpu|l_rss|r_lastModifiTime|r_cpu|r_rss|
   * +---+----------------+-----+-----+----------------+-----+-----+ | 1| 3| 10| 10| | | | | 1| | |
   * | 7| 20| 20| | 2| 4| 10| 10| | | | | 3| | | | 8| 10| 10|
   *
   * <p>Return: |tid|lastModifiTime|cpu|rss| +---+--------------+---+---+ | 1| 3| 16| 16|
   *
   * @param leftPrefix field prefix when merge from left table to align table
   * @param rightPrefix field prefix when merge from right table to align table
   * @param alignWindow align window return from selectAlignWindow
   * @return see above example
   */
  private static SelectHavingStep<Record> selectFieldsHasLeftAndRight(
      DSLContext create,
      String leftPrefix,
      String rightPrefix,
      long a,
      long b,
      SelectHavingStep<Record> alignWindow) {
    ArrayList<SelectField<?>> fieldsHasLeftAndRight = new ArrayList<SelectField<?>>();
    fieldsHasLeftAndRight.add(DSL.field(Fields.tid.name()).as(Fields.tid.name()));
    fieldsHasLeftAndRight.add(DSL.field(Fields.tName.name()).as(Fields.tName.name()));
    for (String metricName : METRIC_COLUMNS) {
      fieldsHasLeftAndRight.add(
          DSL.field(leftPrefix + LAST_UPDATE_TIME_FIELD, Long.class)
              .sub(a)
              .mul(DSL.field(leftPrefix + metricName, Double.class))
              .add(
                  DSL.val(b)
                      .sub(DSL.field(leftPrefix + LAST_UPDATE_TIME_FIELD, Long.class))
                      .mul(DSL.field(rightPrefix + metricName, Double.class)))
              .div(b - a)
              .as(metricName));
    }
    fieldsHasLeftAndRight.add(
        DSL.field(leftPrefix + LAST_UPDATE_TIME_FIELD).as(LAST_UPDATE_TIME_FIELD));

    Condition conditionHasLeftAndRight =
        DSL.field(leftPrefix + LAST_UPDATE_TIME_FIELD, Long.class)
            .isNotNull()
            .and(DSL.field(rightPrefix + LAST_UPDATE_TIME_FIELD, Long.class).isNotNull());

    return create.select(fieldsHasLeftAndRight).from(alignWindow).where(conditionHasLeftAndRight);
  }

  /**
   * Select records that only exists in the left window.
   *
   * <p>Example: alignWindow: |tid|l_lastModifiTime|l_cpu|l_rss|r_lastModifiTime|r_cpu|r_rss|
   * +---+----------------+-----+-----+----------------+-----+-----+ | 1| 3| 10| 10| | | | | 1| | |
   * | 7| 20| 20| | 2| 4| 10| 10| | | | | 3| | | | 8| 10| 10|
   *
   * <p>Return: |tid|lastModifiTime|cpu|rss| +---+--------------+---+---+ | 2| 4| 10| 10|
   *
   * @param leftPrefix field prefix when merge from left table to align table
   * @param rightPrefix field prefix when merge from right table to align table
   * @param alignWindow align window return from selectAlignWindow
   * @return see above example
   */
  private static SelectHavingStep<Record> selectFieldsHasLeftOnly(
      DSLContext create,
      String leftPrefix,
      String rightPrefix,
      SelectHavingStep<Record> alignWindow) {
    ArrayList<SelectField<?>> fieldsHasLeftOnly = new ArrayList<SelectField<?>>();
    fieldsHasLeftOnly.add(DSL.field(Fields.tid.name()).as(Fields.tid.name()));
    fieldsHasLeftOnly.add(DSL.field(Fields.tName.name()).as(Fields.tName.name()));
    for (String metricName : METRIC_COLUMNS) {
      fieldsHasLeftOnly.add(DSL.field(leftPrefix + metricName, Double.class).as(metricName));
    }
    fieldsHasLeftOnly.add(
        DSL.field(leftPrefix + LAST_UPDATE_TIME_FIELD).as(LAST_UPDATE_TIME_FIELD));

    Condition conditionHasLeftOnly =
        DSL.field(leftPrefix + LAST_UPDATE_TIME_FIELD, Long.class)
            .isNotNull()
            .and(DSL.field(rightPrefix + LAST_UPDATE_TIME_FIELD, Long.class).isNull());

    return create.select(fieldsHasLeftOnly).from(alignWindow).where(conditionHasLeftOnly);
  }

  /**
   * Select records that only exists in the right window.
   *
   * <p>Example: alignWindow: |tid|l_lastModifiTime|l_cpu|l_rss|r_lastModifiTime|r_cpu|r_rss|
   * +---+----------------+-----+-----+----------------+-----+-----+ | 1| 3| 10| 10| | | | | 1| | |
   * | 7| 20| 20| | 2| 4| 10| 10| | | | | 3| | | | 8| 10| 10|
   *
   * <p>Return: |tid|lastModifiTime|cpu|rss| +---+--------------+---+---+ | 3| | 10| 10|
   *
   * @param leftPrefix field prefix when merge from left table to align table
   * @param rightPrefix field prefix when merge from right table to align table
   * @param alignWindow align window return from selectAlignWindow
   * @return see above example
   */
  private static SelectHavingStep<Record> selectFieldsHasRightOnly(
      DSLContext create,
      String leftPrefix,
      String rightPrefix,
      SelectHavingStep<Record> alignWindow) {
    ArrayList<SelectField<?>> fieldsHasRightOnly = new ArrayList<SelectField<?>>();
    fieldsHasRightOnly.add(DSL.field(Fields.tid.name()).as(Fields.tid.name()));
    fieldsHasRightOnly.add(DSL.field(Fields.tName.name()).as(Fields.tName.name()));
    for (String metricName : METRIC_COLUMNS) {
      fieldsHasRightOnly.add(DSL.field(rightPrefix + metricName, Double.class).as(metricName));
    }
    fieldsHasRightOnly.add(
        DSL.field(leftPrefix + LAST_UPDATE_TIME_FIELD).as(LAST_UPDATE_TIME_FIELD));

    Condition conditionHasRightOnly =
        DSL.field(leftPrefix + LAST_UPDATE_TIME_FIELD, Long.class)
            .isNull()
            .and(DSL.field(rightPrefix + LAST_UPDATE_TIME_FIELD, Long.class).isNotNull());

    return create.select(fieldsHasRightOnly).from(alignWindow).where(conditionHasRightOnly);
  }

  /**
   * Merge left window table and right window table into align window.
   *
   * <p>Example: Left window table |tid|lastModifiTime|cpu|rss| +---+--------------+---+---+ | 1| 3|
   * 10| 10| | 2| 4| 10| 10|
   *
   * <p>Right window table |tid|lastModifiTime|cpu|rss| +---+--------------+---+---+ | 1| 7| 20| 20|
   * | 3| 8| 10| 10|
   *
   * <p>Return align window |tid|l_lastModifiTime|l_cpu|l_rss|r_lastModifiTime|r_cpu|r_rss|
   * +---+----------------+-----+-----+----------------+-----+-----+ | 1| 3| 10| 10| | | | | 1| | |
   * | 7| 20| 20| | 2| 4| 10| 10| | | | | 3| | | | 8| 10| 10|
   *
   * @param create DSLContext
   * @param leftTableName left table name
   * @param rightTableName right table name
   * @param leftPrefix field prefix when merge from left table to align table
   * @param rightPrefix field prefix when merge from right table to align table
   * @return see above example
   */
  private static SelectHavingStep<Record> selectAlignWindow(
      DSLContext create,
      String leftTableName,
      String rightTableName,
      String leftPrefix,
      String rightPrefix) {
    List<SelectField<?>> fields = new ArrayList<SelectField<?>>();
    fields.add(DSL.field(Fields.tid.name(), String.class).as(Fields.tid.name()));
    fields.add(DSL.field(Fields.tName.name(), String.class).as(Fields.tName.name()));
    fields.add(
        DSL.max(DSL.field(leftPrefix + LAST_UPDATE_TIME_FIELD, Long.class))
            .as(leftPrefix + LAST_UPDATE_TIME_FIELD));
    for (String c : METRIC_COLUMNS) {
      fields.add(DSL.max(DSL.field(leftPrefix + c, Double.class)).as(leftPrefix + c));
    }
    fields.add(
        DSL.max(DSL.field(rightPrefix + LAST_UPDATE_TIME_FIELD, Long.class))
            .as(rightPrefix + LAST_UPDATE_TIME_FIELD));
    for (String c : METRIC_COLUMNS) {
      fields.add(DSL.max(DSL.field(rightPrefix + c, Double.class)).as(rightPrefix + c));
    }

    return create
        .select(fields)
        .from(
            selectAlignWindowFromLeft(create, leftTableName, leftPrefix, rightPrefix)
                .unionAll(
                    selectAlignWindowFromRight(create, rightTableName, leftPrefix, rightPrefix)))
        .groupBy(DSL.field(Fields.tid.name(), String.class));
  }

  private static SelectHavingStep<Record> selectAlignWindowFromLeft(
      DSLContext create, String tableName, String leftPrefix, String rightPrefix) {
    List<SelectField<?>> fields = new ArrayList<SelectField<?>>();
    fields.add(DSL.field(Fields.tid.name(), String.class).as(Fields.tid.name()));
    fields.add(DSL.field(Fields.tName.name(), String.class).as(Fields.tName.name()));
    fields.add(
        DSL.field(LAST_UPDATE_TIME_FIELD, Long.class).as(leftPrefix + LAST_UPDATE_TIME_FIELD));
    for (String c : METRIC_COLUMNS) {
      fields.add(DSL.field(c, Double.class).as(leftPrefix + c));
    }
    fields.add(DSL.val(null, Long.class).as(rightPrefix + LAST_UPDATE_TIME_FIELD));
    for (String c : METRIC_COLUMNS) {
      fields.add(DSL.val(null, Double.class).as(rightPrefix + c));
    }
    return create.select(fields).from(tableName);
  }

  private static SelectHavingStep<Record> selectAlignWindowFromRight(
      DSLContext create, String tableName, String leftPrefix, String rightPrefix) {
    List<SelectField<?>> fields = new ArrayList<SelectField<?>>();
    fields.add(DSL.field(Fields.tid.name(), String.class).as(Fields.tid.name()));
    fields.add(DSL.field(Fields.tName.name(), String.class).as(Fields.tName.name()));
    fields.add(DSL.val(null, Long.class).as(leftPrefix + LAST_UPDATE_TIME_FIELD));
    for (String c : METRIC_COLUMNS) {
      fields.add(DSL.val(null, Double.class).as(leftPrefix + c));
    }
    fields.add(
        DSL.field(LAST_UPDATE_TIME_FIELD, Long.class).as(rightPrefix + LAST_UPDATE_TIME_FIELD));
    for (String c : METRIC_COLUMNS) {
      fields.add(DSL.field(c, Double.class).as(rightPrefix + c));
    }
    return create.select(fields).from(tableName);
  }

  public List<Field<?>> getFields() {
    List<Field<?>> fields = new ArrayList<Field<?>>();
    for (String dimension : dimensionColumns) {
      fields.add(DSL.field(dimension, String.class));
    }
    for (String metric : METRIC_COLUMNS) {
      fields.add(DSL.field(metric, Double.class));
    }
    fields.add(DSL.field(DSL.name(LAST_UPDATE_TIME_FIELD), Long.class));
    return fields;
  }

  public Set<String> getMetricColumns() {
    return OSMetricsSnapshot.METRIC_COLUMNS;
  }
}
