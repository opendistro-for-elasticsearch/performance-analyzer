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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.DBUtils;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.MetricName;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.BatchBindStep;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.SelectField;
import org.jooq.SelectHavingStep;
import org.jooq.SelectJoinStep;
import org.jooq.impl.DSL;

public class MemoryDBSnapshot implements Removable {
  private static final Logger LOG = LogManager.getLogger(MemoryDBSnapshot.class);

  private static final String WEIGHT = "weight";

  private static final Field<Double> WEIGHT_FIELD = DSL.field(WEIGHT, Double.class);

  protected final DSLContext create;
  protected final String tableName;

  // the last update time of the /dev/shm/performanceanalyzer file that is the data
  // source of our db table.
  protected long lastUpdatedTime;

  private final List<String> dimensionNames;

  private final List<Field<String>> dimensionsFields;

  private final List<Field<Double>> metadata;

  // We have 1 table for every metadata in the disk database. This map stores
  // the mapping from table names to the fields used to retrieve table
  // contents from memory db to disk db.
  private final Map<String, List<Field<?>>> tableSelectFieldsMap;

  private final Map<String, List<Field<String>>> tableGroupByFieldsMap;

  private final Map<String, Condition> tableWhereClauseMap;

  private final boolean isAligned;

  /**
   * @param conn In-memory database connection
   * @param tableNamePrefix db table name prefix
   * @param windowEndTime When creating un-aligned db snapshot, we use the time stamp when reader
   *     starts processing in a round as the 3rd parameter (Let's call it readerProcessTime). Reader
   *     process /dev/shm/performanceanalyzer files every 5 seconds. When creating aligned db
   *     snapshot, we use previous window's end time. Note the previous window's end time is
   *     computed after using PerformanceAnalyzerMetrics.getTimeInterval(). So if previous window's
   *     actual end time is 6000. After invoking PerformanceAnalyzerMetrics.getTimeInterval(6000,
   *     MetricsConfiguration.SAMPLING_INTERVAL), it is 5000.
   * @param aligned whether this snapshot is for aligning
   */
  public MemoryDBSnapshot(
      Connection conn, MetricName tableNamePrefix, long windowEndTime, boolean aligned) {
    this.create = DSL.using(conn, SQLDialect.SQLITE);
    this.isAligned = aligned;
    String tableNameSuffix = aligned ? "_aligned" : "";
    this.tableName = tableNamePrefix.toString() + windowEndTime + tableNameSuffix;
    lastUpdatedTime = -1;

    dimensionNames =
        MetricPropertiesConfig.getInstance().getProperty(tableNamePrefix).getDimensionNames();
    dimensionsFields =
        MetricPropertiesConfig.getInstance().getProperty(tableNamePrefix).getDimensionFields();
    metadata = MetricPropertiesConfig.getInstance().getProperty(tableNamePrefix).getMetricFields();

    tableSelectFieldsMap =
        MetricPropertiesConfig.getInstance().getProperty(tableNamePrefix).getTableSelectMap();
    tableGroupByFieldsMap =
        MetricPropertiesConfig.getInstance()
            .getProperty(tableNamePrefix)
            .getTableGroupByFieldsMap();
    tableWhereClauseMap =
        MetricPropertiesConfig.getInstance().getProperty(tableNamePrefix).getTableWhereClauseMap();

    // the tables should have columns in order:
    // dimensions columns, metrics columns
    LOG.debug("Creating a new snapshot table - {}", tableName);
    create.createTable(this.tableName).columns(dimensionsFields).columns(metadata).execute();
  }

  public MemoryDBSnapshot(Connection conn, MetricName tableNamePrefix, long windowEndTime) {
    this(conn, tableNamePrefix, windowEndTime, false);
  }

  public DSLContext getDSLContext() {
    return create;
  }

  @Override
  public void remove() {
    LOG.debug("Dropping {}", this.tableName);
    if (dbTableExists()) {
      create.dropTable(DSL.table(this.tableName)).execute();
    }
  }

  /**
   * @return the last update time of the /dev/shm/performanceanalyzer file that is the data source
   *     of our db table.
   */
  public long getLastUpdatedTime() {
    return this.lastUpdatedTime;
  }

  public void setLastUpdatedTime(long lastUpdatedTime) {
    this.lastUpdatedTime = lastUpdatedTime;
  }

  public String getTableName() {
    return this.tableName;
  }

  public SelectHavingStep<Record> selectAll() {
    return create.select().from(this.tableName);
  }

  public Result<Record> fetchAll() {
    return create.select().from(DSL.table(this.tableName)).fetch();
  }

  public BatchBindStep startBatchPut() {
    int totalSize = this.dimensionsFields.size() + this.metadata.size();
    List<Object> dummyValues = new ArrayList<>(totalSize);
    for (int i = 0; i < this.dimensionsFields.size(); i++) {
      dummyValues.add(null);
    }
    for (int i = 0; i < this.metadata.size(); i++) {
      dummyValues.add(null);
    }
    return create.batch(create.insertInto(DSL.table(this.tableName)).values(dummyValues));
  }

  public List<Field<String>> getDimensions() {
    return dimensionsFields;
  }

  public Collection<Field<Double>> getMetrics() {
    return metadata;
  }

  /**
   * Given metrics in two windows calculates a new window which overlaps with the given windows.
   * |------leftWindow-------|-------rightWindow--------| t a b |-----------alignedWindow------|
   *
   * <p>This method assumes that both left/right windows are greater than or equal to 5 seconds.
   *
   * @param leftWindow MemoryDBSnapshot for the /dev/shm/performanceanalyzer file written before t.
   *     We save MemoryDBSnapshot in a map where the key is the time at which the
   *     /dev/shm/performanceanalyzer file was written, and the value is the MemoryDBSnapshot
   *     itself.
   * @param rightWindow MemoryDBSnapshot for the /dev/shm/performanceanalyzer file written after t
   * @param a aligned window start time.
   * @param b aligned window end time.
   * @param t leftWindow end time, as well as right window start time
   */
  public void alignWindow(
      MemoryDBSnapshot leftWindow, MemoryDBSnapshot rightWindow, long t, long a, long b) {
    ArrayList<SelectField<?>> alignedFields = new ArrayList<SelectField<?>>();
    alignedFields.addAll(getDimensions());
    for (Field<Double> metric : getMetrics()) {
      alignedFields.add(DSL.sum(metric).div(DSL.sum(WEIGHT_FIELD)).as(metric));
    }

    List<SelectField<?>> leftWinFields = new ArrayList<SelectField<?>>();
    leftWinFields.addAll(getDimensions());
    leftWinFields.add(DSL.val(t - a).as(WEIGHT));
    for (Field<Double> metric : getMetrics()) {
      leftWinFields.add(metric.mul(t - a).as(metric.getName()));
    }
    List<SelectField<?>> rightWinFields = new ArrayList<SelectField<?>>();
    rightWinFields.addAll(getDimensions());
    rightWinFields.add(DSL.val(b - t).as(WEIGHT));
    for (Field<Double> metric : getMetrics()) {
      rightWinFields.add(metric.mul(b - t).as(metric.getName()));
    }

    SelectJoinStep<Record> recordsSource =
        create
            .select(alignedFields)
            .from(
                create
                    .select(leftWinFields)
                    .from(leftWindow.tableName)
                    .unionAll(create.select(rightWinFields).from(rightWindow.getTableName())));

    if (getDimensions().isEmpty()) {
      create.insertInto(DSL.table(this.tableName)).select(recordsSource).execute();
    } else {
      create
          .insertInto(DSL.table(this.tableName))
          .select(recordsSource.groupBy(getDimensions()))
          .execute();
    }
  }

  /**
   * Precondition: The order of columns in each values[i] should match the table we have created in
   * the constructor. We cannot check this programmatically. People who write code calling this
   * method is responsible for verification.
   *
   * @param values each values[i] is a row
   */
  public void insertMultiRows(Object[][] values) {
    if (values == null || values.length == 0) {
      return;
    }
    BatchBindStep batchHandle = startBatchPut();
    for (int i = 0; i < values.length; i++) {
      batchHandle.bind(values[i]);
    }
    batchHandle.execute();
  }

  protected Result<Record> fetchMetric(Condition condition, SelectField<?>... column) {
    return create.select(column).from(DSL.table(this.tableName)).where(condition).fetch();
  }

  protected boolean dbTableExists() {
    return DBUtils.checkIfTableExists(create, tableName);
  }

  protected Result<Record1<String>> fetchTableSchema() {
    return create
        .select(DSL.field("sql", String.class))
        .from(DSL.table("sqlite_master"))
        .where(DSL.field("name", String.class).eq(this.tableName))
        .fetch();
  }

  public Map<String, SelectHavingStep<Record>> selectMetadataSource() {
    Map<String, SelectHavingStep<Record>> selectFromTable = new HashMap<>();
    for (Map.Entry<String, List<Field<?>>> entry : tableSelectFieldsMap.entrySet()) {
      String tableName = entry.getKey();
      selectFromTable.put(
          tableName,
          create
              .select(entry.getValue())
              .from(this.tableName)
              .where(tableWhereClauseMap.get(tableName))
              .groupBy(tableGroupByFieldsMap.get(tableName)));
    }
    return selectFromTable;
  }

  public Map<String, List<Field<?>>> getTableSelectFieldsMap() {
    return tableSelectFieldsMap;
  }

  public List<String> getDimensionNames() {
    return dimensionNames;
  }

  public boolean isAligned() {
    return isAligned;
  }
}
