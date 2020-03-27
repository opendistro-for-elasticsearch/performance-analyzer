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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatExceptionCode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.JsonConverter;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.JsonPathNotFoundException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.BatchBindStep;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.impl.DSL;

public class MetricProperties {
  private static final Logger LOG = LogManager.getLogger(MetricProperties.class);

  public static final MetricDimension[] EMPTY_DIMENSION = new MetricDimension[] {};

  private FileHandler handler;
  // dimensions inferred else where (e.g., index name in file path).
  // The order should match the grouping parts in filePathRegex. For example,
  // if index name is the first group, and shard is the 2nd group, the first
  // element of derivedDimension should be index name and the 2nd element
  // should be shard id.
  private MetricDimension[] derivedDimension;

  int getDirectDimensionsSize() {
    return directDimensions.length;
  }

  private MetricDimension[] directDimensions;

  public int getMetadataSize() {
    return metadata.length;
  }

  private MetricValue[] metadata;

  // a list of dimension names
  private List<String> dimensionNames;

  // a list of dimension fields derived from dimensionNames
  private List<Field<String>> dimensionFields;

  // map from table prefix name to a list of metadata fields
  private List<Field<Double>> metadataFields;

  // We have 1 table for every metadata in the disk database. This list stores
  // table names for each metadata in metadata in order. Usually, we use
  // metadata name for table name. But it is possible we use the same medatada
  // name in different snapshots. For example, "util" can be in both
  // disk/network (not true right now).
  // We make it configurable on our end, so that we don't run into this
  // issue.
  private List<String> metadataTableNames;

  // disk db table name -> fields in select from memory db table to get
  // contents for the disk db table
  private Map<String, List<Field<?>>> inMemoryTableSelectFieldsMap = new HashMap<>();

  private Map<String, List<Field<String>>> inMemoryTableGroupByFieldsMap = new HashMap<>();

  private Map<String, Condition> inMemoryTableWhereClauseMap = new HashMap<>();

  public MetricProperties(
      MetricDimension[] derivedDimension,
      MetricDimension[] dimensions,
      MetricValue[] values,
      FileHandler handler,
      Map<String, String> customizedTableNames) {
    this(derivedDimension, dimensions, values, handler);
    customizeMetricTableName(customizedTableNames);

    initializeTableSelectFields();
  }

  public MetricProperties(
      MetricDimension[] derivedDimension,
      MetricDimension[] dimensions,
      MetricValue[] values,
      FileHandler handler) {
    super();
    this.handler = handler;
    this.derivedDimension = derivedDimension.clone();
    this.directDimensions = dimensions.clone();
    this.metadata = values.clone();
    this.inMemoryTableSelectFieldsMap = new HashMap<>();

    initializeFields();

    initializeTableSelectFields();
  }

  public MetricProperties(MetricDimension[] dimensions, MetricValue[] values, FileHandler handler) {
    this(EMPTY_DIMENSION, dimensions, values, handler);
  }

  public List<Field<Double>> getMetricFields() {
    return metadataFields;
  }

  public List<Field<String>> getDimensionFields() {
    return dimensionFields;
  }

  @VisibleForTesting
  void setHandler(FileHandler handler) {
    this.handler = handler;
  }

  FileHandler getHandler() {
    return handler;
  }

  boolean processMetrics(
      File file,
      MemoryDBSnapshot snap,
      long startTime,
      long lastSnapTimestamp,
      BatchBindStep batchHandle)
      throws IOException {

    try (BufferedReader bufferedReader = new BufferedReader(new FileReader(file))) {
      String line = bufferedReader.readLine();
      if (line == null) {
        return false;
      }

      long lastModifiedTime =
          JsonConverter.getLongValue(line, PerformanceAnalyzerMetrics.METRIC_CURRENT_TIME);

      // Only consider metrics if the file has been updated in the 5
      // second window.
      if (lastModifiedTime > startTime || lastModifiedTime <= lastSnapTimestamp) {
        return false;
      }

      // snap's last updated time is the highest last modified time of all
      // the entries in the snapshot.
      if (snap.getLastUpdatedTime() < lastModifiedTime) {
        snap.setLastUpdatedTime(lastModifiedTime);
      }

      String[] derivedDimension = handler.processExtraDimensions(file);

      int numMetrics = derivedDimension.length + directDimensions.length + metadata.length;
      Object[] templateMetricVals = new Object[numMetrics];
      int valIndex = 0;

      for (int i = 0; i < derivedDimension.length; i++) {
        templateMetricVals[valIndex++] = derivedDimension[i];
      }

      boolean processed = false;
      // first line is last modified time of the file.
      // We need last modified time in milliseconds. But JDK method
      // File.lastModified() cannot give that precision. So we need
      // to add last modified time by ourselves.
      // See:
      // https://bugs.java.com/bugdatabase/view_bug.do?bug_id=6939260
      while ((line = bufferedReader.readLine()) != null) {
        processed = processJsonLine(line, batchHandle, templateMetricVals) || processed;
      }
      return processed;
    } catch (JsonPathNotFoundException | JsonProcessingException e) {
      LOG.warn(
          String.format(
              "Fail to get last modified time of %s ExceptionCode: %s",
              file.getAbsolutePath(), StatExceptionCode.JSON_PARSER_ERROR.toString()),
          e);
      StatsCollector.instance().logException(StatExceptionCode.JSON_PARSER_ERROR);
      return false;
    }
  }

  private boolean processEvent(
      Event event,
      MemoryDBSnapshot snap,
      long startTime,
      long lastSnapTimestamp,
      BatchBindStep batchHandle) {

    if (event.value.isEmpty()) {
      return false;
    }
    String[] lines = event.value.split(System.getProperty("line.separator"));

    // First line should be
    // {"current_time":1566152878118}
    long lastModifiedTime = 0;
    try {
      lastModifiedTime =
          JsonConverter.getLongValue(lines[0], PerformanceAnalyzerMetrics.METRIC_CURRENT_TIME);
    } catch (JsonPathNotFoundException ex) {
      LOG.warn(
          String.format(
              "Fail to get last modified time of %s ExceptionCode: %s",
              event.key, StatExceptionCode.JSON_PARSER_ERROR.toString()),
          ex);
      StatsCollector.instance().logException(StatExceptionCode.JSON_PARSER_ERROR);
      return false;
    } catch (JsonProcessingException ex) {
      LOG.warn(
          String.format(
              "Malformed json (%s) ExceptionCode: %s",
              lines[0], StatExceptionCode.JSON_PARSER_ERROR.toString()),
          ex);
      StatsCollector.instance().logException(StatExceptionCode.JSON_PARSER_ERROR);
      return false;
    } catch (IOException ex) {
      LOG.warn(
          String.format(
              "I/O exception processing metric %s with value: %s.%s" + "ExceptionCode: %s",
              event.key, lines[0], File.separator, StatExceptionCode.JSON_PARSER_ERROR.toString()),
          ex);
      StatsCollector.instance().logException(StatExceptionCode.JSON_PARSER_ERROR);
      return false;
    }

    // Only consider metrics if the file has been updated in the 5
    // second window.

    if (lastModifiedTime > startTime || lastModifiedTime <= lastSnapTimestamp) {
      return false;
    }

    // snap's last updated time is the highest last modified time of all
    // the entries in the snapshot.
    if (snap.getLastUpdatedTime() < lastModifiedTime) {
      snap.setLastUpdatedTime(lastModifiedTime);
    }

    String[] derivedDimension = handler.processExtraDimensions(event.key);

    int numMetrics = derivedDimension.length + directDimensions.length + metadata.length;
    Object[] templateMetricVals = new Object[numMetrics];
    int valIndex = 0;

    for (String s : derivedDimension) {
      templateMetricVals[valIndex] = s;
      valIndex += 1;
    }

    boolean processed = false;
    // first line is last modified time of the file.
    // We need last modified time in milliseconds. But JDK method
    // File.lastModified() cannot give that precision. So we need
    // to add last modified time by ourselves.
    // See:
    // https://bugs.java.com/bugdatabase/view_bug.do?bug_id=6939260

    for (int lineNum = 1; lineNum < lines.length; lineNum++) {
      processed = processJsonLine(lines[lineNum], batchHandle, templateMetricVals) || processed;
    }
    return processed;
  }

  boolean processJsonLine(String line, BatchBindStep batchHandle, Object[] templateMetricVals) {
    Map<String, Object> map = JsonConverter.createMapFrom(line);

    if (map.isEmpty()) {
      return false;
    }

    Object[] metricVals = templateMetricVals.clone();
    int startIndex = derivedDimension.length;

    for (int i = 0; i < directDimensions.length; i++) {
      metricVals[startIndex + i] = map.get(directDimensions[i].toString());
    }

    startIndex += directDimensions.length;
    for (int i = 0; i < metadata.length; i++) {
      String key = metadata[i].toString();
      if (map.containsKey(key)) {
        metricVals[startIndex + i] = map.get(key);
      }
    }

    batchHandle.bind(metricVals);
    return true;
  }

  /**
   * @param snap memory database table representation of metric
   * @param startTime when reader starts collecting
   * @param lastSnapTimestamp the highest modified time of all the files processed for the last
   *     snapshot.
   * @return whether any metrics extracted from /dev/shm/performanceanalyzer files
   * @throws Exception thrown if we have issues parsing metrics
   */
  public boolean dispatch(MemoryDBSnapshot snap, long startTime, long lastSnapTimestamp)
      throws Exception {

    long startTimeThirtySecondBucket = PerformanceAnalyzerMetrics.getTimeInterval(startTime);
    long prevThirtySecondBucket =
        startTimeThirtySecondBucket - MetricsConfiguration.ROTATION_INTERVAL;

    BatchBindStep handle = snap.startBatchPut();

    boolean metricProcessed = false;

    // TODO: We can have two rows in db tables with the same dimensions.
    List<File> metricFiles = handler.findFiles4Metric(startTimeThirtySecondBucket);
    for (File f : metricFiles) {
      metricProcessed =
          processMetrics(f, snap, startTime, lastSnapTimestamp, handle) || metricProcessed;
    }

    metricFiles = handler.findFiles4Metric(prevThirtySecondBucket);
    for (File f : metricFiles) {
      metricProcessed =
          processMetrics(f, snap, startTime, lastSnapTimestamp, handle) || metricProcessed;
    }

    if (handle.size() > 0) {
      handle.execute();
    }

    return metricProcessed;
  }

  private List<String> createEnumNameList(Object[] enumValues) {
    if (enumValues != null && enumValues.length > 0) {
      return Arrays.stream(enumValues).map(d -> d.toString()).collect(Collectors.toList());
    }
    return Collections.emptyList();
  }

  /** Initialize fields used for database operation */
  private void initializeFields() {
    dimensionNames = new ArrayList<>();

    dimensionNames.addAll(createEnumNameList(derivedDimension));

    dimensionNames.addAll(createEnumNameList(directDimensions));

    dimensionFields = DBUtils.getStringFieldsFromList(dimensionNames);

    metadataTableNames = createEnumNameList(metadata);
    metadataFields = DBUtils.getDoubleFieldsFromList(metadataTableNames);
  }

  /**
   * Initialize fields used for database operation. Customize some of the table names. See the
   * comments of metricTableNames for details.
   */
  private void customizeMetricTableName(Map<String, String> tableName) {
    for (int i = 0; i < metadataTableNames.size(); i++) {
      String metricName = metadataTableNames.get(i).toString();
      if (tableName.containsKey(metricName)) {
        metadataTableNames.set(i, tableName.get(metricName));
      }
    }
  }

  public List<String> getMetadataTableNames() {
    return metadataTableNames;
  }

  /**
   * Precondition: should be called after metricFields and metricTableNames are fully initialized.
   *
   * <p>Initialize the map from metric table name to select and group by fields. These select fields
   * are used to create disk metric table.
   */
  private void initializeTableSelectFields() {

    for (int i = 0; i < metadataFields.size(); i++) {
      Field<Double> metadataField = metadataFields.get(i);
      String metadataName = metadataField.getName();
      String tableName = metadataTableNames.get(i);

      List<Field<String>> groupByFields = new ArrayList<Field<String>>();
      groupByFields.addAll(getDimensionFields());

      List<Field<?>> selectFields = new ArrayList<Field<?>>();
      selectFields.addAll(getDimensionFields());

      selectFields.add(DSL.sum(metadataField).as("sum_" + metadataName));
      selectFields.add(DSL.avg(metadataField).as("avg_" + metadataName));
      selectFields.add(DSL.min(metadataField).as("min_" + metadataName));
      selectFields.add(DSL.max(metadataField).as("max_" + metadataName));

      inMemoryTableSelectFieldsMap.put(tableName, selectFields);
      inMemoryTableGroupByFieldsMap.put(tableName, groupByFields);

      Condition whereClause = metadataField.isNotNull();
      inMemoryTableWhereClauseMap.put(tableName, whereClause);
    }
  }

  public Map<String, List<Field<?>>> getTableSelectMap() {
    return inMemoryTableSelectFieldsMap;
  }

  public Map<String, List<Field<String>>> getTableGroupByFieldsMap() {
    return inMemoryTableGroupByFieldsMap;
  }

  public Map<String, Condition> getTableWhereClauseMap() {
    return inMemoryTableWhereClauseMap;
  }

  public List<String> getDimensionNames() {
    return dimensionNames;
  }
}
