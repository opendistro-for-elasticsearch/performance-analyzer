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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.reader;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatExceptionCode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.JsonConverter;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.JsonPathNotFoundException;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.BatchBindStep;

public class NodeMetricsEventProcessor implements EventProcessor {
  private static final Logger LOG = LogManager.getLogger(NodeMetricsEventProcessor.class);

  private Map<AllMetrics.MetricName, MemoryDBSnapshot> metricsSnapshotMap;
  private Map<AllMetrics.MetricName, BatchBindStep> metricsBatchBindMap;
  private long startTime;
  private long endTime;
  private AllMetrics.MetricName lastUpdatedMetric;
  private Map<AllMetrics.MetricName, NavigableMap<Long, MemoryDBSnapshot>> nodeMetricsMap;

  private NodeMetricsEventProcessor(
      Map<AllMetrics.MetricName, MemoryDBSnapshot> metricsSnapshotMap) {
    this.metricsSnapshotMap = metricsSnapshotMap;
    this.metricsBatchBindMap = new HashMap<>();
  }

  static NodeMetricsEventProcessor buildNodeMetricEventsProcessor(
      long currTimestamp,
      Connection conn,
      Map<AllMetrics.MetricName, NavigableMap<Long, MemoryDBSnapshot>> nodeMetricsMap)
      throws Exception {
    Map<AllMetrics.MetricName, MemoryDBSnapshot> metricSnapshotMap = new HashMap<>();

    for (AllMetrics.MetricName metric : AllMetrics.MetricName.values()) {
      NavigableMap<Long, MemoryDBSnapshot> currMetricMap = nodeMetricsMap.get(metric);
      MemoryDBSnapshot currSnap = currMetricMap.get(currTimestamp);
      if (currSnap == null) {
        currSnap = new MemoryDBSnapshot(conn, metric, currTimestamp);
        currMetricMap.put(currTimestamp, currSnap);
      }
      metricSnapshotMap.put(metric, currSnap);
    }
    NodeMetricsEventProcessor eventProcessor = new NodeMetricsEventProcessor(metricSnapshotMap);
    eventProcessor.setNodeMetricsMap(nodeMetricsMap);
    return eventProcessor;
  }

  void setNodeMetricsMap(
      Map<AllMetrics.MetricName, NavigableMap<Long, MemoryDBSnapshot>> nodeMetricsMap) {
    this.nodeMetricsMap = nodeMetricsMap;
  }

  @Override
  public void initializeProcessing(long startTime, long endTime) {
    this.startTime = startTime;
    this.endTime = endTime;
    for (AllMetrics.MetricName metric : AllMetrics.MetricName.values()) {
      MemoryDBSnapshot dbSnap = metricsSnapshotMap.get(metric);
      metricsBatchBindMap.put(metric, dbSnap.startBatchPut());
    }
  }

  @Override
  public void finalizeProcessing() {
    for (AllMetrics.MetricName metric : AllMetrics.MetricName.values()) {
      BatchBindStep batchHandle = metricsBatchBindMap.get(metric);
      MemoryDBSnapshot dbSnap = metricsSnapshotMap.get(metric);
      if (batchHandle != null && batchHandle.size() > 0) {
        batchHandle.execute();
        NavigableMap<Long, MemoryDBSnapshot> currMap = nodeMetricsMap.get(metric);
        currMap.put(dbSnap.getLastUpdatedTime(), dbSnap);
      }
    }
  }

  @Override
  public boolean shouldProcessEvent(Event event) {
    for (String metric : MetricPropertiesConfig.getInstance().getMetricPathMap().values()) {
      if (event.key.contains(metric)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void commitBatchIfRequired() {
    if (lastUpdatedMetric != null) {
      BatchBindStep handle = metricsBatchBindMap.get(lastUpdatedMetric);
      if (handle.size() > BATCH_LIMIT) {
        handle.execute();
        metricsBatchBindMap.put(
            lastUpdatedMetric, metricsSnapshotMap.get(lastUpdatedMetric).startBatchPut());
      }
    }
  }

  @Override
  public void processEvent(Event nodeMetric) {
    String key = nodeMetric.key.split(File.separatorChar == '\\' ? "\\\\" : File.separator)[0];
    AllMetrics.MetricName name =
        MetricPropertiesConfig.getInstance().getEventKeyToMetricNameMap().get(key);

    MemoryDBSnapshot snap = metricsSnapshotMap.get(name);
    BatchBindStep batchHandler =
        metricsBatchBindMap.computeIfAbsent(name, k -> snap.startBatchPut());
    MetricProperties currParser = MetricPropertiesConfig.getInstance().getProperty(name);
    if (processEvent(nodeMetric, snap, startTime, batchHandler, currParser)) {
      lastUpdatedMetric = name;
    }
  }

  private boolean processEvent(
      Event event,
      MemoryDBSnapshot snap,
      long startTime,
      BatchBindStep batchHandle,
      MetricProperties metricProperties) {

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

    // snap's last updated time is the highest last modified time of all
    // the entries in the snapshot.
    if (snap.getLastUpdatedTime() < lastModifiedTime) {
      snap.setLastUpdatedTime(lastModifiedTime);
    }

    String[] derivedDimension = metricProperties.getHandler().processExtraDimensions(event.key);

    int numMetrics =
        derivedDimension.length
            + metricProperties.getDirectDimensionsSize()
            + metricProperties.getMetadataSize();
    Object[] templateMetricVals = new Object[numMetrics];
    int valIndex = 0;

    for (String s : derivedDimension) {
      templateMetricVals[valIndex] = s;
      valIndex += 1;
    }

    // first line is last modified time of the file.
    // We need last modified time in milliseconds. But JDK method
    // File.lastModified() cannot give that precision. So we need
    // to add last modified time by ourselves.
    // See:
    // https://bugs.java.com/bugdatabase/view_bug.do?bug_id=6939260

    boolean processed = false;
    for (int lineNum = 1; lineNum < lines.length; lineNum++) {
      processed =
          metricProperties.processJsonLine(lines[lineNum], batchHandle, templateMetricVals)
              || processed;
    }
    return processed;
  }
}
