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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.OSMetricsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.OSMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;
import java.io.File;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.BatchBindStep;

public class OSEventProcessor implements EventProcessor {

  private static final Logger LOG = LogManager.getLogger(OSEventProcessor.class);
  private List<String> tidToDelete;
  private OSMetricsSnapshot osSnap;
  private BatchBindStep handle;
  private long startTime;
  private long endTime;
  private Map<String, Long> lastUpdateTimePerTid;

  private OSEventProcessor(OSMetricsSnapshot osSnap) {
    this.osSnap = osSnap;
    tidToDelete = new ArrayList<>();
  }

  static EventProcessor buildOSMetricEventsProcessor(
      long startTime,
      long endTime,
      Connection conn,
      NavigableMap<Long, OSMetricsSnapshot> osMetricsMap)
      throws Exception {
    if (osMetricsMap.get(endTime) == null) {
      OSMetricsSnapshot osSnap = new OSMetricsSnapshot(conn, "os_", endTime);
      osMetricsMap.put(endTime, osSnap);
    }
    OSMetricsSnapshot osSnap = osMetricsMap.get(endTime);
    return new OSEventProcessor(osSnap);
  }

  public void initializeProcessing(long startTime, long endTime) {
    handle = osSnap.startBatchPut();
    this.startTime = startTime;
    this.endTime = endTime;
    lastUpdateTimePerTid = osSnap.getLastUpdateTimePerTid();
  }

  public void finalizeProcessing() {
    osSnap.deleteByTid(tidToDelete);
    if (handle.size() > 0) {
      handle.execute();
    }
  }

  public void processEvent(Event event) {
    String key = event.key;
    String threadID = key.split(File.separatorChar == '\\' ? "\\\\" : File.separator)[1];
    processOSEvent(event.value, threadID);
    // Flush data to sqlite when batch size is 500
    if (handle.size() == 500) {
      handle.execute();
      handle = osSnap.startBatchPut();
    }
  }

  @Override
  public boolean shouldProcessEvent(Event event) {
    return event.key.contains(PerformanceAnalyzerMetrics.sOSPath);
  }

  @Override
  public void commitBatchIfRequired() {
    if (handle.size() > BATCH_LIMIT) {
      handle.execute();
      handle = osSnap.startBatchPut();
    }
  }

  private Map<String, String> extrackKeyValFromData(String osMetricsData) {
    String[] lines = osMetricsData.split(System.lineSeparator());
    Map<String, String> osMetricsKeyValPairs = new HashMap<>();
    for (String line : lines) {
      String[] pair = line.split(PerformanceAnalyzerMetrics.sKeyValueDelimitor);
      osMetricsKeyValPairs.put(pair[0], String.join(":", Arrays.copyOfRange(pair, 1, pair.length)));
    }
    return osMetricsKeyValPairs;
  }

  private boolean processOSEvent(String data, String threadID) {
    Map<String, Double> osMetrics = new HashMap<>();
    // LOG.info("Processing OS Metrics data", osMetricsdata);

    Map<String, String> processedData = extrackKeyValFromData(data);

    long opFileLastModified =
        Long.parseLong(
            processedData.computeIfAbsent(
                PerformanceAnalyzerMetrics.METRIC_CURRENT_TIME, k -> String.valueOf(0)));
    if (opFileLastModified > endTime) {
      LOG.info(
          "File last modified {} time is greater than endTime - {}", opFileLastModified, endTime);
      opFileLastModified = endTime;
    }
    // Discard os metrics if the file has not been updated in the 5 second window.
    if (opFileLastModified < startTime) {
      return false;
    }
    // Only put data when opFile.lastModified() is newer than the lastUpdateTime in database.
    // If there is an update, We'll delete existing data and insert new data.
    if (lastUpdateTimePerTid.containsKey(threadID)) {
      if (lastUpdateTimePerTid.get(threadID) == opFileLastModified) {
        // TODO: Check why this is happening.
        // LOG.info("Skipping OS metrics");
        return false;
      }
      tidToDelete.add(threadID);
    }

    OSMetrics[] metrics = OSMetrics.values();
    for (OSMetrics metric : metrics) {
      String metricVal = processedData.get(metric.toString());
      if (metricVal != null) {
        Double val = Double.parseDouble(metricVal);
        osMetrics.put(metric.toString(), val);
      }
    }

    String threadName = processedData.get(OSMetricsCollector.MetaDataFields.threadName.toString());

    // The three new fields will be added to the array of the OS_METRICS enum members:
    // the thread ID at index 0.
    // thread name at index 1.
    // and the last modified time as index n-1.
    int numMetrics = metrics.length + 3;
    Object[] metricVals = new Object[numMetrics];
    metricVals[0] = threadID;
    metricVals[1] = threadName;
    for (int i = 2; i < numMetrics - 1; i++) {
      metricVals[i] = osMetrics.get(metrics[i - 2].toString());
    }
    metricVals[numMetrics - 1] = opFileLastModified;

    handle.bind(metricVals);
    return true;
  }
}
