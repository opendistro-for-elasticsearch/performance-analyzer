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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;
import java.io.File;
import java.sql.Connection;
import java.util.Map;
import java.util.NavigableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.BatchBindStep;

public class MasterMetricsEventProcessor implements EventProcessor {
  private static final Logger LOG = LogManager.getLogger(MasterMetricsEventProcessor.class);
  private MasterEventMetricsSnapshot masterSnap;
  private BatchBindStep handle;
  private long startTime;
  private long endTime;

  private MasterMetricsEventProcessor(MasterEventMetricsSnapshot masterSnap) {
    this.masterSnap = masterSnap;
  }

  static MasterMetricsEventProcessor buildMasterMetricEventsProcessor(
      long currWindowStartTime,
      Connection conn,
      NavigableMap<Long, MasterEventMetricsSnapshot> masterEventMetricsMap) {
    MasterEventMetricsSnapshot masterSnap = masterEventMetricsMap.get(currWindowStartTime);
    if (masterSnap == null) {
      masterSnap = new MasterEventMetricsSnapshot(conn, currWindowStartTime);
      Map.Entry<Long, MasterEventMetricsSnapshot> entry = masterEventMetricsMap.lastEntry();
      if (entry != null) {
        masterSnap.rolloverInflightRequests(entry.getValue());
      }
      masterEventMetricsMap.put(currWindowStartTime, masterSnap);
    }
    return new MasterMetricsEventProcessor(masterSnap);
  }

  @Override
  public void initializeProcessing(long startTime, long endTime) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.handle = masterSnap.startBatchPut();
  }

  @Override
  public void finalizeProcessing() {
    if (handle.size() > 0) {
      handle.execute();
    }
    LOG.debug("Final masterEvents request metrics {}", masterSnap.fetchAll());
  }

  @Override
  public void processEvent(Event event) {
    String[] keyElements = event.key.split(File.separatorChar == '\\' ? "\\\\" : File.separator);
    String threadId = keyElements[1];
    String insertOrder = keyElements[3];
    String startOrFinish = keyElements[4];
    if (startOrFinish.equals(PerformanceAnalyzerMetrics.START_FILE_NAME)) {
      emitStartMasterEventMetric(event, insertOrder, threadId);
    } else if (startOrFinish.equals(PerformanceAnalyzerMetrics.FINISH_FILE_NAME)) {
      emitEndMasterEventMetric(event, insertOrder, threadId);
    }
  }

  @Override
  public boolean shouldProcessEvent(Event event) {
    return event.key.contains(PerformanceAnalyzerMetrics.sMasterTaskPath);
  }

  @Override
  public void commitBatchIfRequired() {
    if (handle.size() > BATCH_LIMIT) {
      handle.execute();
      handle = masterSnap.startBatchPut();
    }
  }

  // threads/7462/master_task/245/start
  // current_time:1566413947489
  // MasterTaskPriority:URGENT
  // StartTime:1566413946989
  // MasterTaskType:delete-index
  // MasterTaskMetadata: [[nyc_taxis/f1i57IF8RCeI9nsKiLRMOg]]
  // MasterTaskQueueTime:11$
  private void emitStartMasterEventMetric(Event entry, String insertOrder, String threadId) {

    Map<String, String> keyValueMap = MetricsParser.extractEntryData(entry.value);
    String priority =
        keyValueMap.get(AllMetrics.MasterMetricDimensions.MASTER_TASK_PRIORITY.toString());
    long st = Long.parseLong(keyValueMap.get(AllMetrics.CommonMetric.START_TIME.toString()));
    String taskType =
        keyValueMap.get(AllMetrics.MasterMetricDimensions.MASTER_TASK_TYPE.toString());
    String taskMetadata =
        keyValueMap.get(AllMetrics.MasterMetricDimensions.MASTER_TASK_METADATA.toString());
    long queueTime =
        Long.parseLong(
            keyValueMap.get(AllMetrics.MasterMetricDimensions.MASTER_TASK_QUEUE_TIME.toString()));

    handle.bind(threadId, insertOrder, priority, taskType, taskMetadata, queueTime, st, null);
  }

  // An example master_task finish
  // threads/7462/master_task/245/finish
  // current_time:1566413959491
  // FinishTime:1566413958991
  private void emitEndMasterEventMetric(Event entry, String insertOrder, String threadId) {
    Map<String, String> keyValueMap = MetricsParser.extractEntryData(entry.value);
    long finishTime =
        Long.parseLong(keyValueMap.get(AllMetrics.CommonMetric.FINISH_TIME.toString()));
    handle.bind(threadId, insertOrder, null, null, null, null, null, finishTime);
  }
}
