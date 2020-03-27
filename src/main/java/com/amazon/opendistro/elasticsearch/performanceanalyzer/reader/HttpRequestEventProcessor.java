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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.HttpDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.HttpMetric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;
import java.io.File;
import java.sql.Connection;
import java.util.Map;
import java.util.NavigableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.BatchBindStep;

public class HttpRequestEventProcessor implements EventProcessor {

  private static final Logger LOG = LogManager.getLogger(HttpRequestEventProcessor.class);
  private HttpRequestMetricsSnapshot httpSnap;
  private BatchBindStep handle;
  private long startTime;
  private long endTime;

  private HttpRequestEventProcessor(HttpRequestMetricsSnapshot httpSnap) {
    this.httpSnap = httpSnap;
  }

  static HttpRequestEventProcessor buildHttpRequestMetricEventsProcessor(
      long currWindowStartTime,
      long currWindowEndTime,
      Connection conn,
      NavigableMap<Long, HttpRequestMetricsSnapshot> httpRqMetricsMap)
      throws Exception {
    if (httpRqMetricsMap.get(currWindowStartTime) == null) {
      HttpRequestMetricsSnapshot httpRqSnap =
          new HttpRequestMetricsSnapshot(conn, currWindowStartTime);
      Map.Entry<Long, HttpRequestMetricsSnapshot> entry = httpRqMetricsMap.lastEntry();
      if (entry != null) {
        httpRqSnap.rolloverInflightRequests(entry.getValue());
      }
      httpRqMetricsMap.put(currWindowStartTime, httpRqSnap);
      return new HttpRequestEventProcessor(httpRqSnap);
    } else {
      return new HttpRequestEventProcessor(httpRqMetricsMap.get(currWindowStartTime));
    }
  }

  public void initializeProcessing(long startTime, long endTime) {
    this.startTime = startTime;
    this.endTime = endTime;
    this.handle = httpSnap.startBatchPut();
  }

  public void finalizeProcessing() {
    if (handle.size() > 0) {
      handle.execute();
    }
  }

  public void processEvent(Event event) {
    String[] keyItems = event.key.split(File.separatorChar == '\\' ? "\\\\" : File.separator);
    // If the item in the index 1 is http, then proceed.
    if (keyItems[1].equals(PerformanceAnalyzerMetrics.sHttpPath)) {
      if (keyItems[4].equals(PerformanceAnalyzerMetrics.START_FILE_NAME)) {
        emitStartHttpMetric(event, keyItems);
      } else if (keyItems[4].equals(PerformanceAnalyzerMetrics.FINISH_FILE_NAME)) {
        emitFinishHttpMetric(event, keyItems);
      }
    }
  }

  @Override
  public boolean shouldProcessEvent(Event event) {
    return event.key.contains(PerformanceAnalyzerMetrics.sHttpPath);
  }

  @Override
  public void commitBatchIfRequired() {
    if (handle.size() > BATCH_LIMIT) {
      handle.execute();
      handle = httpSnap.startBatchPut();
    }
  }

  // A keyItem is of the form : [threads, http, bulk, 43369, start]
  //
  // Example value part of the entry is:
  // current_time:1566413979979
  // StartTime:1566413979979
  // Indices:
  // HTTP_RequestDocs:10000$
  private void emitStartHttpMetric(Event entry, String[] keyItems) {
    Map<String, String> keyValueMap = MetricsParser.extractEntryData(entry.value);

    String startTimeVal = keyValueMap.get(HttpMetric.START_TIME.toString());
    String itemCountVal = keyValueMap.get(HttpMetric.HTTP_REQUEST_DOCS.toString());
    try {
      long st = Long.parseLong(startTimeVal);

      // This can be null
      String indices = keyValueMap.get(HttpDimension.INDICES.toString());
      long itemCount = Long.parseLong(itemCountVal);
      // A keyItem is of the form : [threads, http, bulk, 43369, start]
      String rid = keyItems[3];
      String operation = keyItems[2];
      handle.bind(rid, operation, indices, null, null, itemCount, st, null);
    } catch (NumberFormatException e) {
      LOG.error(
          "Unable to parse string. StartTime:{}, itemCount:{}, ExcepionCode: {},\n startMetrics:{}",
          startTimeVal,
          itemCountVal,
          StatExceptionCode.READER_PARSER_ERROR.toString(),
          entry.key);
      StatsCollector.instance().logException(StatExceptionCode.READER_PARSER_ERROR);
      throw e;
    }
  }

  // A keyItem is of the form : [threads, http, bulk, 43369, finish]
  // Example finish value blob
  // current_time:1566413987986
  // FinishTime:1566413987986
  // HTTPRespCode:200
  // Exception:
  private void emitFinishHttpMetric(Event entry, String[] keyItems) {
    Map<String, String> keyValueMap = MetricsParser.extractEntryData(entry.value);

    String finishTimeVal = keyValueMap.get(HttpMetric.FINISH_TIME.toString());
    String status = keyValueMap.get(HttpDimension.HTTP_RESP_CODE.toString());
    String exception = keyValueMap.get(HttpDimension.EXCEPTION.toString());
    try {
      long ft = Long.parseLong(finishTimeVal);
      String rid = keyItems[3];
      String operation = keyItems[2];
      handle.bind(rid, operation, null, status, exception, null, null, ft);
    } catch (NumberFormatException e) {
      LOG.error(
          "Unable to parse string. FinishTime:{} ExcepionCode: {} \n finishMetrics:{}",
          finishTimeVal,
          StatExceptionCode.READER_PARSER_ERROR.toString(),
          entry.key);
      StatsCollector.instance().logException(StatExceptionCode.READER_PARSER_ERROR);
      throw e;
    }
  }
}
