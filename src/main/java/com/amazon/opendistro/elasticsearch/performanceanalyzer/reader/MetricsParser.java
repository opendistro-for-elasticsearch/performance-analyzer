/*
 * Copyright <2019> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.BatchBindStep;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.OSMetricsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.HttpDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.HttpMetric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.OSMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ShardBulkDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ShardBulkMetric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CommonMetric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.MasterMetricDimensions;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.FileHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatExceptionCode;

/**
 * Read metrics files emitted by Elasticsearch in /dev/shm and efficiently load them into tables for further processing.
 */
public class MetricsParser {
    private static final Logger LOG = LogManager.getLogger(MetricsParser.class);

    public long getThirtySecondBucket(long startTime) {
        return PerformanceAnalyzerMetrics.getTimeInterval(startTime);
    }

    public void parseOSMetrics(String rootLocation, long startTime, long endTime,
                               OSMetricsSnapshot osMetricsSnap) throws Exception {
        long startTimeThirtySecondBucket = getThirtySecondBucket(startTime);

        File threadsFile = new File(rootLocation + File.separator
                + startTimeThirtySecondBucket + File.separator
                + PerformanceAnalyzerMetrics.sThreadsPath);

        BatchBindStep handle = osMetricsSnap.startBatchPut();
        List<String> tidToDelete = new ArrayList<>();
        processOSMetricsForFile(threadsFile, osMetricsSnap, startTime, endTime, handle, tidToDelete);

        osMetricsSnap.deleteByTid(tidToDelete);

        if (handle.size() > 0) {
            handle.execute();
        }
    }

    public void processOSMetricsForFile(File threadsFile,
                                        OSMetricsSnapshot osMetricsSnap, long startTime, long endTime,
                                        BatchBindStep batchHandle, List<String> tidToDelete) throws Exception {

        Map<String, Long> lastUpdateTimePerTid = osMetricsSnap.getLastUpdateTimePerTid();

        boolean retVal = false;
        if (threadsFile.exists()) {
            for (File threadIDFile: threadsFile.listFiles()) {
                if (!threadIDFile.getName().equals(PerformanceAnalyzerMetrics.sHttpPath)) {
                    String threadID = threadIDFile.getName();

                    for (File opFile: threadIDFile.listFiles()) {
                        if (opFile.getName().equals(PerformanceAnalyzerMetrics.sOSPath)) {
                            retVal = processOSMetrics(opFile, threadID, lastUpdateTimePerTid, startTime,
                                    endTime, batchHandle, tidToDelete)
                                    || retVal;
                        }
                    }
                }
            }
        }
        LOG.info("processOSMetricsForFile ret: {}", retVal);;
    }

    public void parseRequestMetrics(String rootLocation, long startTime,
            long endTime, ShardRequestMetricsSnapshot rqMetricsSnap) throws Exception {

        long startTimeThirtySecondBucket = getThirtySecondBucket(startTime);
        File threadsFile = new File(rootLocation + File.separator
                + startTimeThirtySecondBucket + File.separator
                + PerformanceAnalyzerMetrics.sThreadsPath);

        BatchBindStep handle = rqMetricsSnap.startBatchPut();
        if (threadsFile.exists()) {
            for (File threadIDFile: threadsFile.listFiles()) {
                if (!threadIDFile.getName().equals(PerformanceAnalyzerMetrics.sHttpPath)) {
                    String threadID = threadIDFile.getName();

                    for (File opFile: threadIDFile.listFiles()) {
                        if (opFile.getName().equals(PerformanceAnalyzerMetrics.sShardBulkPath)
                                || opFile.getName().equals(PerformanceAnalyzerMetrics.sShardFetchPath)
                                || opFile.getName().equals(PerformanceAnalyzerMetrics.sShardQueryPath)) {
                            handleESMetrics(opFile, threadID, startTime, endTime, handle);
                        }
                    }
                }
            }
        }

        if (handle.size() > 0) {
            handle.execute();
        }
    }

    public void parseHttpMetrics(String rootLocation, long startTime,
            long endTime, HttpRequestMetricsSnapshot rqMetricsSnap) throws Exception {

        long startTimeThirtySecondBucket = getThirtySecondBucket(startTime);
        File httpFile = new File(rootLocation + File.separator
                + startTimeThirtySecondBucket + File.separator
                + PerformanceAnalyzerMetrics.sThreadsPath + File.separator + PerformanceAnalyzerMetrics.sHttpPath);

        BatchBindStep handle = rqMetricsSnap.startBatchPut();

        if (httpFile.exists()) {
            for (File opFile: httpFile.listFiles()) {
                String operation = opFile.getName();
                for (File rFile: opFile.listFiles()) {
                    String requestId = rFile.getName();
                    for (File metricsFile: rFile.listFiles()) {
                        long lastModified = FileHelper.getLastModified(metricsFile, startTime, endTime);
                        if (lastModified < startTime || lastModified >= endTime) {
                            continue;
                        }
                        try {
                            if (metricsFile.getName().equals(PerformanceAnalyzerMetrics.START_FILE_NAME)) {
                                emitStartHttpMetric(metricsFile, requestId, operation, handle);
                            } else  if (metricsFile.getName().equals(PerformanceAnalyzerMetrics.FINISH_FILE_NAME)) {
                                emitFinishHttpMetric(metricsFile, requestId, operation, handle);
                            }
                        } catch (Exception e) {
                            LOG.error(e, e);
                            LOG.error("Error parsing file - {} ExcepionCode: {}\n",
                                       metricsFile.getAbsolutePath(), StatExceptionCode.READER_PARSER_ERROR.toString());
                            StatsCollector.instance().logException(StatExceptionCode.READER_PARSER_ERROR);
                            throw e;
                        }
                    }
                }
            }
        }

        if (handle.size() > 0) {
            handle.execute();
        }
    }

    public void parseMasterEventMetrics(String rootLocation,
                                        long startTime,
                                        long endTime,
                                        MasterEventMetricsSnapshot masterEventMetricsSnapshot) {

        long startTimeThirtySecondBucket = getThirtySecondBucket(startTime);
        File threadsFile = new File(rootLocation + File.separator
                + startTimeThirtySecondBucket + File.separator
                + PerformanceAnalyzerMetrics.sThreadsPath);

        BatchBindStep handle = masterEventMetricsSnapshot.startBatchPut();
        if (threadsFile.exists()) {

            try {
                // Perform level order traversal on file directories
                Queue<File> queue = new LinkedList<>();
                Queue<String> idQueue = new LinkedList<>();

                expandThreadDirectory(threadsFile, queue);
                expandThreadIDDirectory(queue, idQueue);
                expandOperationDirectory(queue, idQueue);
                expandInsertOrderDirectory(queue, idQueue);

                emitMasterStartFinishMetrics(startTime, endTime, handle, queue, idQueue);
            } catch (Exception e) {
                LOG.error("Failed to parse master metrics with ExceptionCode: " + StatExceptionCode.READER_PARSER_ERROR.toString(),  e);
                StatsCollector.instance().logException(StatExceptionCode.READER_PARSER_ERROR);
            }
        }

        if (handle.size() > 0) {
            handle.execute();
        }
    }

    private void emitMasterStartFinishMetrics(long startTime,
                                              long endTime,
                                              BatchBindStep handle,
                                              Queue<File> queue,
                                              Queue<String> idQueue) {

        // process start and finish
        while (!queue.isEmpty()) {
            File metricsFile = queue.poll();
            String threadID = idQueue.poll();
            String insertOder = idQueue.poll();

            long lastModified = FileHelper.getLastModified(metricsFile, startTime, endTime);
            if (lastModified < startTime || lastModified >= endTime) {
                continue;
            }

            String metrics = PerformanceAnalyzerMetrics.getMetric(metricsFile.getAbsolutePath());
            try {
                if (metricsFile.getName().equals(PerformanceAnalyzerMetrics.START_FILE_NAME)) {
                    emitStartMasterEventMetric(metrics, insertOder, threadID, handle);
                } else  if (metricsFile.getName().equals(PerformanceAnalyzerMetrics.FINISH_FILE_NAME)) {
                    emitEndMasterEventMetric(metrics, insertOder, threadID, handle);
                }
            } catch (Exception e) {
                LOG.error(e, e);
                LOG.error("Error parsing file - {} ExcepionCode: {},\n {}",
                          metricsFile.getAbsolutePath(), StatExceptionCode.READER_PARSER_ERROR.toString(), metrics);
                StatsCollector.instance().logException(StatExceptionCode.READER_PARSER_ERROR);
            }
        }
    }

    private void expandInsertOrderDirectory(Queue<File> queue, Queue<String> idQueue) {

        int size = queue.size();
        for (int i = 0; i < size; i++) {
            File insertOrderFile = queue.poll();
            String threadID = idQueue.poll();
            String insertOder = insertOrderFile.getName();

            for (File metricsFile: insertOrderFile.listFiles()) {
                queue.add(metricsFile);
                idQueue.add(threadID);
                idQueue.add(insertOder);
            }
        }
    }

    private void expandOperationDirectory(Queue<File> queue, Queue<String> idQueue) {

        int size = queue.size();
        for (int i = 0; i < size; i++) {
            File opFile = queue.poll();
            String threadId = idQueue.poll();

            for (File insertOrderFile : opFile.listFiles()) {
                queue.add(insertOrderFile);
                idQueue.add(threadId);
            }
        }
    }

    private void expandThreadIDDirectory(Queue<File> queue, Queue<String> idQueue) {

        int size = queue.size();
        for (int i = 0; i < size; i++) {
            File threadIDFile = queue.poll();
            String threadID = threadIDFile.getName();

            for (File opFile : threadIDFile.listFiles()) {
                if (opFile.getName().equals(PerformanceAnalyzerMetrics.sMasterTaskPath)) {
                    queue.add(opFile);
                    idQueue.add(threadID);
                }
            }
        }
    }

    private void expandThreadDirectory(File threadsFile, Queue<File> queue) {

        for (File threadIDFile: threadsFile.listFiles()) {
            if (!threadIDFile.getName().equals(PerformanceAnalyzerMetrics.sHttpPath)) {
                queue.add(threadIDFile);
            }
        }
    }

    private void emitStartMasterEventMetric(String startMetrics,
                                            String insertOrder,
                                            String threadId,
                                            BatchBindStep handle) {

        String priority = PerformanceAnalyzerMetrics.extractMetricValue(startMetrics,
                MasterMetricDimensions.MASTER_TASK_PRIORITY.toString());

        long st = Long.parseLong(PerformanceAnalyzerMetrics.extractMetricValue(startMetrics,
                CommonMetric.START_TIME.toString()));

        String taskType = PerformanceAnalyzerMetrics.extractMetricValue(startMetrics,
                MasterMetricDimensions.MASTER_TASK_TYPE.toString());

        String taskMetadata = PerformanceAnalyzerMetrics.extractMetricValue(startMetrics,
                MasterMetricDimensions.MASTER_TASK_METADATA.toString());

        long queueTime = Long.parseLong(PerformanceAnalyzerMetrics.extractMetricValue(startMetrics,
                MasterMetricDimensions.MASTER_TASK_QUEUE_TIME.toString()));

        handle.bind(threadId, insertOrder, priority, taskType, taskMetadata, queueTime, st, null);
    }

    private void emitEndMasterEventMetric(String startMetrics,
                                          String insertOrder,
                                          String threadId,
                                          BatchBindStep handle) {

        long finishTime = Long.parseLong(PerformanceAnalyzerMetrics.extractMetricValue(startMetrics,
                CommonMetric.FINISH_TIME.toString()));
        handle.bind(threadId, insertOrder, null, null, null, null, null, finishTime);
    }

    private void emitStartHttpMetric(File metricFile, String rid,
            String operation, BatchBindStep handle) {

        String startMetrics = PerformanceAnalyzerMetrics.getMetric(metricFile.getAbsolutePath());
        String startTimeVal = PerformanceAnalyzerMetrics.extractMetricValue(startMetrics, HttpMetric.START_TIME.toString());
        String itemCountVal = PerformanceAnalyzerMetrics.extractMetricValue(startMetrics, HttpMetric.HTTP_REQUEST_DOCS.toString());
        try {
            long st = Long.parseLong(startTimeVal);
            String indices = PerformanceAnalyzerMetrics.extractMetricValue(startMetrics, HttpDimension.INDICES.toString());
            long itemCount = Long.parseLong(itemCountVal);
            handle.bind(rid, operation, indices, null, null, itemCount, st, null);
        } catch (NumberFormatException e) {
            LOG.error("Unable to parse string. StartTime:{}, itemCount:{}, ExcepionCode: {},\n startMetrics:{}",
                    startTimeVal, itemCountVal, StatExceptionCode.READER_PARSER_ERROR.toString(), startMetrics);
            StatsCollector.instance().logException(StatExceptionCode.READER_PARSER_ERROR);
            throw e;
        }
    }

    private void emitFinishHttpMetric(File metricFile, String rid,
            String operation, BatchBindStep handle) {
        String finishMetrics = PerformanceAnalyzerMetrics.getMetric(metricFile.getAbsolutePath());


        String finishTimeVal = PerformanceAnalyzerMetrics.extractMetricValue(finishMetrics, HttpMetric.FINISH_TIME.toString());
        String status = PerformanceAnalyzerMetrics.extractMetricValue(finishMetrics, HttpDimension.HTTP_RESP_CODE.toString());
        String exception = PerformanceAnalyzerMetrics.extractMetricValue(finishMetrics, HttpDimension.EXCEPTION.toString());
        try {
        long ft = Long.parseLong(finishTimeVal);
        handle.bind(rid, operation, null, status, exception, null, null, ft);
        } catch (NumberFormatException e) {
            LOG.error("Unable to parse string. FinishTime:{} ExcepionCode: {} \n finishMetrics:{}",
                    finishTimeVal, StatExceptionCode.READER_PARSER_ERROR.toString(), finishMetrics);
            StatsCollector.instance().logException(StatExceptionCode.READER_PARSER_ERROR);
            throw e;
        }
    }

    private boolean processOSMetrics(File opFile, String threadID,
                                     Map<String, Long> lastUpdateTimePerTid,
                                     long startTime, long endTime,
                                     BatchBindStep batchHandle, List<String> tidToDelete) throws Exception {
        Map<String, Double> osMetrics = new HashMap<>();
        long opFileLastModified = FileHelper.getLastModified(opFile, startTime, endTime);
        if (opFileLastModified > endTime) {
            opFileLastModified = endTime;
            LOG.info("File last modified time is greater than endTime - {}", opFile.getAbsolutePath());
        }
        //Discard os metrics if the file has not been updated in the 5 second window.
        if (opFileLastModified < startTime) {
            return false;
        }
        //Only put data when opFile.lastModified() is newer than the lastUpdateTime in database.
        //If there is an update, We'll delete existing data and insert new data.
        if (lastUpdateTimePerTid.containsKey(threadID)) {
            if (lastUpdateTimePerTid.get(threadID) == opFileLastModified) {
                return false;
            }
            tidToDelete.add(threadID);
        }

        String sOSMetrics = PerformanceAnalyzerMetrics.getMetric(opFile.getAbsolutePath());
        OSMetrics[] metrics = OSMetrics.values();
        for (OSMetrics metric : metrics) {
            try {
                String metricVal = PerformanceAnalyzerMetrics.extractMetricValue(sOSMetrics, metric.toString());
                if (metricVal != null) {
                    Double val = Double.parseDouble(metricVal);
                    osMetrics.put(metric.toString(), val);
                }
            } catch (Exception e) {
                LOG.error(e, e);
                LOG.error("Error parsing file - {} ExcepionCode: {},\n {}",
                          opFile.getAbsolutePath(), StatExceptionCode.READER_PARSER_ERROR.toString(), sOSMetrics);
                StatsCollector.instance().logException(StatExceptionCode.READER_PARSER_ERROR);
                throw e;
            }
        }

        String threadName = PerformanceAnalyzerMetrics.extractMetricValue(sOSMetrics,
                OSMetricsCollector.MetaDataFields.threadName.toString());

        int numMetrics = metrics.length + 3;
        Object [] metricVals = new Object[numMetrics];
        metricVals[0] = threadID;
        metricVals[1] = threadName;
        for (int i = 2; i < numMetrics - 1; i++) {
            metricVals[i] = osMetrics.get(metrics[i - 2].toString());
        }
        metricVals[numMetrics - 1] =  opFileLastModified;

        batchHandle.bind(metricVals);
        return true;
    }

    private void handleESMetrics(File esMetrics, String threadID,
            long startTime, long endTime, BatchBindStep handle) {
        String operation = esMetrics.getName(); //- shardBulk, shardSearch etc..
        for (File idFile : esMetrics.listFiles()) {
            try {
                handleidFile(idFile, threadID, startTime, endTime, operation, handle);
            } catch (Exception e) {
                LOG.error("Failed to parse ES Metrics with ExcepionCode: " + StatExceptionCode.READER_PARSER_ERROR.toString(), e);
                StatsCollector.instance().logException(StatExceptionCode.READER_PARSER_ERROR);
            }
        }
    }

    private String getPrimary(String primary) {
        return primary == null ? "NA" : (primary.equals("true") ? "primary" : "replica");
    }

    private void emitStartMetric(String startMetrics, String rid, String threadId,
            String operation, BatchBindStep handle) {
        long st = Long.parseLong(PerformanceAnalyzerMetrics.extractMetricValue(startMetrics,
                ShardBulkMetric.START_TIME.toString()));
        String indexName = PerformanceAnalyzerMetrics.extractMetricValue(startMetrics,
                ShardBulkDimension.INDEX_NAME.toString());
        String shardId = PerformanceAnalyzerMetrics.extractMetricValue(startMetrics,
                ShardBulkDimension.SHARD_ID.toString());
        String primary = getPrimary(PerformanceAnalyzerMetrics.extractMetricValue(startMetrics,
                ShardBulkDimension.PRIMARY.toString()));
        String docCountString = PerformanceAnalyzerMetrics.extractMetricValue(startMetrics,
                ShardBulkMetric.ITEM_COUNT.toString());
        long docCount = 0;
        if (docCountString != null) {
            docCount = Long.parseLong(docCountString);
        }
        handle.bind(shardId, indexName, rid, threadId, operation, primary, st, null, docCount);
    }

    private void emitFinishMetric(String finishMetrics, String rid, String threadId,
            String operation, BatchBindStep handle) {
        long ft = Long.parseLong(PerformanceAnalyzerMetrics.extractMetricValue(finishMetrics,
                ShardBulkMetric.FINISH_TIME.toString()));
        String indexName = PerformanceAnalyzerMetrics.extractMetricValue(finishMetrics,
                ShardBulkDimension.INDEX_NAME.toString());
        String shardId = PerformanceAnalyzerMetrics.extractMetricValue(finishMetrics,
                ShardBulkDimension.SHARD_ID.toString());
        String primary = getPrimary(PerformanceAnalyzerMetrics.extractMetricValue(finishMetrics,
                ShardBulkDimension.PRIMARY.toString()));
        handle.bind(shardId, indexName, rid, threadId, operation, primary, null, ft, null);
    }

    private void handleidFile(File idFile, String threadID, long startTime,
            long endTime, String operation, BatchBindStep handle) {
        String rid = idFile.getName();
        long lastModified = FileHelper.getLastModified(idFile, startTime, endTime);
        if (lastModified < startTime || lastModified >= endTime) {
            return;
        }
        for (File metricsFile: idFile.listFiles()) {
            String metrics = PerformanceAnalyzerMetrics.getMetric(metricsFile.getAbsolutePath());
            try {
                if (metricsFile.getName().equals(PerformanceAnalyzerMetrics.START_FILE_NAME)) {
                    emitStartMetric(metrics, rid, threadID, operation, handle);
                } else  if (metricsFile.getName().equals(PerformanceAnalyzerMetrics.FINISH_FILE_NAME)) {
                    emitFinishMetric(metrics, rid, threadID, operation, handle);
                }
            } catch (Exception e) {
                LOG.error(e, e);
                LOG.error("Error parsing file - {},\n {}", metricsFile.getAbsolutePath(), metrics);
                LOG.error("Error parsing file - {} ExcepionCode: {},\n {}",
                          metricsFile.getAbsolutePath(), StatExceptionCode.READER_PARSER_ERROR.toString(), metrics);
                StatsCollector.instance().logException(StatExceptionCode.READER_PARSER_ERROR);
            }
        }
    }
}


