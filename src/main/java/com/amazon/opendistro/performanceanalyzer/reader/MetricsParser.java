package com.amazon.opendistro.performanceanalyzer.reader;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import com.amazon.opendistro.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.amazon.opendistro.performanceanalyzer.util.FileHelper;
import org.jooq.BatchBindStep;

import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.Http_Metrics;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.OS_Metrics;
import com.amazon.opendistro.performanceanalyzer.metrics.MetricsConfiguration;

/**
 * Read metrics files emitted by Elasticsearch in /dev/shm and efficiently load them into tables for further processing.
 */
public class MetricsParser {
    private static final Logger LOG = LogManager.getLogger(MetricsParser.class);

    public long getThirtySecondBucket(long startTime) {
        return PerformanceAnalyzerMetrics.getTimeInterval(startTime);
    }

    public boolean parseOSMetrics(String rootLocation,
            long startTime, OSMetricsSnapshot osMetricsSnap, long lastSnapTimestamp) throws Exception {
        long startTimeThirtySecondBucket = getThirtySecondBucket(startTime);
        long prevThirtySecondBucket = startTimeThirtySecondBucket - MetricsConfiguration.ROTATION_INTERVAL;

        File threadsFile = new File(rootLocation + File.separator
                + startTimeThirtySecondBucket + File.separator
                + PerformanceAnalyzerMetrics.sThreadsPath);

        File prevThreadsFile = new File(rootLocation + File.separator
                + prevThirtySecondBucket + File.separator
                + PerformanceAnalyzerMetrics.sThreadsPath);

        BatchBindStep handle = osMetricsSnap.startBatchPut();
        boolean retVal = processOSMetricsForFile(threadsFile, osMetricsSnap, startTime, lastSnapTimestamp, handle);
        boolean prevRetVal = processOSMetricsForFile(prevThreadsFile, osMetricsSnap, startTime, lastSnapTimestamp, handle);

        if (handle.size() > 0) {
            handle.execute();
        }
        return retVal || prevRetVal;
    }

    public boolean processOSMetricsForFile(File threadsFile,
            OSMetricsSnapshot osMetricsSnap, long startTime, long lastSnapTimestamp,
            BatchBindStep batchHandle) throws Exception {
        boolean retVal = false;
        if (threadsFile.exists()) {
            for (File threadIDFile: threadsFile.listFiles()) {
                if (!threadIDFile.getName().equals(PerformanceAnalyzerMetrics.sHttpPath)) {
                    String threadID = threadIDFile.getName();

                    for (File opFile: threadIDFile.listFiles()) {
                        if (opFile.getName().equals(PerformanceAnalyzerMetrics.sOSPath)) {
                            retVal = processOSMetrics(opFile, threadID, osMetricsSnap, startTime,
                                    lastSnapTimestamp, batchHandle)
                                || retVal;
                        }
                    }
                }
            }
        }
        return retVal;
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
                            LOG.error("Error parsing file - {},\n {}", metricsFile.getAbsolutePath());
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

    private void emitStartHttpMetric(File metricFile, String rid,
            String operation, BatchBindStep handle) {
        String startMetrics = PerformanceAnalyzerMetrics.getMetric(metricFile.getAbsolutePath());
        String startTimeVal = PerformanceAnalyzerMetrics.extractMetricValue(startMetrics, Http_Metrics.startTime.name());
        String itemCountVal = PerformanceAnalyzerMetrics.extractMetricValue(startMetrics, Http_Metrics.itemCount.name());
        try {
            long st = Long.parseLong(startTimeVal);
            String indices = PerformanceAnalyzerMetrics.extractMetricValue(startMetrics, Http_Metrics.indices.name());
            long itemCount = Long.parseLong(itemCountVal);
            handle.bind(rid, operation, indices, null, null, itemCount, st, null);
        } catch (NumberFormatException e) {
            LOG.error("Unable to parse string. StartTime:{}, itemCount:{},\n startMetrics:{}",
                    startTimeVal, itemCountVal, startMetrics);
            throw e;
        }
    }

    private void emitFinishHttpMetric(File metricFile, String rid,
            String operation, BatchBindStep handle) {
        String finishMetrics = PerformanceAnalyzerMetrics.getMetric(metricFile.getAbsolutePath());

        String finishTimeVal = PerformanceAnalyzerMetrics.extractMetricValue(finishMetrics, Http_Metrics.finishTime.name());
        String status = PerformanceAnalyzerMetrics.extractMetricValue(finishMetrics, Http_Metrics.status.name());
        String exception = PerformanceAnalyzerMetrics.extractMetricValue(finishMetrics, Http_Metrics.exception.name());
        try {
        long ft = Long.parseLong(finishTimeVal);
        handle.bind(rid, operation, null, status, exception, null, null, ft);
        } catch (NumberFormatException e) {
            LOG.error("Unable to parse string. FinishTime:{}\n finishMetrics:{}",
                    finishTimeVal, finishMetrics);
            throw e;
        }
    }

    private boolean processOSMetrics(File opFile, String threadID,
            OSMetricsSnapshot osMetricsSnap, long startTime, long lastSnapTimestamp,
            BatchBindStep batchHandle) throws Exception {
        Map<String, Double> osMetrics = new HashMap<>();
        long opFileLastModified = FileHelper.getLastModified(opFile, startTime, lastSnapTimestamp);
        //Only consider os metrics if the file has been updated in the 5 second window.
        if (opFileLastModified > startTime || opFileLastModified <= lastSnapTimestamp) {
            return false;
        }

        String sOSMetrics = PerformanceAnalyzerMetrics.getMetric(opFile.getAbsolutePath());
        OS_Metrics[] metrics = OS_Metrics.values();
        for (OS_Metrics metric : metrics) {
            try {
                String metricVal = PerformanceAnalyzerMetrics.extractMetricValue(sOSMetrics, metric.name());
                if (metricVal != null) {
                    Double val = Double.parseDouble(metricVal);
                    osMetrics.put(metric.name(), val);
                }
            } catch (Exception e) {
                LOG.error(e, e);
                LOG.error("Error parsing file - {},\n {}", opFile.getAbsolutePath(), sOSMetrics);
                throw e;
            }
        }

        String threadName = PerformanceAnalyzerMetrics.extractMetricValue(sOSMetrics, "threadName");

        int numMetrics = metrics.length + 2;
        Object [] metricVals = new Object[numMetrics];
        metricVals[0] = threadID;
        metricVals[1] = threadName;
        for (int i = 2; i < numMetrics; i++) {
            metricVals[i] = osMetrics.get(metrics[i - 2].name());
        }

        batchHandle.bind(metricVals);
        if (osMetricsSnap.getLastUpdatedTime() < opFile.lastModified()) {
            osMetricsSnap.setLastUpdatedTime(opFile.lastModified());
        }
        return true;
    }

    private void handleESMetrics(File esMetrics, String threadID,
            long startTime, long endTime, BatchBindStep handle) {
        String operation = esMetrics.getName(); //- shardBulk, shardSearch etc..
        for (File idFile : esMetrics.listFiles()) {
            try {
                handleidFile(idFile, threadID, startTime, endTime, operation, handle);
            } catch (Exception e) {
                LOG.error("Failed to parse ES Metrics", e);
            }
        }
    }

    private String getPrimary(String primary) {
        return primary == null ? "NA" : (primary.equals("true") ? "primary" : "replica");
    }

    private void emitStartMetric(String startMetrics, String rid, String threadId,
            String operation, BatchBindStep handle) {
        long st = Long.parseLong(PerformanceAnalyzerMetrics.extractMetricValue(startMetrics, "startTime"));
        String indexName = PerformanceAnalyzerMetrics.extractMetricValue(startMetrics, "indexName");
        String shardId = PerformanceAnalyzerMetrics.extractMetricValue(startMetrics, "shardId");
        String primary = getPrimary(PerformanceAnalyzerMetrics.extractMetricValue(startMetrics, "primary"));
        handle.bind(shardId, indexName, rid, threadId, operation, primary, st, null);
    }

    private void emitFinishMetric(String finishMetrics, String rid, String threadId,
            String operation, BatchBindStep handle) {
        long ft = Long.parseLong(PerformanceAnalyzerMetrics.extractMetricValue(finishMetrics, "finishTime"));
        String indexName = PerformanceAnalyzerMetrics.extractMetricValue(finishMetrics, "indexName");
        String shardId = PerformanceAnalyzerMetrics.extractMetricValue(finishMetrics, "shardId");
        String primary = getPrimary(PerformanceAnalyzerMetrics.extractMetricValue(finishMetrics, "primary"));
        handle.bind(shardId, indexName, rid, threadId, operation, primary, null, ft);
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
                if (metricsFile.getName().equals("start")) {
                    emitStartMetric(metrics, rid, threadID, operation, handle);
                } else  if (metricsFile.getName().equals("finish")) {
                    emitFinishMetric(metrics, rid, threadID, operation, handle);
                }
            } catch (Exception e) {
                LOG.error(e, e);
                LOG.error("Error parsing file - {},\n {}", metricsFile.getAbsolutePath(), metrics);
            }
        }
    }
}

