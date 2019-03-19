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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;

@SuppressWarnings("checkstyle:constantname")
public class PerformanceAnalyzerMetrics {
    private static final Logger LOG = LogManager.getLogger(PerformanceAnalyzerMetrics.class);
    public static final String sDevShmLocation = PluginSettings.instance().getMetricsLocation();
    public static final String sDevShmScratchLocation = "performanceanalyzer_scratch";
    public static final String sIndicesPath = "indices";
    public static final String sThreadPoolPath = "thread_pool";
    public static final String sThreadsPath = "threads";
    public static final String sCircuitBreakerPath = "circuit_breaker";
    public static final String sShardBulkPath = "shardbulk";
    public static final String sShardFetchPath = "shardfetch";
    public static final String sShardQueryPath = "shardquery";
    public static final String sMasterTaskPath = "master_task";
    public static final String sHttpPath = "http";
    public static final String sOSPath = "os_metrics";
    public static final String sHeapPath = "heap_metrics";
    public static final String sNodesPath = "node_metrics";
    public static final String sPendingTasksPath = "pending_tasks";
    public static final String sDisksPath = "disk_metrics";
    public static final String sTCPPath = "tcp_metrics";
    public static final String sIPPath = "ip_metrics";
    public static final String sKeyValueDelimitor = ":";
    public static final String sMetricNewLineDelimitor = System.getProperty("line.separator");
    public static final String START_FILE_NAME = "start";
    public static final String FINISH_FILE_NAME = "finish";
    public static final String MASTER_CURRENT = "current";
    public static final String MASTER_META_DATA = "metadata";
    public static final String METRIC_CURRENT_TIME = "current_time";

    private static final int NUM_RETRIES_FOR_TMP_FILE = 10;

    private static final boolean IS_METRICS_LOG_ENABLED =
            System.getProperty("performanceanalyzer.metrics.log.enabled", "False").equalsIgnoreCase("True");

    private static final int sTimeInterval =
        MetricsConfiguration.CONFIG_MAP.get(PerformanceAnalyzerMetrics.class).rotationInterval;

    public static long getTimeInterval(long startTime) {
        return getTimeInterval(startTime, sTimeInterval);
    }

    public static long getTimeInterval(long startTime, int timeInterval) {
        return (startTime / timeInterval) * timeInterval;
    }

    public static String getCurrentTimeMetric() {
        return METRIC_CURRENT_TIME + sKeyValueDelimitor + System.currentTimeMillis();
    }

    public static String generatePath(long startTime, String... keysPath) {
        StringBuilder stringBuilder = new StringBuilder(sDevShmLocation);

        stringBuilder.append(String.valueOf(PerformanceAnalyzerMetrics.getTimeInterval(startTime))).append(File.separator);

        for (String key: keysPath) {
            stringBuilder.append(File.separator).append(key);
        }

        return stringBuilder.toString();
    }

    public static void addMetricEntry(StringBuilder value, String metricKey, String metricValue) {
        value.append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(metricKey)
                .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(metricValue);
    }

    public static void addMetricEntry(StringBuilder value, String metricKey, long metricValue) {
        value.append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(metricKey)
                .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(metricValue);
    }

    public static void emitMetric(String keyPath, String value) {
        File file = new File(keyPath);
        if (IS_METRICS_LOG_ENABLED) {
            LOG.info(keyPath + "\n" + value);
        }

        try {
            java.nio.file.Files.createDirectories(file.getParentFile().toPath());
        } catch (IOException ex) {
            LOG.debug(
                    (Supplier<?>) () -> new ParameterizedMessage(
                            "Error In Creating Directories: {} for keyPath:{}",
                            ex.toString(), keyPath),
                    ex);
            return;
        }

        File tmpFile = null;
        try {
            tmpFile = writeToTmp(keyPath, value);
        } catch (Exception ex) {
            LOG.debug(
                    (Supplier<?>) () -> new ParameterizedMessage(
                            "Error in Writing to Tmp File: {} for keyPath:{}",
                            ex.toString(), keyPath),
                    ex);
            return;
        }

        try {
            tmpFile.renameTo(file);
        } catch (Exception ex) {
            LOG.debug(
                    (Supplier<?>) () -> new ParameterizedMessage(
                            "Error in Renaming Tmp File: {} for keyPath:{}",
                            ex.toString(), keyPath),
                    ex);
        }
    }

    private static File writeToTmp(String keyPath, String value) throws Exception {
        int numRetries = 0;

        //- try 10 times to avoid the hash code collision
        while (numRetries < NUM_RETRIES_FOR_TMP_FILE) {
            //- creating a tmp file under: /dev/shm/performanceanalyzer/<TIMESTAMP>/performanceanalyzer_scrtach/
            //- In case rename fails, we don't need to delete this, auto purge will happen when the TIMESTAMP bucket will purged
            //- To avoid collisions, temp file name chosen as:
            //- hashcode of (absolue metric file path + value + current time nano seconds)
            StringBuilder tmp = new StringBuilder().append(keyPath).append(value).append(String.valueOf(System.nanoTime()));
            File file = new File(PerformanceAnalyzerMetrics.generatePath(System.currentTimeMillis(), sDevShmScratchLocation,
                    String.valueOf(tmp.toString().hashCode())));
            java.nio.file.Files.createDirectories(file.getParentFile().toPath());
            if (file.createNewFile()) {
                try (FileOutputStream fos = new FileOutputStream(file);) {
                    fos.write(value.getBytes());
                }
                return file;
            }
            numRetries++;
        }
        throw new Exception("Tmp file not able to create after " + NUM_RETRIES_FOR_TMP_FILE + " retries");
    }

    public static String getMetric(long startTime, String... keysPath) {
        StringBuilder stringBuilder = new StringBuilder(sDevShmLocation);

        stringBuilder.append(String.valueOf(PerformanceAnalyzerMetrics.getTimeInterval(startTime))).append(File.separator);

        for (String key: keysPath) {
            stringBuilder.append(File.separator).append(key);
        }

        return getMetric(stringBuilder.toString());
    }

    public static String getMetric(String keyPath) {
        try {
            return new String(Files.readAllBytes(Paths.get(keyPath)));
        } catch (Exception ex) {
            //-todo logging
//            ex.printStackTrace();
            return "";
        }
    }

    public static String extractMetricValue(String metricVal, String key) {
        int startIndex = metricVal.indexOf(key);

        if (startIndex != -1) {
            startIndex = metricVal.indexOf(sKeyValueDelimitor, startIndex);
            int endIndex = metricVal.indexOf(sMetricNewLineDelimitor, startIndex + 1);

            if (endIndex == -1) {
                endIndex = metricVal.length();
            }
            return metricVal.substring(startIndex + 1, endIndex);
        }
        return null;
    }

    public static void removeMetrics(String keyPath) {
        removeMetrics(new File(keyPath));
    }

    public static void removeMetrics(File keyPathFile) {
        if (keyPathFile.isDirectory()) {
            String[] children = keyPathFile.list();
            for (int i = 0; i < children.length; i++) {
                removeMetrics(new File(keyPathFile, children[i]));
            }
        }
        try {
            keyPathFile.delete();
        } catch (Exception ex) {
            LOG.debug(
                    (Supplier<?>) () -> new ParameterizedMessage(
                            "Error in deleting file: {} for keyPath:{}",
                            ex.toString(), keyPathFile.getAbsolutePath()),
                    ex);
        }
    }

    public static String getJsonCurrentMilliSeconds() {
        return new StringBuilder().append("{\"")
                .append(PerformanceAnalyzerMetrics.METRIC_CURRENT_TIME).append("\"")
                .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor)
                .append(System.currentTimeMillis()).append("}").toString();
    }
}

