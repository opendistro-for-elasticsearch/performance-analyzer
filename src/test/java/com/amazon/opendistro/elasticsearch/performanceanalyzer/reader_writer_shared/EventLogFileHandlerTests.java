/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.HeapMetricsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PerformanceAnalyzerController;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.http_action.config.PerformanceAnalyzerConfigAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.writer.EventLogQueueProcessor;
import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class EventLogFileHandlerTests {
    @Mock private PerformanceAnalyzerController mockController;
    @Mock private PerformanceAnalyzerConfigAction configAction;

    private final String pathToTestMetricsDir = "/tmp/testMetrics/";
    private final EventLog eventLog = new EventLog();
    private final EventLogFileHandler eventLogFileHandler =
            new EventLogFileHandler(eventLog, pathToTestMetricsDir);
    private EventLogQueueProcessor queuePurgerAndPersistor;

    @Before
    public void init() {
        initMocks(this);
        deleteDirectory(new File(pathToTestMetricsDir));
        boolean newDir = new File(pathToTestMetricsDir).mkdir();
        when(mockController.isPerformanceAnalyzerEnabled()).thenReturn(true);
        PerformanceAnalyzerConfigAction.setInstance(configAction);

        queuePurgerAndPersistor =
                new EventLogQueueProcessor(
                        eventLogFileHandler,
                        MetricsConfiguration.SAMPLING_INTERVAL,
                        MetricsConfiguration.SAMPLING_INTERVAL,
                        mockController);
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");
    }

    @After
    public void cleanup() {
        deleteDirectory(new File(pathToTestMetricsDir));
    }

    @Test
    public void testFileWithMetrics() throws InterruptedException {
        generateWriterFile(5);
        queuePurgerAndPersistor.purgeQueueAndPersist();
        String bucketTime1 = String.valueOf(getTimeBucket());
        assertTrue(
                "Tmp file should be present if metrics are available",
                isFilePresent(getTmpFileName(bucketTime1)));
        Assert.assertEquals(5, checkFileForMetrics(getTmpFileName(bucketTime1)));

        // Generate the next writer file tmp file
        Thread.sleep(5_000);
        generateWriterFile(4);
        queuePurgerAndPersistor.purgeQueueAndPersist();
        String bucketTime2 = String.valueOf(getTimeBucket());
        assertTrue(
                "Tmp file should be present if metrics are available",
                isFilePresent(getTmpFileName(bucketTime2)));
        Assert.assertEquals(4, checkFileForMetrics(getTmpFileName(bucketTime2)));

        // Waiting and calling purgeQueueAndPersist method
        // for tmp file to be renamed to timestamp file
        Thread.sleep(5_000);
        queuePurgerAndPersistor.purgeQueueAndPersist();
        assertTrue(
                "Timestamp file should be present if tmp file is present",
                isFilePresent(bucketTime1));
        Assert.assertEquals(5, checkFileForMetrics(bucketTime1));

        // Waiting and calling purgeQueueAndPersist method
        // for tmp file to be renamed to timestamp file
        Thread.sleep(5_000);
        queuePurgerAndPersistor.purgeQueueAndPersist();
        assertTrue(
                "Timestamp file should be present if tmp file is present",
                isFilePresent(bucketTime1));
        Assert.assertEquals(4, checkFileForMetrics(bucketTime2));
    }

    @Test
    public void testNoFileWithNoMetrics() throws InterruptedException {
        queuePurgerAndPersistor.purgeQueueAndPersist();
        String bucketTime1 = String.valueOf(getTimeBucket());
        assertFalse(
                "Tmp file should not be present if metrics are not available",
                isFilePresent(getTmpFileName(bucketTime1)));

        // Waiting and calling purgeQueueAndPersist method
        // for tmp file to be renamed to timestamp file
        Thread.sleep(5_000);
        queuePurgerAndPersistor.purgeQueueAndPersist();
        Thread.sleep(5_000);
        queuePurgerAndPersistor.purgeQueueAndPersist();
        assertFalse(
                "Timestamp file should not be present if tmp file is not present",
                isFilePresent(bucketTime1));
    }

    // TODO: Write a test for case when one single Event is larger than BUFFER_SIZE

    private boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }

    private long generateWriterFile(int count) {
        long currTime = System.currentTimeMillis();
        HeapMetricsCollector heapMetricsCollector = new HeapMetricsCollector();

        for (int i = 0; i < count; i++) {
            heapMetricsCollector.collectMetrics(currTime);
        }
        return currTime;
    }

    private long getTimeBucket() {
        return PerformanceAnalyzerMetrics.getTimeInterval(
                System.currentTimeMillis(), MetricsConfiguration.SAMPLING_INTERVAL);
    }

    private String getTmpFileName(String filename) {
        return filename + ".tmp";
    }

    private boolean isFilePresent(String filename) {
        Path pathToFile = Paths.get(pathToTestMetricsDir, filename);
        File tempFile = new File(pathToFile.toString());
        return tempFile.exists();
    }

    private long checkFileForMetrics(String filename) {
        Path pathToFile = Paths.get(pathToTestMetricsDir, filename);
        File tempFile = new File(pathToFile.toString());
        long metrics_count = 0;
        try {
            Scanner reader = new Scanner(tempFile);
            while (reader.hasNextLine()) {
                String line = reader.nextLine();
                if (line.contains("current_time")) {
                    metrics_count += 1;
                }
            }
            reader.close();
        } catch (FileNotFoundException e) {
            Assert.fail("File not found filename:" + filename);
        }
        return metrics_count;
    }
}
