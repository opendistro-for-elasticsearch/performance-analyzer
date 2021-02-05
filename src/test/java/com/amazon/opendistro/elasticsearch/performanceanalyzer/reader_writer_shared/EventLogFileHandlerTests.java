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

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.HeapMetricsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PerformanceAnalyzerController;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.http_action.config.PerformanceAnalyzerConfigAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.writer.EventLogQueueProcessor;
import java.io.File;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class EventLogFileHandlerTests {
    @Mock private PerformanceAnalyzerController mockController;
    @Mock private PerformanceAnalyzerConfigAction configAction;

    String pathToTestMetricsDir;

    @Before
    public void init() {
        initMocks(this);
        pathToTestMetricsDir = "/tmp/testMetrics/";
        deleteDirectory(new File(pathToTestMetricsDir));
        boolean newDir = new File(pathToTestMetricsDir).mkdir();
        when(mockController.isPerformanceAnalyzerEnabled()).thenReturn(true);
        PerformanceAnalyzerConfigAction.setInstance(configAction);
    }

    private boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }

    // @After
    public void cleanup() {
        deleteDirectory(new File(pathToTestMetricsDir));
    }

    private long generateWriterFile(int count) {
        long currTime = System.currentTimeMillis();
        HeapMetricsCollector heapMetricsCollector = new HeapMetricsCollector();

        for (int i = 0; i < count; i++) {
            heapMetricsCollector.collectMetrics(currTime);
        }

        EventLog eventLog = new EventLog();
        EventLogFileHandler eventLogFileHandler = new EventLogFileHandler(eventLog, pathToTestMetricsDir);
        EventLogQueueProcessor queuePurgerAndPersistor = new EventLogQueueProcessor(eventLogFileHandler,
                MetricsConfiguration.SAMPLING_INTERVAL, MetricsConfiguration.SAMPLING_INTERVAL, mockController);
        queuePurgerAndPersistor.purgeQueueAndPersist();
        return PerformanceAnalyzerMetrics.getTimeInterval(currTime);
    }

    @Test
    public void write() {
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");
        generateWriterFile(4);
    }

    @Test
    public void readSmallFile() {
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");
        // String filename = String.valueOf(generateWriterFile(4));
        EventLog eventLog = new EventLog();
        EventLogFileHandler eventLogFileHandler = new EventLogFileHandler(eventLog, "test_files/new_format");
        // Assert.assertEquals(4, eventLogFileHandler.read("1566152850000").size());
    }

    @Test
    public void readLargeFile() {
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");
        String filename = String.valueOf(generateWriterFile(7));
        EventLog eventLog = new EventLog();
        EventLogFileHandler eventLogFileHandler = new EventLogFileHandler(eventLog, pathToTestMetricsDir);
        // eventLogFileHandler.read(filename);
    }


    // TODO: Write a test for case when one single Event is larger than BUFFER_SIZE
}
