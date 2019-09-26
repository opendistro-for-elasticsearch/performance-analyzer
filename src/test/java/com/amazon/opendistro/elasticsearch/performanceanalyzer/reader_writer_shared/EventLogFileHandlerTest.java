package com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared;

import java.io.File;

import org.junit.Before;
import org.junit.Test;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.HeapMetricsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.writer.EventLogQueueProcessor;

public class EventLogFileHandlerTest {
    String pathToTestMetricsDir;

    @Before
    public void init() {
        pathToTestMetricsDir = "/tmp/testMetrics/";
        deleteDirectory(new File(pathToTestMetricsDir));
        boolean newDir = new File(pathToTestMetricsDir).mkdir();
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

        for (int i=0; i<count; i++) {
            heapMetricsCollector.collectMetrics(currTime);
        }

        EventLog eventLog = new EventLog();
        EventLogFileHandler eventLogFileHandler = new EventLogFileHandler(eventLog, pathToTestMetricsDir);
        EventLogQueueProcessor queuePurgerAndPersistor = new EventLogQueueProcessor(eventLogFileHandler,
                MetricsConfiguration.SAMPLING_INTERVAL, MetricsConfiguration.SAMPLING_INTERVAL);
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
