package com.amazon.opendistro.elasticsearch.performanceanalyzer.reader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ESResources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.EventLogFileHandler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.EventLog;

@Ignore
public class NewFormatProcessorTest {
    private static final String DUMP_DIR = "/tmp";

    @BeforeClass
    public static void unzipFiles() {
        // Unzip files in the /tmp directory.
        String newFilesZipPath = "test_files/new_format/new_format.tar.gz";
        String oldFilesZipPath = "test_files/old_format/old_format.tar.gz";

        String[] newFilesCreationCmd = {"tar", "-xf", newFilesZipPath, "-C", DUMP_DIR};
        String[] oldFilesCreationCmd = {"tar", "-xf", oldFilesZipPath, "-C", DUMP_DIR};


        if (!Files.exists(Paths.get(DUMP_DIR + "/old_format"))) {
            try {
                Runtime.getRuntime().exec(oldFilesCreationCmd);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (!Files.exists(Paths.get(DUMP_DIR + "/new_format"))) {
            try {
                Runtime.getRuntime().exec(newFilesCreationCmd);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void removeFilesAndDir(Path path) throws IOException {
        Files.walk(path)
                .map(Path::toFile)
                .sorted(Comparator.reverseOrder())
                .forEach(File::delete);

    }
    // @AfterClass
    public static void removeFiles() throws IOException {
        removeFilesAndDir(Paths.get(DUMP_DIR + "/old_format"));
        removeFilesAndDir(Paths.get(DUMP_DIR + "/new_format"));
    }

    @Before
    public void init() {
        ESResources.INSTANCE.setPluginFileLocation("");
    }

    //@Test
    public void processNewMetricsFormat() throws Exception {
        Path path = Paths.get(System.getProperty("user.dir"));
        Path configPath = Paths.get(path.toString(), "test_files/new_format/performance-analyzer.properties");
        System.out.println("==" + configPath + "==");
        System.setProperty("configFilePath", configPath.toString());

        ReaderMetricsProcessor readerMetricsProcessor = new ReaderMetricsProcessor(
                DUMP_DIR + "/new_format", true);

        EventLogFileHandler eventLogFileHandler = new EventLogFileHandler(new EventLog(), DUMP_DIR + "/new_format");
        // Map<String, List<Event>> metricsDataMap =
                // eventLogFileHandler.read(String.valueOf(1566413970000L));
        readerMetricsProcessor.processMetrics(DUMP_DIR + "/new_format", 1566413970000L);
    }

    //@Test
    public void processOldMetricsFormat() throws Exception {
        Path path = Paths.get(System.getProperty("user.dir"));
        Path configPath = Paths.get(path.toString(), "test_files/old_format/performance-analyzer.properties");
        System.out.println("==" + configPath + "==");
        System.setProperty("configFilePath", configPath.toString());

        ReaderMetricsProcessor readerMetricsProcessor = new ReaderMetricsProcessor(DUMP_DIR + "/old_format");
        readerMetricsProcessor.processMetrics(DUMP_DIR + "/old_format", 1566413970000L);
    }

    private ReaderMetricsProcessorDummy createReaderMetricsProcessor(DATA_FORMAT format) throws Exception {
        Path path = Paths.get(System.getProperty("user.dir"));
        Path configPath = Paths.get(path.toString(), "test_files/" + format.format + "/performance-analyzer.properties");
        System.out.println("==" + configPath + "==");
        System.setProperty("configFilePath", configPath.toString());

        return new ReaderMetricsProcessorDummy(format);
    }

    // The idea is to call processNodeMetrics with the old format and the new format and compare the results.
    @Test
    public void nodeMetricsComparison() throws Exception {
        ReaderMetricsProcessorDummy oldFormatProcessor = createReaderMetricsProcessor(DATA_FORMAT.OLD);
        ReaderMetricsProcessorDummy newFormatProcessor = createReaderMetricsProcessor(DATA_FORMAT.NEW);

        newFormatProcessor.parseNodeMetrics(1566413970000L);
        oldFormatProcessor.parseNodeMetrics(1566413970000L);
        Map<AllMetrics.MetricName, NavigableMap<Long, MemoryDBSnapshot>> oldFormatNodeMetricsMap =
                oldFormatProcessor.getNodeMetricsMap();
        Map<AllMetrics.MetricName, NavigableMap<Long, MemoryDBSnapshot>> newFormatNodeMetricsMap =
                newFormatProcessor.getNodeMetricsMap();

        compareNodeMetricsMaps(oldFormatNodeMetricsMap, newFormatNodeMetricsMap);
    }

    //@Test
    public void compareRequestMetricsSnapshots() throws Exception {
        ReaderMetricsProcessorDummy oldFormatProcessor = createReaderMetricsProcessor(DATA_FORMAT.OLD);
        ReaderMetricsProcessorDummy newFormatProcessor = createReaderMetricsProcessor(DATA_FORMAT.NEW);

        newFormatProcessor.parseRequestMetrics(1566413970000L);
        oldFormatProcessor.parseRequestMetrics(1566413970000L);
        NavigableMap<Long, ShardRequestMetricsSnapshot> oldFormatReqMetricsMap =
                oldFormatProcessor.getShardRequestMetricsMap();
        NavigableMap<Long, ShardRequestMetricsSnapshot> newFormatReqMetricsMap =
                newFormatProcessor.getShardRequestMetricsMap();

        compareReqMetricsMaps(oldFormatReqMetricsMap, newFormatReqMetricsMap);
    }

    //@Test
    public void compareHttpRequestMetricsSnapshots() throws Exception {
        ReaderMetricsProcessorDummy oldFormatProcessor = createReaderMetricsProcessor(DATA_FORMAT.OLD);
        ReaderMetricsProcessorDummy newFormatProcessor = createReaderMetricsProcessor(DATA_FORMAT.NEW);

        oldFormatProcessor.parseHttpRequestMetrics(1566413970000L);
        newFormatProcessor.parseHttpRequestMetrics(1566413970000L);
        NavigableMap<Long, HttpRequestMetricsSnapshot> oldFormatReqMetricsMap =
                oldFormatProcessor.getHttpRqMetricsMap();
        NavigableMap<Long, HttpRequestMetricsSnapshot> newFormatReqMetricsMap =
                newFormatProcessor.getHttpRqMetricsMap();

        compareHttpReqMetricsMaps(oldFormatReqMetricsMap, newFormatReqMetricsMap);
    }

    //@Test
    public void compareMasterRequestMetricsSnapshots() throws Exception {
        ReaderMetricsProcessorDummy oldFormatProcessor = createReaderMetricsProcessor(DATA_FORMAT.OLD);
        ReaderMetricsProcessorDummy newFormatProcessor = createReaderMetricsProcessor(DATA_FORMAT.NEW);

        oldFormatProcessor.parseMasterEventMetrics(1566413970000L);
        newFormatProcessor.parseMasterEventMetrics(1566413970000L);
        NavigableMap<Long, MasterEventMetricsSnapshot> oldFormatReqMetricsMap =
                oldFormatProcessor.getMasterEventMetricsMap();
        NavigableMap<Long, MasterEventMetricsSnapshot> newFormatReqMetricsMap =
                newFormatProcessor.getMasterEventMetricsMap();

        compareMasterReqMetricsMaps(oldFormatReqMetricsMap, newFormatReqMetricsMap);
    }

    //@Test
    public void compareOsMetricsSnapshot() throws Exception {
        ReaderMetricsProcessorDummy oldFormatProcessor = createReaderMetricsProcessor(DATA_FORMAT.OLD);
        ReaderMetricsProcessorDummy newFormatProcessor = createReaderMetricsProcessor(DATA_FORMAT.NEW);

        newFormatProcessor.parseOsMetrics(1566413970000L);
        oldFormatProcessor.parseOsMetrics(1566413970000L);
        NavigableMap<Long, OSMetricsSnapshot> oldFormatNodeMetricsMap =
                oldFormatProcessor.getOsMetricsMap();
        NavigableMap<Long, OSMetricsSnapshot> newFormatNodeMetricsMap =
                newFormatProcessor.getOsMetricsMap();

        compareOsMetricsMaps(oldFormatNodeMetricsMap, newFormatNodeMetricsMap);
    }

    private void compareOsMetricsMaps(NavigableMap<Long, OSMetricsSnapshot> old,
                                      NavigableMap<Long, OSMetricsSnapshot> nw) {
        Assert.assertEquals(old.size(), nw.size());
        String oldFilename = "old.txt";
        String newFilename = "new.txt";
        for (NavigableMap.Entry<Long, OSMetricsSnapshot> oldEntry: old.entrySet()) {
            OSMetricsSnapshot newSnap =  nw.get(oldEntry.getKey());
            try {
                PrintWriter oldf = new PrintWriter(oldFilename);
                oldf.println(oldEntry.getValue().fetchAll());
                System.out.println("OLDD");
                System.out.println(oldEntry.getValue().fetchAll());

                PrintWriter newf = new PrintWriter(newFilename);
                newf.println(newSnap.fetchAll());
                System.out.println("NEWW");
                System.out.println(newSnap.fetchAll());
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                Assert.fail();
            }
            compareFiles(oldFilename, newFilename);
        }

    }
    private void compareReqMetricsMaps(NavigableMap<Long, ShardRequestMetricsSnapshot> old,
                                      NavigableMap<Long, ShardRequestMetricsSnapshot> nw) {
        Assert.assertEquals(old.size(), nw.size());
        String oldFilename = "old.txt";
        String newFilename = "new.txt";
        for (NavigableMap.Entry<Long, ShardRequestMetricsSnapshot> oldEntry: old.entrySet()) {
            ShardRequestMetricsSnapshot newSnap =  nw.get(oldEntry.getKey());
            try {
                PrintWriter oldf = new PrintWriter(oldFilename);
                oldf.println(oldEntry.getValue().fetchAll());

                PrintWriter newf = new PrintWriter(newFilename);
                newf.println(newSnap.fetchAll());
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                Assert.fail();
            }
            compareFiles(oldFilename, newFilename);
        }

    }

    private void compareHttpReqMetricsMaps(NavigableMap<Long, HttpRequestMetricsSnapshot> old,
                                       NavigableMap<Long, HttpRequestMetricsSnapshot> nw) {
        Assert.assertEquals(old.size(), nw.size());
        String oldFilename = "old.txt";
        String newFilename = "new.txt";
        for (NavigableMap.Entry<Long, HttpRequestMetricsSnapshot> oldEntry: old.entrySet()) {
            HttpRequestMetricsSnapshot newSnap =  nw.get(oldEntry.getKey());
            try {
                PrintWriter oldf = new PrintWriter(oldFilename);
                oldf.println(oldEntry.getValue().fetchAll());

                PrintWriter newf = new PrintWriter(newFilename);
                newf.println(newSnap.fetchAll());
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                Assert.fail();
            }
            compareFiles(oldFilename, newFilename);
        }

    }

    private void compareMasterReqMetricsMaps(NavigableMap<Long, MasterEventMetricsSnapshot> old,
                                           NavigableMap<Long, MasterEventMetricsSnapshot> nw) {
        Assert.assertEquals(old.size(), nw.size());
        String oldFilename = "old.txt";
        String newFilename = "new.txt";
        for (NavigableMap.Entry<Long, MasterEventMetricsSnapshot> oldEntry: old.entrySet()) {
            MasterEventMetricsSnapshot newSnap =  nw.get(oldEntry.getKey());
            try {
                PrintWriter oldf = new PrintWriter(oldFilename);
                oldf.println(oldEntry.getValue().fetchAll());

                PrintWriter newf = new PrintWriter(newFilename);
                newf.println(newSnap.fetchAll());
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                Assert.fail();
            }
            compareFiles(oldFilename, newFilename);
        }

    }

    private void compareFiles(String file1, String file2) {
        Path p1 = Paths.get(file1);
        Path p2 = Paths.get(file2);

        try {
            List<String> lines1 = Files.readAllLines(p1);
            List<String> lines2 = Files.readAllLines(p2);
            Assert.assertEquals(lines1.size(), lines2.size());

            for (int i=0; i < lines1.size(); i++) {
                Assert.assertEquals(String.format("(%s)\n!=\n(%s)", lines1.get(i), lines2.get(i)),
                        lines1.get(i), lines2.get(i));
            }
            Files.deleteIfExists(p1);
            Files.deleteIfExists(p2);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void compareNodeMetricsMaps(Map<AllMetrics.MetricName, NavigableMap<Long, MemoryDBSnapshot>> old,
                                        Map<AllMetrics.MetricName, NavigableMap<Long, MemoryDBSnapshot>> nw) {
        Assert.assertEquals(old.size(), nw.size());

        for (Map.Entry<AllMetrics.MetricName, NavigableMap<Long, MemoryDBSnapshot>> entryOld : old.entrySet()) {
            System.out.println("=== " + entryOld.getKey().name() + " ====");
            for (Map.Entry<Long, MemoryDBSnapshot> snapOld: entryOld.getValue().entrySet()) {
                Long kOld = snapOld.getKey();
                System.out.println("For time interval " + kOld);
                MemoryDBSnapshot vOld = snapOld.getValue();
                MemoryDBSnapshot vNew = nw.get(entryOld.getKey()).get(kOld);
                // Assert.assertEquals(v1, v2);
                System.out.println("All OLD data");
                System.out.println(vOld.fetchAll());
                System.out.println("All NEW data");
                System.out.println(vNew.fetchAll());
            }
        }
    }


    private enum DATA_FORMAT {
        OLD("old_format"),
        NEW("new_format");

        String format;

        DATA_FORMAT(String format) {
            this.format = format;
        }
    }

    static class ReaderMetricsProcessorDummy extends ReaderMetricsProcessor {
        private final DATA_FORMAT format;
        private final String rootLocation;

        ReaderMetricsProcessorDummy(DATA_FORMAT format) throws Exception {
            super(DUMP_DIR + "/" + format.format,
                    format == DATA_FORMAT.NEW);
            this.rootLocation = DUMP_DIR + "/" + format.format;
            this.format = format;
        }

        void processMetrics(long timestamp) throws Exception {
            super.processMetrics(rootLocation, timestamp);
        }

        public NodeMetricsEventProcessor parseNodeMetrics(long timestamp) throws Exception {
            switch (format) {
                case OLD:
                    // super.parseNodeMetrics(timestamp);
                    break;
                case NEW:
                    long prevWindowEndTime = timestamp - MetricsConfiguration.SAMPLING_INTERVAL;
                    EventLogFileHandler eventLogFileHandler = new EventLogFileHandler(new EventLog(), rootLocation);
                    // Map<String, List<Event>> cmetricsDataMap =
                    //         eventLogFileHandler.read(String.valueOf(timestamp));
                    // Map<String, List<Event>> pmetricsDataMap =
                    //         eventLogFileHandler.read(String.valueOf(prevWindowEndTime));
                    // super.parseNodeMetrics(timestamp, cmetricsDataMap);
                    break;
            }
            return null;
        }

        void parseOsMetrics(long timestamp) throws Exception {
            long currWindowEndTime = PerformanceAnalyzerMetrics.getTimeInterval(
                    timestamp, MetricsConfiguration.SAMPLING_INTERVAL);
            long prevWindowEndTime = currWindowEndTime - MetricsConfiguration.SAMPLING_INTERVAL;
            switch (format) {
                case OLD:
                    super.parseOSMetrics(rootLocation, currWindowEndTime,
                            currWindowEndTime+ MetricsConfiguration.SAMPLING_INTERVAL);
                    break;
                case NEW:
                    // Map<String, List<Event>> currMetricsDataMap =
                    //         getEventLogFileHandler().read(String.valueOf(currWindowEndTime));
                    // Map<String, List<Event>> lastMetricsDataMap =
                    //         getEventLogFileHandler().read(String.valueOf(prevWindowEndTime));

                    // super.parseOSMetrics(currWindowEndTime,
                    //         currWindowEndTime + MetricsConfiguration.SAMPLING_INTERVAL,
                    //         currMetricsDataMap);
                    break;
            }
        }

        void parseRequestMetrics(long timestamp) throws Exception {
            long currWindowEndTime = PerformanceAnalyzerMetrics.getTimeInterval(
                    timestamp, MetricsConfiguration.SAMPLING_INTERVAL);
            long prevWindowEndTime = currWindowEndTime - MetricsConfiguration.SAMPLING_INTERVAL;
            switch (format) {
                case OLD:
                    super.parseRequestMetrics(rootLocation, prevWindowEndTime, currWindowEndTime);
                    break;
                case NEW:
                    // Map<String, List<Event>> currMetricsDataMap =
                    //         getEventLogFileHandler().read(String.valueOf(prevWindowEndTime));
                    // super.parseRequestMetrics(rootLocation,
                    //    prevWindowEndTime, currWindowEndTim/ e,
                    //        currMetricsDataMap);
                    break;
            }
        }
        void parseHttpRequestMetrics(long timestamp) throws Exception {
            long currWindowEndTime = PerformanceAnalyzerMetrics.getTimeInterval(
                    timestamp, MetricsConfiguration.SAMPLING_INTERVAL);
            long prevWindowEndTime = currWindowEndTime - MetricsConfiguration.SAMPLING_INTERVAL;
            switch (format) {
                case OLD:
                    super.parseHttpRequestMetrics(rootLocation, prevWindowEndTime, currWindowEndTime);
                    break;
                case NEW:
                    // Map<String, List<Event>> currMetricsDataMap =
                    //         getEventLogFileHandler().read(String.valueOf(prevWindowEndTime));
                    // super.parseHttpRequestMetrics(rootLocation,
                    //    prevWindowEndTime, currWindowEndTime,
                    //        currMetricsDataMap);
                    break;
            }
        }
        MasterMetricsEventProcessor parseMasterEventMetrics(long timestamp) {
            long currWindowEndTime = PerformanceAnalyzerMetrics.getTimeInterval(
                    timestamp, MetricsConfiguration.SAMPLING_INTERVAL);
            long prevWindowEndTime = currWindowEndTime - MetricsConfiguration.SAMPLING_INTERVAL;
            switch (format) {
                case OLD:
                    super.parseMasterEventMetrics(rootLocation, prevWindowEndTime, currWindowEndTime);
                    break;
                case NEW:
                    // Map<String, List<Event>> currMetricsDataMap =
                    //         getEventLogFileHandler().read(String.valueOf(prevWindowEndTime));
                    // super.parseMasterEventMetrics(rootLocation, prevWindowEndTime, currWindowEndTime,
                    //         currMetricsDataMap);
                    break;
            }
            return null;
        }
    }
}
