package com.amazon.opendistro.elasticsearch.performanceanalyzer.reader;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentSkipListMap;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Tests to make sure that the MetricsDB files are correctly deleted.
 */
public class ReaderTrimDBTests {
    NavigableMap<Long, MetricsDB> metricsDBMap;
    private Vector<String> fileNames;

    @Before
    public void setUp() {
        metricsDBMap = new ConcurrentSkipListMap<>();
        fileNames = new Vector<>();
        System.setProperty("java.io.tmpdir", "/tmp");
    }

    @After
    public void tearDown() {
        for (String file : fileNames) {
            File dbFile = new File(file);
        }
    }

    private void createDB(long startTime) throws Exception {
        MetricsDB db = new MetricsDB(startTime);
        metricsDBMap.put(startTime, new MetricsDB(startTime));
        fileNames.add(db.getDBFilePath());

    }

    private void createNDBs(int start, int number) throws Exception {
        for (long i = start; i < start + number; i++) {
            createDB(i);
        }
    }

    /**
     * The trimDatabase removes all the in-memory entries from the map provided to it as an argument, leaving behind
     * the maxFiles number of entries. This essentially tests that. Deleting the in-memory entries may or may not
     * clean up the files from the disk. This is controlled by the the third argument. If set to false, the the files
     * on disk are left behind. So essentially, no files will be deleted.
     *
     * @throws Exception if creating metricsDB file fails
     */
    @Test
    public void testMetricsDBFilesNotCleaned() throws Exception {
        int maxFiles = 2;
        int numDBs = 10;

        createNDBs(0, numDBs);
        assertEquals(numDBs, metricsDBMap.size());
        assertEquals(fileNames.size(), numDBs);

        // Goal is to clean up the in-memory entries leaving behind the maxFiles most recent ones, but to leave the
        // on disk files untouched.
        ReaderMetricsProcessor.trimDatabases(metricsDBMap, maxFiles, false);

        // Because deleteDBFiles is set to false, we expect no files to be deleted.
        for (String file : fileNames) {
            File dbFile = new File(file);
            assertTrue(dbFile.exists());
        }

        // We check to see that only maxFiles entries are left.
        assertEquals(metricsDBMap.size(), maxFiles);

        // Add a few more files.
        createNDBs(numDBs, numDBs);
        assertEquals(numDBs + maxFiles, metricsDBMap.size());
        assertEquals(numDBs * 2, fileNames.size());

        // All the old and the newly created on disk files should not have been cleaned up by this call.
        ReaderMetricsProcessor.trimDatabases(metricsDBMap, maxFiles, false);

        // Because deleteDBFiles is set to false, we expect no files to be deleted.
        for (String file : fileNames) {
            File dbFile = new File(file);
            assertTrue(dbFile.exists());
        }

        // We check to see that only maxFiles entries are left.
        assertEquals(metricsDBMap.size(), maxFiles);
    }

    /**
     * A counterpart of the testMetricsDBFilesNotCleaned test but checks that the files are cleaned up when the
     * deleteDBFiles boolean is set to true.
     *
     * @throws Exception if creating metricsDB file fails
     */
    @Test
    public void testMetricsDBFilesCleaned() throws Exception {
        int maxFiles = 2;
        int numDBs = 10;

        createNDBs(0, numDBs);
        assertEquals(metricsDBMap.size(), numDBs);
        assertEquals(fileNames.size(), numDBs);

        // The idea is to clean up the in memory entries and the on disk files leaving behind only the maxFiles
        // number of entries.
        ReaderMetricsProcessor.trimDatabases(metricsDBMap, maxFiles, true);

        // Because deleteDBFiles is set to true, we expect only maxFiles number of files remaining.
        int count = 0;
        for (String file : fileNames) {
            File dbFile = new File(file);
            if (count < numDBs - maxFiles) {
                assertFalse(dbFile.exists());
            } else {
                assertTrue(dbFile.exists());
            }
            count++;
        }

        assertEquals(metricsDBMap.size(), maxFiles);
        fileNames.removeAllElements();

        for (Map.Entry<Long, MetricsDB> pair : metricsDBMap.entrySet()) {
            fileNames.add(pair.getValue().getDBFilePath());
        }

        createNDBs(numDBs, numDBs);
        assertEquals(metricsDBMap.size(), numDBs + maxFiles);
        assertEquals(fileNames.size(), numDBs + maxFiles);

        ReaderMetricsProcessor.trimDatabases(metricsDBMap, maxFiles, true);

        count = 0;
        // Because deleteDBFiles is set to true, we expect only maxFiles number of files remaining.
        for (String file : fileNames) {
            File dbFile = new File(file);
            if (count < numDBs) {
                assertFalse("NOT expected to find file: " + file, dbFile.exists());
            } else {
                assertTrue("Expected to find file: " + file, dbFile.exists());
            }
            count++;
        }
    }
}
