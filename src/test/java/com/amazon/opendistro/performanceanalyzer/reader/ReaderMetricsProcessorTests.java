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

package com.amazon.opendistro.performanceanalyzer.reader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FilenameFilter;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.CommonDimension;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.MasterPendingValue;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.MetricName;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.OSMetrics;
import com.amazon.opendistro.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.performanceanalyzer.metricsdb.MetricsDB;

public class ReaderMetricsProcessorTests extends AbstractReaderTests {

    public ReaderMetricsProcessorTests() throws SQLException, ClassNotFoundException {
        super();
    }

    // Disabled on purpose
    // @Test
    public void testReaderMetricsProcessor() throws Exception {
        String rootLocation = "build/private/test_resources/dev/shm";
        deleteAll();
        ReaderMetricsProcessor mp = new ReaderMetricsProcessor(rootLocation);
        mp.processMetrics(rootLocation, 1535065139000L);
        mp.processMetrics(rootLocation, 1535065169000L);
        mp.processMetrics(rootLocation, 1535065199000L);
        mp.processMetrics(rootLocation, 1535065229000L);
        mp.processMetrics(rootLocation, 1535065259000L);
        mp.processMetrics(rootLocation, 1535065289000L);
        mp.processMetrics(rootLocation, 1535065319000L);
        mp.processMetrics(rootLocation, 1535065349000L);
        Result<Record> res = mp.getMetricsDB().getValue().queryMetric(Arrays.asList(OSMetrics.CPU_UTILIZATION.toString()),
                Arrays.asList("sum"),
                Arrays.asList(CommonDimension.SHARD_ID.toString(),
                        CommonDimension.INDEX_NAME.toString(),
                        CommonDimension.OPERATION.toString()));
        Double shardFetchCpu = 0d;
        for (Record record: res) {
            if (record.get(CommonDimension.OPERATION.toString()).equals("shardfetch")) {
                shardFetchCpu = Double.parseDouble(record.get(OSMetrics.CPU_UTILIZATION.toString()).toString());
                break;
            }
        }
        assertEquals(0.0016D, shardFetchCpu.doubleValue(), 0.0001);

        mp.trimOldSnapshots();
        mp.deleteDBs();
    }

    public void deleteAll() {
        final File folder = new File("/tmp");
        final File[] files = folder.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(final File dir, final String name) {
                return name.matches("metricsdb_.*");
            }
        });
        for (final File file : files) {
            if (!file.delete()) {
                System.err.println("Can't remove " + file.getAbsolutePath());
            }
        }
    }

    private NavigableMap<Long, MemoryDBSnapshot> setUpAligningWindow(
            long lastUpdateTime3)
            throws Exception {
        // time line
        // writer writes to the left window at 2000l
        // reader reads at 6001l
        // writer writes to the right window at 7000l
        // reader reads at 11001l
        // writer writes to the right window at 12000l
        // reader reads at 16001l
        MemoryDBSnapshot masterPendingSnap1 = new MemoryDBSnapshot(conn,
                MetricName.MASTER_PENDING, 6001L);
        long lastUpdateTime1 = 2000L;
        masterPendingSnap1.setLastUpdatedTime(lastUpdateTime1);
        Object[][] values1 = { { 0 } };
        masterPendingSnap1.insertMultiRows(values1);

        MemoryDBSnapshot masterPendingSnap2 = new MemoryDBSnapshot(conn,
                MetricName.MASTER_PENDING, 11001L);
        long lastUpdateTime2 = 7000L;
        masterPendingSnap2.setLastUpdatedTime(lastUpdateTime2);
        Object[][] values2 = { { 1 } };
        masterPendingSnap2.insertMultiRows(values2);

        MemoryDBSnapshot masterPendingSnap3 = new MemoryDBSnapshot(conn,
                MetricName.MASTER_PENDING, 16001L );
        masterPendingSnap2.setLastUpdatedTime(lastUpdateTime3);
        Object[][] values3 = { { 3 } };
        masterPendingSnap3.insertMultiRows(values3);

        NavigableMap<Long, MemoryDBSnapshot> metricMap = new TreeMap<>();
        metricMap.put(lastUpdateTime1, masterPendingSnap1);
        metricMap.put(lastUpdateTime2, masterPendingSnap2);
        metricMap.put(lastUpdateTime3, masterPendingSnap3);

        return metricMap;
    }

    private NavigableMap<Long, MemoryDBSnapshot> setUpAligningWindow()
            throws Exception {
        return setUpAligningWindow(12000L);
    }

    /**
     *  Time line
     *    + writer writes 0 to the left window at 2000l
     *    + reader reads at 6001l
     *    + writer writes 1 to the right window at 7000l
     *    + reader reads at 11001l
     *    + writer writes 3 to the right window at 12000l
     *    + reader reads at 16001l
     *
     * Given metrics in two writer windows calculates a new reader window which
     *  overlaps with the given windows.
     * |------leftWindow-------|-------rightWindow--------|
     *                        7000
     *            5000                       100000
     *            |-----------alignedWindow———|
     *
     * We retrieve left and right window using a metric map, whose key is the
     *  largest last modification time.
     * leftWindow = metricsMap.get(7000) = 1
     * rightWindow = metricsMap.get(12000) = 3
     *
     * We use values in the future to represent values in the past.  So if at
     *  t1, writer writes values 1, the interval [t1-sample interval, t1] has
     *   value 1.
     * So [2000, 7000] maps to 1, and [7000, 12000] maps to 3.  We end up
     *  having
     * (1 * 2000 + 3 * 3000) / 5000 = 2.2
     *
     * @throws Exception
     */
    @Test
    public void testAlignNodeMetrics() throws Exception {
        ReaderMetricsProcessor mp = new ReaderMetricsProcessor(rootLocation);
        long readerTime1 = 6001L;
        long readerTime2 = 11001L;
        NavigableMap<Long, MemoryDBSnapshot> metricMap = setUpAligningWindow(
                );
        // The 3rd parameter is windowEndTime.
        // So we compute algined metrics based previous reader window [6000L,
        // 11000l]. But we use PerformanceAnalyzerMetrics.getTimeInterval to
        // compute the aligned reader window time: 10000.
        // So our aligned window time is [5000,10000].
        MemoryDBSnapshot masterPendingFinal = new MemoryDBSnapshot(conn,
                MetricName.MASTER_PENDING,
                PerformanceAnalyzerMetrics.getTimeInterval(readerTime2,
                        MetricsConfiguration.SAMPLING_INTERVAL),
                true);

        MemoryDBSnapshot alignedWindow = mp.alignNodeMetrics(
                MetricName.MASTER_PENDING, metricMap,
                PerformanceAnalyzerMetrics.getTimeInterval(readerTime1,
                        MetricsConfiguration.SAMPLING_INTERVAL),
                PerformanceAnalyzerMetrics.getTimeInterval(readerTime2,
                        MetricsConfiguration.SAMPLING_INTERVAL),
                masterPendingFinal);

        Result<Record> res = alignedWindow.fetchAll();
        assertTrue(1 == res.size());
        Field<Double> valueField = DSL.field(
                MasterPendingValue.MASTER_PENDING_QUEUE_SIZE.toString(), Double.class);
        Double pending = Double
                .parseDouble(res.get(0).get(valueField).toString());
        assertEquals(2.2d, pending, 0.001);
    }

    @Test
    public void testEmitNodeMetrics() throws Exception {
        // the Connection that the test uses and ReaderMetricsProcessor uses are
        // different.
        // Need to use the same one otherwise table created in the test won't be
        // visible in ReaderMetricsProcessor.
        ReaderMetricsProcessor mp = new ReaderMetricsProcessor(rootLocation);
        ReaderMetricsProcessor spyMp = Mockito.spy(mp);
        Mockito.doReturn(this.conn).when(spyMp).getConnection();

        long readerTime2 = 11001L;
        NavigableMap<Long, MemoryDBSnapshot> metricMap = setUpAligningWindow(
                );

        spyMp.putNodeMetricsMap(MetricName.MASTER_PENDING, metricMap);

        MetricsDB db = new MetricsDB(System.currentTimeMillis());
        spyMp.emitNodeMetrics(
                PerformanceAnalyzerMetrics.getTimeInterval(readerTime2,
                MetricsConfiguration.SAMPLING_INTERVAL), db);

        Result<Record> res = db
                .queryMetric(MasterPendingValue.MASTER_PENDING_QUEUE_SIZE.toString());

        assertTrue(1 == res.size());

        Record row0 = res.get(0);
        for (int i=0; i<row0.size(); i++) {
            Double pending = Double
                    .parseDouble(row0.get(i).toString());
            assertEquals(2.2d, pending, 0.001);

        }
    }

    /**
     * Reader window is: 10000~15000
     * Writer hasn't write to 17000 yet.  Writer only has written at:
     * 2001, 7001, 12001
     * Since the reader needs two windows to align:
     * [7001 ~ 12001] and [12001 ~ 17001]
     * and the window [12001 ~ 17001] does not exist, reader would skip
     * aligning and use the value of [7001 ~ 12001] instead.
     * @throws Exception
     */
    @Test
    public void testMissingUpperWriterWindow() throws Exception {
        // the Connection that the test uses and ReaderMetricsProcessor uses are
        // different.
        // Need to use the same one otherwise table created in the test won't be
        // visible in ReaderMetricsProcessor.
        ReaderMetricsProcessor mp = new ReaderMetricsProcessor(rootLocation);
        ReaderMetricsProcessor spyMp = Mockito.spy(mp);
        Mockito.doReturn(this.conn).when(spyMp).getConnection();

        long readerTime2 = 16001L;
        NavigableMap<Long, MemoryDBSnapshot> metricMap = setUpAligningWindow(
                );

        spyMp.putNodeMetricsMap(MetricName.MASTER_PENDING, metricMap);

        MetricsDB db = new MetricsDB(System.currentTimeMillis());
        spyMp.emitNodeMetrics(
                PerformanceAnalyzerMetrics.getTimeInterval(readerTime2,
                MetricsConfiguration.SAMPLING_INTERVAL), db);

        Result<Record> res = db
                .queryMetric(MasterPendingValue.MASTER_PENDING_QUEUE_SIZE.toString());

        assertTrue(1 == res.size());

        Record row0 = res.get(0);
        for (int i=0; i<row0.size(); i++) {
            Double pending = Double
                    .parseDouble(row0.get(i).toString());
            assertEquals(3.0, pending, 0.001);

        }

        // db tables should not be deleted
        for (MemoryDBSnapshot value : metricMap.values()) {
            assertTrue(value.dbTableExists());
        }
    }

    /**
     * Make sure we return null in alignNodeMetrics when right window {}
     *  snapshot ends at or before endTime.  This is possible because writer
     *   writes in less than 5 seconds (writer does not guarantee write every
     *    5 seconds). Reader does not expect that. Changed to return null in
     *     this case.
     * @throws Exception
     */
    @Test
    public void testWriterWindowEndsBeforeReaderWindow() throws Exception {
        // the Connection that the test uses and ReaderMetricsProcessor uses are
        // different.
        // Need to use the same one otherwise table created in the test won't be
        // visible in ReaderMetricsProcessor.
        ReaderMetricsProcessor mp = new ReaderMetricsProcessor(rootLocation);
        ReaderMetricsProcessor spyMp = Mockito.spy(mp);
        Mockito.doReturn(this.conn).when(spyMp).getConnection();

        long readerTime2 = 11001L;
        NavigableMap<Long, MemoryDBSnapshot> metricMap = setUpAligningWindow(
                9999L);

        spyMp.putNodeMetricsMap(MetricName.MASTER_PENDING, metricMap);

        MetricsDB db = new MetricsDB(System.currentTimeMillis());
        spyMp.emitNodeMetrics(PerformanceAnalyzerMetrics.getTimeInterval(readerTime2,
                MetricsConfiguration.SAMPLING_INTERVAL), db);

        assertTrue(!db.metricExists(
                MasterPendingValue.MASTER_PENDING_QUEUE_SIZE.toString()));
    }
}
