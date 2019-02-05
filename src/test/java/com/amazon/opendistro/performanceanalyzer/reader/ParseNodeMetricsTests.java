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
import java.sql.SQLException;
import java.util.Map;
import java.util.NavigableMap;

import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;
import org.junit.Test;

import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.DiskDimension;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.DiskValue;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.MasterPendingValue;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.MetricName;
import com.amazon.opendistro.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;

public class ParseNodeMetricsTests extends AbstractReaderTests {
    public ParseNodeMetricsTests() throws SQLException, ClassNotFoundException {
        super();
    }

    @Test
    public void testParseMasterPendingMetrics() throws Exception {
        long currTimestamp = System.currentTimeMillis() + 4000;

        long currTimeBucket = PerformanceAnalyzerMetrics.getTimeInterval(currTimestamp);
        String currentTimeBucketStr = String.valueOf(currTimeBucket);
        temporaryFolder.newFolder(currentTimeBucketStr,
                PerformanceAnalyzerMetrics.sPendingTasksPath, PerformanceAnalyzerMetrics.MASTER_CURRENT);
        File output = temporaryFolder.newFile(createRelativePath(
                currentTimeBucketStr, PerformanceAnalyzerMetrics.sPendingTasksPath,
                PerformanceAnalyzerMetrics.MASTER_CURRENT, PerformanceAnalyzerMetrics.MASTER_META_DATA));
        long lastUpdatedTime = System.currentTimeMillis();

        int pendingTasksCount = 0;
        write(output, false, getCurrentMilliSeconds(lastUpdatedTime),
                createPendingTaskMetrics(pendingTasksCount));

        MetricProperties masterPendingProperty = MetricPropertiesConfig
                .getInstance().getProperty(MetricName.MASTER_PENDING);


        masterPendingProperty.getHandler().setRootLocation(rootLocation);

        ReaderMetricsProcessor mp = new ReaderMetricsProcessor(rootLocation);
        mp.parseNodeMetrics(currTimestamp);

        for (Map.Entry<MetricName, NavigableMap<Long, MemoryDBSnapshot>> entry : mp
                .getNodeMetricsMap().entrySet()) {
            NavigableMap<Long, MemoryDBSnapshot> curMap = entry.getValue();
            if (entry.getKey() != MetricName.MASTER_PENDING) {
                assertTrue(curMap.isEmpty());
                continue;
            }

            assertTrue(1 == curMap.size());
            Map.Entry<Long, MemoryDBSnapshot> firstEntry = curMap.lastEntry();
            assertTrue(lastUpdatedTime == firstEntry.getKey());

            MemoryDBSnapshot curSnap = firstEntry.getValue();
            Result<Record> res = curSnap.fetchAll();
            assertTrue(1 == res.size());

            Field<Double> field = DSL.field(
                    MasterPendingValue.MASTER_PENDING_QUEUE_SIZE.toString(),
                    Double.class);

            Record record0 = res.get(0);

            Double pendingCount = Double
                    .parseDouble(record0.get(field).toString());
            assertEquals(0, pendingCount, 0.001);

        }

        mp.trimOldSnapshots();
        mp.deleteDBs();
    }

    @Test
    public void testParseDiskMetrics() throws Exception {
        long currTimestamp = System.currentTimeMillis() + 4000;

        long currTimeBucket = PerformanceAnalyzerMetrics.getTimeInterval(currTimestamp);
        String currentTimeBucketStr = String.valueOf(currTimeBucket);
        temporaryFolder.newFolder(currentTimeBucketStr);
        File output = temporaryFolder.newFile(createRelativePath(
                currentTimeBucketStr, PerformanceAnalyzerMetrics.sDisksPath));
        long lastUpdatedTime = System.currentTimeMillis();

        String diskXvda = "xvda";
        String diskNvme0n1 = "nvme0n1";
        double util1 = 0.0008d;
        double wait1 = 2.0d;
        double srate1 = 14.336d;
        double util2 = 0.0d;
        double wait2 = 0.0d;
        double srate2 = 0.0d;
        write(output, false, getCurrentMilliSeconds(lastUpdatedTime),
                createDiskMetrics(diskXvda, util1, wait1, srate1),
                createDiskMetrics(diskNvme0n1, 0.0d, 0.0d, 0.0));

        MetricProperties diskProperty = MetricPropertiesConfig.getInstance()
                .getProperty(MetricName.DISK_METRICS);

        String rootLocation = temporaryFolder.getRoot().getCanonicalPath()
                + File.separator;
        diskProperty.getHandler().setRootLocation(rootLocation);

        ReaderMetricsProcessor mp = new ReaderMetricsProcessor(rootLocation);
        mp.parseNodeMetrics(currTimestamp);

        verifyDiskMetrics(mp.getNodeMetricsMap(), lastUpdatedTime, diskXvda,
                diskNvme0n1, util1, wait1, srate1, util2, wait2, srate2, 1);

        util1 = 0.0009d;
        wait1 = 2.1d;
        srate1 = 14.436d;
        util2 = 0.1009d;
        wait2 = 0.0d;
        srate2 = 0.0d;

        lastUpdatedTime = System.currentTimeMillis();

        write(output, false, getCurrentMilliSeconds(lastUpdatedTime),
                createDiskMetrics(diskXvda, util1, wait1, srate1),
                createDiskMetrics(diskNvme0n1, util2, wait2, srate2));

        currTimestamp = System.currentTimeMillis() + 4000;
        mp.parseNodeMetrics(currTimestamp);

        verifyDiskMetrics(mp.getNodeMetricsMap(), lastUpdatedTime, diskXvda,
                diskNvme0n1, util1, wait1, srate1, util2, wait2, srate2, 2);

        mp.trimOldSnapshots();
        mp.deleteDBs();
    }

    private void verifyDiskMetrics(
            Map<MetricName, NavigableMap<Long, MemoryDBSnapshot>> nodeMetricsMap,
            long lastUpdatedTime, String diskXvda, String diskNvme0n1,
            double util1, double wait1, double srate1, double util2,
            double wait2, double srate2, int expectedDiskMapSize) {
        for (Map.Entry<MetricName, NavigableMap<Long, MemoryDBSnapshot>> entry : nodeMetricsMap
                .entrySet()) {
            NavigableMap<Long, MemoryDBSnapshot> curMap = entry.getValue();
            if (entry.getKey() != MetricName.DISK_METRICS) {
                assertTrue(curMap.isEmpty());
                continue;
            }

            assertTrue(expectedDiskMapSize == curMap.size());
            Map.Entry<Long, MemoryDBSnapshot> firstEntry = curMap.lastEntry();
            assertTrue(lastUpdatedTime == firstEntry.getKey());

            MemoryDBSnapshot curSnap = firstEntry.getValue();
            assertTrue(2 == curSnap.fetchAll().size());

            @SuppressWarnings("unchecked")
            Field<Double>[] fields = new Field[3];

            fields[0] = DSL.field(DiskValue.DISK_UTILIZATION.toString(), Double.class);
            fields[1] = DSL.field(DiskValue.DISK_WAITTIME.toString(), Double.class);
            fields[2] = DSL.field(DiskValue.DISK_SERVICE_RATE.toString(), Double.class);

            Result<Record> resRecordDiskXvda = curSnap.fetchMetric(
                    getDimensionEqCondition(DiskDimension.DISK_NAME, diskXvda),
                    fields);

            assertTrue(1 == resRecordDiskXvda.size());

            Record record0 = resRecordDiskXvda.get(0);

            Double util = Double.parseDouble(record0.get(fields[0]).toString());
            assertEquals(util1, util, 0.001);

            Double wait = Double.parseDouble(record0.get(fields[1]).toString());
            assertEquals(wait1, wait, 0.001);

            Double srate = Double
                    .parseDouble(record0.get(fields[2]).toString());
            assertEquals(srate1, srate, 0.001);

            Result<Record> resRecordNvme0n1 = curSnap.fetchMetric(
                    getDimensionEqCondition(DiskDimension.DISK_NAME, diskNvme0n1),
                    fields);

            assertTrue(1 == resRecordDiskXvda.size());

            Record record1 = resRecordNvme0n1.get(0);

            util = Double.parseDouble(record1.get(fields[0]).toString());
            assertEquals(util2, util, 0.001);

            wait = Double.parseDouble(record1.get(fields[1]).toString());
            assertEquals(wait2, wait, 0.001);

            srate = Double.parseDouble(record1.get(fields[2]).toString());
            assertEquals(srate2, srate, 0.001);
        }
    }


}
