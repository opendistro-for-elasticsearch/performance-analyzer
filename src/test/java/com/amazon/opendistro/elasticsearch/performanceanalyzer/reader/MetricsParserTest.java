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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.CopyTestResource;
import org.junit.Test;

import java.io.File;

public class MetricsParserTest {
    private static final String DB_URL = "jdbc:sqlite:";

    @Test
    public void testMetricsParser() throws Exception {
        ReaderMetricsProcessor mp = new ReaderMetricsProcessor("build/private/test_resources/dev/shm");
        MetricsParser parser = new MetricsParser();
        OSMetricsSnapshot osMetricsSnap = new OSMetricsSnapshot(mp.getConnection(), 1L);
        ShardRequestMetricsSnapshot rqSnap = new ShardRequestMetricsSnapshot(mp.getConnection(), 1L);
        HttpRequestMetricsSnapshot hRqSnap = new HttpRequestMetricsSnapshot(mp.getConnection(),1535065195000L);
        parser.parseHttpMetrics("build/private/test_resources/dev/shm", 1535065195000L, 1535065200000L, hRqSnap);
        parser.parseRequestMetrics("build/private/test_resources/dev/shm", 1535065195000L, 1535065200000L, rqSnap);
        assertEquals(132, hRqSnap.fetchAll().size(), 0);
        assertEquals(266, rqSnap.fetchAll().size(), 0);
    }

    //@Test
    public void perfTest() throws Exception {
        ReaderMetricsProcessor mp = new ReaderMetricsProcessor("test_files/dev/shm");
        MetricsParser parser = new MetricsParser();
        OSMetricsSnapshot osMetricsSnap = new OSMetricsSnapshot(mp.getConnection(), 1L);
        mp.getConnection().setAutoCommit(false);
        long mCurrT = System.currentTimeMillis();
        parser.parseOSMetrics("test_files/dev/shm", 1537233539000L, 1537232364000L, osMetricsSnap);
        long mFinalT = System.currentTimeMillis();
        System.out.println(mFinalT - mCurrT);
    }

    @Test
    public void testOSMetricRotateParse() throws Exception {

        try (CopyTestResource testResource = new CopyTestResource("build/private/test_resources/dev/shm",
                "build/private/test_resources/dev/shm_metricsparser_testOSMetricRotateParse")) {

            ReaderMetricsProcessor mp = new ReaderMetricsProcessor(testResource.getPath());
            MetricsParser parser = new MetricsParser();

            OSMetricsSnapshot osMetricsSnap = new OSMetricsSnapshot(mp.getConnection(), 1L);
            parser.parseOSMetrics(testResource.getPath(), 1535065195000L, 1535065200000L, osMetricsSnap);
            assertEquals(136, osMetricsSnap.fetchAll().size(), 0);

            File file1 = new File(testResource.getPath() + "/1535065170000/threads/7611/os_metrics");
            File file2 = new File(testResource.getPath() + "/1535065170000/threads/6183/os_metrics");
            long orgModifiedTime1 = file1.lastModified();
            long orgModifiedTime2 = file2.lastModified();
            // set modified to higher than end time
            file1.setLastModified(1535065200000L + 2000L);
            // set modified to lower than start time
            file2.setLastModified(1535065195000L - 2000L);
            try {
                osMetricsSnap = new OSMetricsSnapshot(mp.getConnection(), 2L);
                parser.parseOSMetrics(testResource.getPath(), 1535065195000L, 1535065200000L, osMetricsSnap);
                assertEquals(135, osMetricsSnap.fetchAll().size(), 0);
            } catch (Exception e) {
                assertTrue("unexpected exception" + e.getMessage(), false);
            } finally {
                file1.setLastModified(orgModifiedTime1);
                file2.setLastModified(orgModifiedTime2);
            }
        }
    }
}



