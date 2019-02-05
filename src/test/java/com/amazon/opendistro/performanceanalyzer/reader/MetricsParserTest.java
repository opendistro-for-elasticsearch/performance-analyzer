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

import org.junit.Test;

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
        parser.parseOSMetrics("test_files/dev/shm", 1537233539000L, osMetricsSnap, 1537232364000L);
        long mFinalT = System.currentTimeMillis();
        System.out.println(mFinalT - mCurrT);
    }
}

