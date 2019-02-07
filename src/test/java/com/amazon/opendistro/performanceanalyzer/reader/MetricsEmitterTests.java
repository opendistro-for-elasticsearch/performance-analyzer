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

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.closeTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.amazon.opendistro.performanceanalyzer.config.TroubleshootingConfig;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.CommonMetric;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.HttpMetric;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.MetricName;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.OSMetrics;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.TCPDimension;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.TCPValue;
import com.amazon.opendistro.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.performanceanalyzer.metricsdb.Dimensions;
import com.amazon.opendistro.performanceanalyzer.metricsdb.MetricsDB;

@PowerMockIgnore({ "org.apache.logging.log4j.*" })
@RunWith(PowerMockRunner.class)
@PrepareForTest({ TroubleshootingConfig.class })
public class MetricsEmitterTests extends AbstractReaderTests {
    public MetricsEmitterTests() throws SQLException, ClassNotFoundException {
        super();
        // TODO Auto-generated constructor stub
    }

    private static final String DB_URL = "jdbc:sqlite:";

    @Test
    public void testMetricsEmitter() throws Exception {
        //
        Connection conn = DriverManager.getConnection(DB_URL);
        ShardRequestMetricsSnapshot rqMetricsSnap = new ShardRequestMetricsSnapshot(conn, 1535065195000L);
        Map<String, String> dimensions = new HashMap<>();
        dimensions.put(ShardRequestMetricsSnapshot.Fields.INDEX_NAME.toString(), "ac-test");
        dimensions.put(ShardRequestMetricsSnapshot.Fields.SHARD_ID.toString(), "1");
        dimensions.put(ShardRequestMetricsSnapshot.Fields.OPERATION.toString(), "shardBulk");
        dimensions.put(ShardRequestMetricsSnapshot.Fields.SHARD_ROLE.toString(), "primary");
        dimensions.put("tid", "1");
        dimensions.put("rid", "1");
        rqMetricsSnap.putStartMetric(1535065196120L, dimensions);
        rqMetricsSnap.putEndMetric(1535065196323L, dimensions);
        dimensions.put("rid", "2");
        dimensions.put(ShardRequestMetricsSnapshot.Fields.OPERATION.toString(), "shardSearch");
        rqMetricsSnap.putStartMetric(1535065197323L, dimensions);
        dimensions.put("rid", "3");
        dimensions.put("tid", "2");
        rqMetricsSnap.putStartMetric(1535065198323L, dimensions);
        rqMetricsSnap.putEndMetric(1535065199923L, dimensions);
        Result<Record> res = rqMetricsSnap.fetchThreadUtilizationRatio();
        Float tUtil = Float.parseFloat(res.get(0).get("tUtil").toString());
        assertEquals(0.07048611f, tUtil.floatValue(), 0);

        OSMetricsSnapshot osMetricsSnap = new OSMetricsSnapshot(conn, 1L);
        //Create OSMetricsSnapshot
        Map<String, Double> metrics = new HashMap<>();
        Map<String, String> osDim = new HashMap<>();
        osDim.put("tid", "1");
        osDim.put("tName", "elasticsearch[E-C7clp][search][T#1]");
        metrics.put(OSMetrics.CPU_UTILIZATION.toString(), 2.3333d);
        metrics.put(OSMetrics.PAGING_RSS.toString(), 3.63d);
        osMetricsSnap.putMetric(metrics, osDim);
        osDim.put("tid", "2");
        osDim.put("tName", "elasticsearch[E-C7clp][bulk][T#2]");
        metrics.put(OSMetrics.CPU_UTILIZATION.toString(), 3.3333d);
        metrics.put(OSMetrics.PAGING_RSS.toString(), 1.63d);
        osMetricsSnap.putMetric(metrics, osDim);
        osDim.put("tid", "3");
        osDim.put("tName", "GC");
        metrics.put(OSMetrics.CPU_UTILIZATION.toString(), 3.3333d);
        metrics.put(OSMetrics.PAGING_RSS.toString(), 1.63d);
        osMetricsSnap.putMetric(metrics, osDim);

        DSLContext create = DSL.using(conn, SQLDialect.SQLITE);
        MetricsDB db = new MetricsDB(System.currentTimeMillis());
        MetricsEmitter.emitAggregatedOSMetrics(create, db, osMetricsSnap, rqMetricsSnap);
        res = db.queryMetric(Arrays.asList(OSMetrics.PAGING_RSS.toString(), OSMetrics.CPU_UTILIZATION.toString()),
                Arrays.asList("sum", "sum"),
                Arrays.asList(ShardRequestMetricsSnapshot.Fields.SHARD_ID.toString(),
                        ShardRequestMetricsSnapshot.Fields.INDEX_NAME.toString(),
                        ShardRequestMetricsSnapshot.Fields.OPERATION.toString()));

        Double cpu = Double.parseDouble(res.get(0).get(OSMetrics.CPU_UTILIZATION.toString()).toString());
        assertEquals(0.164465243055556d, cpu.doubleValue(), 0);
    }


    @Test(expected = Exception.class)
    public void testMetricsEmitterInvalidData() throws Exception {
        //
        PowerMockito.mockStatic(TroubleshootingConfig.class);
        PowerMockito.when(TroubleshootingConfig.getEnableDevAssert()).thenReturn(true);

        Connection conn = DriverManager.getConnection(DB_URL);
        ShardRequestMetricsSnapshot rqMetricsSnap = new ShardRequestMetricsSnapshot(conn, 1535065195000L);
        Map<String, String> dimensions = new HashMap<>();
        dimensions.put(ShardRequestMetricsSnapshot.Fields.INDEX_NAME.toString(), "ac-test");
        dimensions.put(ShardRequestMetricsSnapshot.Fields.SHARD_ID.toString(), "1");
        dimensions.put(ShardRequestMetricsSnapshot.Fields.OPERATION.toString(), "shardBulk");
        dimensions.put(ShardRequestMetricsSnapshot.Fields.SHARD_ROLE.toString(), "primary");
        dimensions.put("tid", "1");
        dimensions.put("rid", "1");
        rqMetricsSnap.putStartMetric(1535065196120L, dimensions);
        rqMetricsSnap.putEndMetric(1535065196323L, dimensions);
        dimensions.put("rid", "2");
        dimensions.put(ShardRequestMetricsSnapshot.Fields.OPERATION.toString(), "shardSearch");
        rqMetricsSnap.putStartMetric(1535065197323L, dimensions);
        dimensions.put("rid", "3");
        dimensions.put("tid", "2");
        rqMetricsSnap.putStartMetric(1535065198323L, dimensions);
        rqMetricsSnap.putEndMetric(1535065199923L, dimensions);
        Result<Record> res = rqMetricsSnap.fetchThreadUtilizationRatio();
        Float tUtil = Float.parseFloat(res.get(0).get("tUtil").toString());
        assertEquals(0.07048611f, tUtil.floatValue(), 0);

        OSMetricsSnapshot osMetricsSnap = new OSMetricsSnapshot(conn, 1L);
        //Create OSMetricsSnapshot
        Map<String, Double> metrics = new HashMap<>();
        Map<String, String> osDim = new HashMap<>();
        osDim.put("tid", "1");
        osDim.put("tName", "elasticsearch[E-C7clp][search][T#1]");
        metrics.put(OSMetrics.CPU_UTILIZATION.toString(), 2.3333d);
        metrics.put(OSMetrics.PAGING_RSS.toString(), 3.63d);
        osMetricsSnap.putMetric(metrics, osDim);
        osDim.put("tid", "2");
        osDim.put("tName", "GC thread");
        metrics.put(OSMetrics.CPU_UTILIZATION.toString(), 3.3333d);
        metrics.put(OSMetrics.PAGING_RSS.toString(), 1.63d);
        osMetricsSnap.putMetric(metrics, osDim);
        osDim.put("tid", "3");
        osDim.put("tName", "GC");
        metrics.put(OSMetrics.CPU_UTILIZATION.toString(), 3.3333d);
        metrics.put(OSMetrics.PAGING_RSS.toString(), 1.63d);
        osMetricsSnap.putMetric(metrics, osDim);

        DSLContext create = DSL.using(conn, SQLDialect.SQLITE);
        MetricsDB db = new MetricsDB(System.currentTimeMillis());
        MetricsEmitter.emitAggregatedOSMetrics(create, db, osMetricsSnap, rqMetricsSnap);
        res = db.queryMetric(Arrays.asList(OSMetrics.PAGING_RSS.toString(), OSMetrics.CPU_UTILIZATION.toString()),
                Arrays.asList("sum", "sum"),
                Arrays.asList(ShardRequestMetricsSnapshot.Fields.SHARD_ID.toString(),
                        ShardRequestMetricsSnapshot.Fields.INDEX_NAME.toString(),
                        ShardRequestMetricsSnapshot.Fields.OPERATION.toString()));
    }

    @Test
    public void testHttpMetricsEmitter() throws Exception {
        Connection conn = DriverManager.getConnection(DB_URL);
        HttpRequestMetricsSnapshot rqMetricsSnap = new HttpRequestMetricsSnapshot(conn, 1L);
        Map<String, String> dimensions = new HashMap<>();
        dimensions.put(HttpRequestMetricsSnapshot.Fields.OPERATION.toString(), "search");
        dimensions.put(HttpRequestMetricsSnapshot.Fields.HTTP_RESP_CODE.toString(), "200");
        dimensions.put(HttpRequestMetricsSnapshot.Fields.INDICES.toString(), "");
        dimensions.put(HttpRequestMetricsSnapshot.Fields.EXCEPTION.toString(), "");
        dimensions.put("rid", "1");
        rqMetricsSnap.putStartMetric(12345L, 0L, dimensions);
        rqMetricsSnap.putEndMetric(33325L, dimensions);
        dimensions.put("rid", "2");
        dimensions.put(HttpRequestMetricsSnapshot.Fields.OPERATION.toString(), "search");
        rqMetricsSnap.putStartMetric(22245L,0L,dimensions);
        dimensions.put("rid", "3");
        rqMetricsSnap.putStartMetric(10000L,0L,dimensions);
        rqMetricsSnap.putEndMetric(30000L, dimensions);

        DSLContext create = DSL.using(conn, SQLDialect.SQLITE);
        MetricsDB db = new MetricsDB(System.currentTimeMillis());
        MetricsEmitter.emitHttpMetrics(create, db, rqMetricsSnap);
        Result<Record> res = db.queryMetric(Arrays.asList(CommonMetric.LATENCY.toString(),
                HttpMetric.HTTP_TOTAL_REQUESTS.toString()),
                Arrays.asList("avg", "sum"),
                Arrays.asList(HttpRequestMetricsSnapshot.Fields.OPERATION.toString()));

        Float latency = Float.parseFloat(res.get(0).get(CommonMetric.LATENCY.toString()).toString());
        assertEquals(20490.0f, latency.floatValue(), 0);
    }

    @Test
    public void testExtractor() {
        String check = "abc: 2\nbbc:\ncbc:21\n";
        assertEquals(" 2", PerformanceAnalyzerMetrics.extractMetricValue(check, "abc"));
        assertEquals("", PerformanceAnalyzerMetrics.extractMetricValue(check, "bbc"));
        assertEquals("21", PerformanceAnalyzerMetrics.extractMetricValue(check, "cbc"));
    }

    @Test
    public void testThreadNameCategorization() {
        Dimensions dimensions = new Dimensions();
        assertEquals("GC", MetricsEmitter.categorizeThreadName("Gang worker#0 (Parallel GC Threads)", dimensions));
        assertEquals(null , MetricsEmitter.categorizeThreadName("elasticsearch[I9AByra][search][T#4]", dimensions));
        assertEquals("refresh", MetricsEmitter.categorizeThreadName("elasticsearch[I9AByra][refresh][T#1]", dimensions));
        assertEquals("merge", MetricsEmitter.categorizeThreadName("elasticsearch[I9AByra][[nyc_taxis][1]: Lucene Merge", dimensions));
        assertEquals("management", MetricsEmitter.categorizeThreadName("elasticsearch[I9AByra][management]", dimensions));
        assertEquals(null, MetricsEmitter.categorizeThreadName("elasticsearch[I9AByra][search]", dimensions));
        assertEquals(null, MetricsEmitter.categorizeThreadName("elasticsearch[I9AByra][bulk]", dimensions));
        assertEquals("other", MetricsEmitter.categorizeThreadName("Top thread random", dimensions));
    }

    @Test
    public void testEmitNodeMetrics() throws Exception {
        Connection conn = DriverManager.getConnection(DB_URL);

        MemoryDBSnapshot tcpSnap = new MemoryDBSnapshot(conn,
                MetricName.TCP_METRICS,
                5001L);

        long lastUpdatedTime = 2000L;
        tcpSnap.setLastUpdatedTime(lastUpdatedTime);

        Object[][] values = {
                { "0000000000000000FFFF0000E03DD40A", 24, 0, 0, 0, 7, 1 },
                { "0000000000000000FFFF00006733D40A", 23, 0, 0, 0, 6, 1 },
                { "0000000000000000FFFF00000100007F", 24, 0, 0, 0, 10,-1 },
                { "0000000000000000FFFF00005432D40A", 23, 0, 0, 0, 8,5 },
                { "00000000000000000000000000000000", 4, 0, 0, 0, 10, 0 },
                { "0000000000000000FFFF0000F134D40A", 23, 0, 0, 0, 8, 0}};

        tcpSnap.insertMultiRows(values);

        DSLContext create = DSL.using(conn, SQLDialect.SQLITE);
        MetricsDB db = new MetricsDB(System.currentTimeMillis());
        MetricsEmitter.emitNodeMetrics(create, db, tcpSnap);
        Result<Record> res = db.queryMetric(
                Arrays.asList(TCPValue.Net_TCP_NUM_FLOWS.toString(),
                        TCPValue.Net_TCP_SSTHRESH.toString()),
                Arrays.asList("sum", "avg"),
                Arrays.asList(TCPDimension.DEST_ADDR.toString()));

        assertTrue(6 ==  res.size());

        for (int i = 0; i < 6; i++) {
            Record record0 = res.get(i);
            Double numFlows = Double.parseDouble(
                    record0.get(TCPValue.Net_TCP_NUM_FLOWS.toString()).toString());

            assertThat(numFlows.doubleValue(), anyOf(closeTo(24, 0.001),
                    closeTo(23, 0.001), closeTo(4, 0.001)));

            Double ssThresh = Double.parseDouble(
                    record0.get(TCPValue.Net_TCP_SSTHRESH.toString()).toString());

            assertThat(ssThresh.doubleValue(), anyOf(closeTo(1, 0.001),
                    closeTo(-1, 0.001), closeTo(5, 0.001), closeTo(0, 0.001)));

        }

    }
}


