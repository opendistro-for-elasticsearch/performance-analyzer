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

import com.amazon.opendistro.performanceanalyzer.config.TroubleshootingConfig;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.Test;

import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.MetricName;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.TCPDimension;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.TCPValue;
import com.amazon.opendistro.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.performanceanalyzer.metricsdb.Dimensions;
import com.amazon.opendistro.performanceanalyzer.metricsdb.MetricsDB;

public class MetricsEmitterTest extends AbstractReaderTest {
    public MetricsEmitterTest() throws SQLException, ClassNotFoundException {
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
        dimensions.put("indexName", "ac-test");
        dimensions.put("shard", "1");
        dimensions.put("operation", "shardBulk");
        dimensions.put("role", "primary");
        dimensions.put("tid", "1");
        dimensions.put("rid", "1");
        rqMetricsSnap.putStartMetric(1535065196120L, dimensions);
        rqMetricsSnap.putEndMetric(1535065196323L, dimensions);
        dimensions.put("rid", "2");
        dimensions.put("operation", "shardSearch");
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
        metrics.put("cpu", 2.3333d);
        metrics.put("rss", 3.63d);
        osMetricsSnap.putMetric(metrics, osDim);
        osDim.put("tid", "2");
        osDim.put("tName", "elasticsearch[E-C7clp][bulk][T#2]");
        metrics.put("cpu", 3.3333d);
        metrics.put("rss", 1.63d);
        osMetricsSnap.putMetric(metrics, osDim);
        osDim.put("tid", "3");
        osDim.put("tName", "GC");
        metrics.put("cpu", 3.3333d);
        metrics.put("rss", 1.63d);
        osMetricsSnap.putMetric(metrics, osDim);

        DSLContext create = DSL.using(conn, SQLDialect.SQLITE);
        MetricsDB db = new MetricsDB(System.currentTimeMillis());
        MetricsEmitter.emitAggregatedOSMetrics(create, db, osMetricsSnap, rqMetricsSnap);
        res = db.queryMetric(Arrays.asList("rss", "cpu"),
                Arrays.asList("sum", "sum"),
                Arrays.asList("shard", "index", "operation"));

        Double cpu = Double.parseDouble(res.get(0).get("cpu").toString());
        assertEquals(0.164465243055556d, cpu.doubleValue(), 0);
    }


    @Test(expected = Exception.class)
    public void testMetricsEmitterInvalidData() throws Exception {
        TroubleshootingConfig.EnableDevAssert = true;
        //
        Connection conn = DriverManager.getConnection(DB_URL);
        ShardRequestMetricsSnapshot rqMetricsSnap = new ShardRequestMetricsSnapshot(conn, 1535065195000L);
        Map<String, String> dimensions = new HashMap<>();
        dimensions.put("indexName", "ac-test");
        dimensions.put("shard", "1");
        dimensions.put("operation", "shardBulk");
        dimensions.put("role", "primary");
        dimensions.put("tid", "1");
        dimensions.put("rid", "1");
        rqMetricsSnap.putStartMetric(1535065196120L, dimensions);
        rqMetricsSnap.putEndMetric(1535065196323L, dimensions);
        dimensions.put("rid", "2");
        dimensions.put("operation", "shardSearch");
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
        metrics.put("cpu", 2.3333d);
        metrics.put("rss", 3.63d);
        osMetricsSnap.putMetric(metrics, osDim);
        osDim.put("tid", "2");
        osDim.put("tName", "GC thread");
        metrics.put("cpu", 3.3333d);
        metrics.put("rss", 1.63d);
        osMetricsSnap.putMetric(metrics, osDim);
        osDim.put("tid", "3");
        osDim.put("tName", "GC");
        metrics.put("cpu", 3.3333d);
        metrics.put("rss", 1.63d);
        osMetricsSnap.putMetric(metrics, osDim);

        DSLContext create = DSL.using(conn, SQLDialect.SQLITE);
        MetricsDB db = new MetricsDB(System.currentTimeMillis());
        MetricsEmitter.emitAggregatedOSMetrics(create, db, osMetricsSnap, rqMetricsSnap);
        res = db.queryMetric(Arrays.asList("rss", "cpu"),
                Arrays.asList("sum", "sum"),
                Arrays.asList("shard", "index", "operation"));
    }

    @Test
    public void testHttpMetricsEmitter() throws Exception {
        TroubleshootingConfig.EnableDevAssert = false;
        Connection conn = DriverManager.getConnection(DB_URL);
        HttpRequestMetricsSnapshot rqMetricsSnap = new HttpRequestMetricsSnapshot(conn, 1L);
        Map<String, String> dimensions = new HashMap<>();
        dimensions.put("operation", "search");
        dimensions.put("status", "200");
        dimensions.put("indices", "");
        dimensions.put("exception", "");
        dimensions.put("rid", "1");
        rqMetricsSnap.putStartMetric(12345L, 0L, dimensions);
        rqMetricsSnap.putEndMetric(33325L, dimensions);
        dimensions.put("rid", "2");
        dimensions.put("operation", "search");
        rqMetricsSnap.putStartMetric(22245L,0L,dimensions);
        dimensions.put("rid", "3");
        rqMetricsSnap.putStartMetric(10000L,0L,dimensions);
        rqMetricsSnap.putEndMetric(30000L, dimensions);

        DSLContext create = DSL.using(conn, SQLDialect.SQLITE);
        MetricsDB db = new MetricsDB(System.currentTimeMillis());
        MetricsEmitter.emitHttpMetrics(create, db, rqMetricsSnap);
        Result<Record> res = db.queryMetric(Arrays.asList("latency", "count"),
                Arrays.asList("avg", "sum"),
                Arrays.asList("operation"));

        Float latency = Float.parseFloat(res.get(0).get("latency").toString());
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
                MetricName.tcp_metrics,
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
                Arrays.asList(TCPValue.numFlows.toString(),
                        TCPValue.SSThresh.toString()),
                Arrays.asList("sum", "avg"),
                Arrays.asList(TCPDimension.dest.toString()));

        assertTrue(6 ==  res.size());

        for (int i = 0; i < 6; i++) {
            Record record0 = res.get(i);
            Double numFlows = Double.parseDouble(
                    record0.get(TCPValue.numFlows.toString()).toString());

            assertThat(numFlows.doubleValue(), anyOf(closeTo(24, 0.001),
                    closeTo(23, 0.001), closeTo(4, 0.001)));

            Double ssThresh = Double.parseDouble(
                    record0.get(TCPValue.SSThresh.toString()).toString());

            assertThat(ssThresh.doubleValue(), anyOf(closeTo(1, 0.001),
                    closeTo(-1, 0.001), closeTo(5, 0.001), closeTo(0, 0.001)));

        }

    }
}


