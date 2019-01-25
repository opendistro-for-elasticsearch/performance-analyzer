package com.amazon.opendistro.performanceanalyzer.reader;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;

import org.jooq.Record;
import org.jooq.Result;

import org.junit.Test;

public class ShardRequestMetricsSnapshotTest {
    private static final String DB_URL = "jdbc:sqlite:";

    @Test
    public void testCreateRequestMetrics() throws Exception {
        Connection conn = DriverManager.getConnection(DB_URL);
        ShardRequestMetricsSnapshot rqMetricsSnap = new ShardRequestMetricsSnapshot(conn, 1535065195000L);
        Map<String, String> dimensions = new HashMap<>();
        dimensions.put("indexName", "ac-test");
        dimensions.put("shard", "1");
        dimensions.put("tid", "1");
        dimensions.put("rid", "1");
        rqMetricsSnap.putStartMetric(1535065196120L, dimensions);
        rqMetricsSnap.putEndMetric(1535065196323L, dimensions);
        dimensions.put("rid", "2");
        rqMetricsSnap.putStartMetric(1535065197323L, dimensions);
        dimensions.put("rid", "3");
        dimensions.put("tid", "2");
        rqMetricsSnap.putStartMetric(1535065198323L, dimensions);
        rqMetricsSnap.putEndMetric(1535065199923L, dimensions);
        Result<Record> res = rqMetricsSnap.fetchThreadUtilizationRatio();
        Float tUtil = Float.parseFloat(res.get(0).get("tUtil").toString());
        assertEquals(0.07048611111111111f, tUtil.floatValue(), 0);
    }

    @Test
    public void testRollover() throws Exception {
        Connection conn = DriverManager.getConnection(DB_URL);
        ShardRequestMetricsSnapshot rqMetricsSnap = new ShardRequestMetricsSnapshot(conn, 1535065195000L);
        Map<String, String> dimensions = new HashMap<>();
        dimensions.put("indexName", "ac-test");
        dimensions.put("shard", "1");
        dimensions.put("tid", "1");
        dimensions.put("rid", "1");
        rqMetricsSnap.putStartMetric(1535065196120L, dimensions);
        rqMetricsSnap.putEndMetric(1535065196323L, dimensions);
        dimensions.put("rid", "2");
        rqMetricsSnap.putStartMetric(1535065197323L, dimensions);
        dimensions.put("rid", "3");
        dimensions.put("tid", "2");
        rqMetricsSnap.putStartMetric(1535065198323L, dimensions);
        rqMetricsSnap.putEndMetric(1535065199923L, dimensions);
        ShardRequestMetricsSnapshot newSnap = new ShardRequestMetricsSnapshot(conn, 1L);
        newSnap.rolloverInflightRequests(rqMetricsSnap);
    }

    @Test
    public void testDedup() throws Exception {
        Connection conn = DriverManager.getConnection(DB_URL);
        ShardRequestMetricsSnapshot rqMetricsSnap = new ShardRequestMetricsSnapshot(conn, 1535065195000L);
        Map<String, String> dimensions = new HashMap<>();
        dimensions.put("indexName", "ac-test");
        dimensions.put("shard", "1");
        dimensions.put("tid", "1");
        dimensions.put("rid", "1");
        rqMetricsSnap.putStartMetric(1535065191120L, dimensions);
        dimensions.put("rid", "2");
        rqMetricsSnap.putStartMetric(1535065197323L, dimensions);
        rqMetricsSnap.putEndMetric(1535065198323L, dimensions);
        dimensions.put("rid", "3");
        dimensions.put("tid", "2");
        rqMetricsSnap.putStartMetric(1535065198323L, dimensions);
        dimensions.put("rid", "4");
        dimensions.put("tid", "3");
        rqMetricsSnap.putStartMetric(1535065191323L, dimensions);
        Result<Record> res = rqMetricsSnap.fetchInflightSelect().fetch();
        assertEquals(2, res.size(), 0);
    }

    @Test
    public void testLatestRequestNotExcluded() throws Exception {
        Connection conn = DriverManager.getConnection(DB_URL);
        ShardRequestMetricsSnapshot rqMetricsSnap = new ShardRequestMetricsSnapshot(conn, 1535065195000L);
        Map<String, String> dimensions = new HashMap<>();
        dimensions.put("indexName", "ac-test");
        dimensions.put("shard", "1");
        dimensions.put("tid", "1");
        dimensions.put("rid", "1");
        rqMetricsSnap.putStartMetric(1535065191120L, dimensions);
        dimensions.put("rid", "2");
        rqMetricsSnap.putStartMetric(1535065192323L, dimensions);
        rqMetricsSnap.putEndMetric(1535065198323L, dimensions);
        dimensions.put("rid", "3");
        rqMetricsSnap.putStartMetric(1535065193323L, dimensions);
        Result<Record> res = rqMetricsSnap.fetchInflightSelect().fetch();
        assertEquals(1, res.size(), 0);
        assertEquals("3", res.get(0).get("rid"));
    }

    @Test
    public void testMultiOp() throws Exception {
        Connection conn = DriverManager.getConnection(DB_URL);
        ShardRequestMetricsSnapshot rqMetricsSnap = new ShardRequestMetricsSnapshot(conn, 1535065195000L);
        Map<String, String> dimensions = new HashMap<>();
        dimensions.put("indexName", "ac-test");
        dimensions.put("shard", "1");
        dimensions.put("tid", "1");
        dimensions.put("rid", "1");
        dimensions.put("operation", "shardquery");
        rqMetricsSnap.putStartMetric(1535065191120L, dimensions);
        dimensions.put("tid", "2");
        dimensions.put("operation", "shardfetch");
        rqMetricsSnap.putStartMetric(1535065197323L, dimensions);
        Result<Record> res = rqMetricsSnap.fetchInflightSelect().fetch();
        assertEquals(2, res.size(), 0);
    }


    @Test
    public void testFetchLatency() throws Exception {
        Connection conn = DriverManager.getConnection(DB_URL);
        ShardRequestMetricsSnapshot rqMetricsSnap = new ShardRequestMetricsSnapshot(conn, 1535065195000L);
        Map<String, String> dimensions = new HashMap<>();
        dimensions.put("indexName", "sonested");
        dimensions.put("shard", "0");
        dimensions.put("tid", "1");
        dimensions.put("rid", "1");
        dimensions.put("operation", "shardquery");
        rqMetricsSnap.putStartMetric(1535065191120L, dimensions);
        rqMetricsSnap.putEndMetric(1535065191130L, dimensions);
        dimensions.put("tid", "2");
        dimensions.put("operation", "shardfetch");
        rqMetricsSnap.putStartMetric(1535065197323L, dimensions);
        dimensions.put("rid", "3");
        rqMetricsSnap.putStartMetric(1535065197373L, dimensions);
        dimensions.put("rid", "4");
        rqMetricsSnap.putEndMetric(1535065197388L, dimensions);
        Result<Record> res = rqMetricsSnap.fetchInflightSelect().fetch();
        assertEquals(2, res.size(), 0);
    }
}

