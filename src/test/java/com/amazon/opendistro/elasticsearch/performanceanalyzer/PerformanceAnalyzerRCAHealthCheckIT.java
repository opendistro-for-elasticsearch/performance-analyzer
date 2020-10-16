package com.amazon.opendistro.elasticsearch.performanceanalyzer;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.WaitFor;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class PerformanceAnalyzerRCAHealthCheckIT extends PerformanceAnalyzerIntegTestBase {
    @Test
    public void checkMetrics() throws Exception {
        ensurePaAndRcaEnabled();
        final String[] jsonString = new String[1];
        WaitFor.waitFor(() -> {
            Request request = new Request("GET",
                    "/_opendistro/_performanceanalyzer/metrics/?metrics=Disk_Utilization&agg=max&dim=&nodes=all");
            Response resp = paClient.performRequest(request);
            Assert.assertEquals(HttpStatus.SC_OK, resp.getStatusLine().getStatusCode());
            jsonString[0] = EntityUtils.toString(resp.getEntity());
            JsonNode root = mapper.readTree(jsonString[0]);
            for (Iterator<JsonNode> it = root.elements(); it.hasNext(); ) {
                JsonNode entry = it.next();
                JsonNode data = entry.get(TestUtils.DATA);
                if (data.get(TestUtils.FIELDS) == null) {
                    return false;
                }
            }
            return jsonString[0] != null && !jsonString[0].isEmpty();
        }, 1, TimeUnit.MINUTES);
        logger.info("jsonString is {}", jsonString[0]);
        JsonNode root = mapper.readTree(jsonString[0]);
        root.forEach( entry -> {
            JsonNode data = entry.get(TestUtils.DATA);
            Assert.assertEquals(1, data.get(TestUtils.FIELDS).size());
            JsonNode field = data.get(TestUtils.FIELDS).get(0);
            Assert.assertEquals(TestUtils.M_DISKUTIL, field.get(TestUtils.FIELD_NAME).asText());
            Assert.assertEquals(TestUtils.DOUBLE_TYPE, field.get(TestUtils.FIELD_TYPE).asText());
            JsonNode records = data.get(TestUtils.RECORDS);
            Assert.assertEquals(1, records.size());
            records.get(0).forEach(record -> Assert.assertTrue(record.asDouble() >= 0));
        });
    }

    @Test
    public void testRcaIsRunning() throws Exception {
        ensurePaAndRcaEnabled();
        WaitFor.waitFor(() -> {
            Request request = new Request("GET", "/_opendistro/_performanceanalyzer/rca");
            try {
                Response resp = paClient.performRequest(request);
                return Objects.equals(HttpStatus.SC_OK, resp.getStatusLine().getStatusCode());
            } catch (Exception e) { // 404, RCA context hasn't been set up yet
                return false;
            }
        }, 2, TimeUnit.MINUTES);
    }
}
