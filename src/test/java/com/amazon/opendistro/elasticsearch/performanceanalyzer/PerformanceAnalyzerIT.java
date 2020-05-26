package com.amazon.opendistro.elasticsearch.performanceanalyzer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PerformanceAnalyzerIT extends ESRestTestCase {
    private static final Logger LOG = LogManager.getLogger(PerformanceAnalyzerIT.class);
    private static final int PORT = 9600;
    private static final ObjectMapper mapper = new ObjectMapper();
    private static RestClient paClient;

    @Before
    public void initPaClient() throws IOException {
        String cluster = System.getProperty("tests.rest.cluster");
        logger.info("Cluster is {}", cluster);
        if (cluster == null) {
            throw new RuntimeException("Must specify [tests.rest.cluster] system property with a comma delimited list of [host:port] "
                    + "to which to send REST requests");
        }
        List<HttpHost> hosts = Collections.singletonList(
                buildHttpHost(cluster.substring(0, cluster.lastIndexOf(":")), PORT));
        logger.info("initializing PerformanceAnalyzer client against {}", hosts);
        paClient = buildClient(restClientSettings(), hosts.toArray(new HttpHost[0]));
    }


    public static void ensurePaAndRcaEnabled() throws Exception {
        // TODO replace with waitFor with a 1min timeout
        for (int i = 0; i < 60; i++) {
            Response resp = client().performRequest(new Request("GET", "_opendistro/_performanceanalyzer/cluster/config"));
            Map<String, Object> respMap = mapper.readValue(EntityUtils.toString(resp.getEntity(), "UTF-8"),
                    new TypeReference<Map<String, Object>>(){});
            if (respMap.get("currentPerformanceAnalyzerClusterState").equals(3)) {
                break;
            }
            Thread.sleep(1000L);
        }
        Response resp = client().performRequest(new Request("GET", "_opendistro/_performanceanalyzer/cluster/config"));
        Map<String, Object> respMap = mapper.readValue(EntityUtils.toString(resp.getEntity(), "UTF-8"),
                new TypeReference<Map<String, Object>>(){});
        if (!respMap.get("currentPerformanceAnalyzerClusterState").equals(3)) {
            throw new Exception("PA and RCA are not enabled on the target cluster!");
        }
    }

    private static class TestUtils {
        public static final String DATA = "data";
        public static final String RECORDS = "records";

        // Field related strings
        public static final String FIELDS = "fields";
        public static final String FIELD_NAME = "name";
        public static final String FIELD_TYPE = "type";
        public static final String DOUBLE_TYPE = "DOUBLE";

        // Metrics related strings
        public static final String M_DISKUTIL = "Disk_Utilization";
    }

    @Test
    public void checkMetrics() throws Exception {
        ensurePaAndRcaEnabled();
        Request request = new Request("GET",
                "/_opendistro/_performanceanalyzer/metrics/?metrics=Disk_Utilization&agg=max&dim=&nodes=all");
        Response resp = paClient.performRequest(request);
        Assert.assertEquals(HttpStatus.SC_OK, resp.getStatusLine().getStatusCode());
        ObjectMapper mapper = new ObjectMapper();
        String jsonString = EntityUtils.toString(resp.getEntity());
        JsonNode root = mapper.readTree(jsonString);
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

    @AfterClass
    public static void closePaClient() throws Exception {
        ESRestTestCase.closeClients();
        paClient.close();
        LOG.debug("AfterClass has run");
    }
}
