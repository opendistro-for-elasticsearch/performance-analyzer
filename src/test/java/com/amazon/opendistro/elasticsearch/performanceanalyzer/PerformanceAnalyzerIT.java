package com.amazon.opendistro.elasticsearch.performanceanalyzer;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.WaitFor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Objects;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PerformanceAnalyzerIT extends ESRestTestCase {
    private static final Logger LOG = LogManager.getLogger(PerformanceAnalyzerIT.class);
    private static final int PORT = Integer.parseInt(System.getProperty("tests.pa.port"));
    private static final ObjectMapper mapper = new ObjectMapper();
    private static RestClient paClient;

    // Don't wipe the cluster after test completion
    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    // This method is the same as ESRestTestCase#buildClient, but it attempts to connect
    // to the provided cluster for 1 minute before giving up. This is useful when we spin up
    // our own Docker cluster on the local node for Integration Testing
    @Override
    protected RestClient buildClient(Settings settings, HttpHost[] hosts) throws IOException {
        final RestClient[] restClientArr = new RestClient[1];
        try {
            WaitFor.waitFor(() -> {
                try {
                    restClientArr[0] = super.buildClient(settings, hosts);
                } catch (Exception e) {
                    logger.debug("Error building RestClient against hosts {}: {}", hosts, e);
                    return false;
                }
                return true;
            }, 1, TimeUnit.MINUTES);
        } catch (Exception e) {
            throw new IOException(e);
        }
        return restClientArr[0];
    }

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
            if (respMap.get("currentPerformanceAnalyzerClusterState").equals(3) &&
                    !respMap.get("currentPerformanceAnalyzerClusterState").equals(7)) {
                break;
            }
            Thread.sleep(1000L);
        }
        Response resp = client().performRequest(new Request("GET", "_opendistro/_performanceanalyzer/cluster/config"));
        Map<String, Object> respMap = mapper.readValue(EntityUtils.toString(resp.getEntity(), "UTF-8"),
                new TypeReference<Map<String, Object>>(){});
        if (!respMap.get("currentPerformanceAnalyzerClusterState").equals(3) &&
                !respMap.get("currentPerformanceAnalyzerClusterState").equals(7)) {
            throw new Exception("PA and RCA are not enabled on the target cluster!");
        }
    }

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
            Response resp = paClient.performRequest(request);
            return Objects.equals(HttpStatus.SC_OK, resp.getStatusLine().getStatusCode());
        }, 2, TimeUnit.MINUTES);
    }

    @After
    public void closePaClient() throws Exception {
        ESRestTestCase.closeClients();
        paClient.close();
        LOG.debug("AfterClass has run");
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
}
