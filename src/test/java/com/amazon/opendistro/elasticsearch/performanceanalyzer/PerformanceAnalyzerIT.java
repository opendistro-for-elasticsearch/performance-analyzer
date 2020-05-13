package com.amazon.opendistro.elasticsearch.performanceanalyzer;

import com.fasterxml.jackson.core.type.TypeReference;
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
        String[] stringUrls = cluster.split(",");
        List<HttpHost> hosts = Collections.singletonList(buildHttpHost("localhost", PORT));
        logger.info("initializing PerformanceAnalyzer client against {}", hosts);
        paClient = buildClient(restClientSettings(), hosts.toArray(new HttpHost[0]));
    }


    public static void ensurePaAndRcaEnabled() throws Exception {
        /*
        Request request = new Request("POST", "_opendistro/_performanceanalyzer/cluster/config");
        request.setJsonEntity("{\"enabled\": true}");
        Response resp = client().performRequest(request);
        assert resp.getStatusLine().getStatusCode() == 200;
        request = new Request("POST", "_opendistro/_performanceanalyzer/rca/cluster/config");
        request.setJsonEntity("{\"enabled\": true}");
        resp = client().performRequest(request);
        assert resp.getStatusLine().getStatusCode() == 200;
        LOG.info("PA INITIAL STATUS {}", EntityUtils.toString(resp.getEntity(), "UTF-8"));
        */
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

    @Test
    public void checkMetrics() throws Exception {
        ensurePaAndRcaEnabled();
        Request request = new Request("GET",
                "/_opendistro/_performanceanalyzer/metrics/?metrics=Disk_Utilization&agg=max&dim=&nodes=all");
        Response resp = paClient.performRequest(request);
        assert resp.getStatusLine().getStatusCode() == HttpStatus.SC_OK;
        LOG.info("PA is emitting metrics!! {}", EntityUtils.toString(resp.getEntity(), "UTF-8"));
        paClient.close();
    }

    @AfterClass
    public static void closePaClient() throws Exception {
        ESRestTestCase.closeClients();
        paClient.close();
        LOG.info("AfterClass has run");
    }
}
