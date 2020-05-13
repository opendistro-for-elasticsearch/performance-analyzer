package com.amazon.opendistro.elasticsearch.performanceanalyzer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Map;

@Ignore
public class RootCauseAnalyzerIT extends ESRestTestCase {
    private static final Logger LOG = LogManager.getLogger(RootCauseAnalyzerIT.class);

    @Test
    public void ensureRcaEnabled() throws Exception {
        Request request = new Request("POST", "_opendistro/_performanceanalyzer/rca/cluster/config");
        request.setJsonEntity("{\"enabled\": true}");
        Response resp = client().performRequest(request);
        assert resp.getStatusLine().getStatusCode() == 200;
        LOG.info("RCA Initial state {}", EntityUtils.toString(resp.getEntity(), "UTF-8"));
        // TODO replace with waitFor
        Thread.sleep(10000L);
        resp = client().performRequest(new Request("GET", "_opendistro/_performanceanalyzer/rca/cluster/config"));
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> respMap = mapper.readValue(EntityUtils.toString(resp.getEntity(), "UTF-8"),
                new TypeReference<Map<String, Object>>(){});
        assert respMap.get("currentPerformanceAnalyzerClusterState").equals(1);
        LOG.info("RCA successfully enabled!!: {}", respMap);
    }
}
