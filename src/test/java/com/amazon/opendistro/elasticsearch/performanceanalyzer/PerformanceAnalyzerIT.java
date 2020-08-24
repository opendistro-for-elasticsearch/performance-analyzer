package com.amazon.opendistro.elasticsearch.performanceanalyzer;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.PerformanceAnalyzerClusterSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.handler.PerformanceAnalyzerClusterSettingHandler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.WaitFor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Objects;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.message.BasicHeader;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
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
    private int paPort;
    private static final ObjectMapper mapper = new ObjectMapper();
    // TODO this must be initialized at construction time to avoid NPEs, we should find a way for subclasses to override this
    private ITConfig config = new ITConfig();
    private static RestClient paClient;

    // Don't wipe the cluster after test completion
    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    protected boolean isHttps() {
        return config.isHttps();
    }

    @Override
    protected String getProtocol() {
        if (isHttps()) {
            return "https";
        }
        return super.getProtocol();
    }

    protected RestClient buildBasicClient(Settings settings, HttpHost[] hosts) throws Exception {
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

    protected RestClient buildClient(Settings settings, HttpHost[] hosts) throws IOException {
        RestClientBuilder builder = RestClient.builder(hosts);
        if (isHttps()) {
            LOG.info("Setting up https client");
            configureHttpsClient(builder, settings);
        } else {
            configureClient(builder, settings);
        }
        builder.setStrictDeprecationMode(true);
        return builder.build();
    }

    public static Map<String, String> buildDefaultHeaders(Settings settings) {
        Settings headers = ThreadContext.DEFAULT_HEADERS_SETTING.get(settings);
        if (headers == null) {
            return Collections.emptyMap();
        } else {
            Map<String, String> defaultHeader = new HashMap<>();
            for (String key : headers.names()) {
                defaultHeader.put(key, headers.get(key));
            }
            return Collections.unmodifiableMap(defaultHeader);
        }
    }

    protected void configureHttpsClient(RestClientBuilder builder, Settings settings) {
        Map<String, String> headers = buildDefaultHeaders(settings);
        Header[] defaultHeaders = new Header[headers.size()];
        int i = 0;
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            defaultHeaders[i++] = new BasicHeader(entry.getKey(), entry.getValue());
        }
        builder.setDefaultHeaders(defaultHeaders);
        builder.setHttpClientConfigCallback((HttpAsyncClientBuilder httpClientBuilder) -> {
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider
                .setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(config.getUser(), config.getPassword()));
            try {
                return httpClientBuilder
                    .setDefaultCredentialsProvider(credentialsProvider)
                    .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                    .setSSLContext(SSLContextBuilder
                        .create()
                        .loadTrustMaterial(null, (X509Certificate[] chain, String authType) -> true)
                        .build());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        String socketTimeoutString = settings.get(CLIENT_SOCKET_TIMEOUT);
        if (socketTimeoutString == null) {
            socketTimeoutString = "60s";
            TimeValue socketTimeout = TimeValue
                .parseTimeValue(socketTimeoutString, CLIENT_SOCKET_TIMEOUT);
            builder.setRequestConfigCallback((RequestConfig.Builder conf) ->
                conf.setSocketTimeout(Math.toIntExact(socketTimeout.millis())));
            if (settings.hasValue(CLIENT_PATH_PREFIX)) {
                builder.setPathPrefix(settings.get(CLIENT_PATH_PREFIX));
            }
        }
    }

    @Before
    public void setupIT() throws Exception {
        String cluster = config.getRestEndpoint();
        paPort = config.getPaPort();
        logger.info("Cluster is {}", cluster);
        if (cluster == null) {
            throw new RuntimeException("Must specify [tests.rest.cluster] system property with a comma delimited list of [host:port] "
                    + "to which to send REST requests");
        }
        List<HttpHost> hosts = Collections.singletonList(
                new HttpHost(cluster.substring(0, cluster.lastIndexOf(":")), paPort, "http"));
        logger.info("initializing PerformanceAnalyzer client against {}", hosts);
        paClient = buildBasicClient(restClientSettings(), hosts.toArray(new HttpHost[0]));
    }

    private enum Component {
        PA,
        RCA
    }

    /**
     * enableComponent enables PA or RCA on the test cluster
     * @param component Either PA or RCA
     * @return The cluster's {@link Response}
     */
    public Response enableComponent(Component component) throws Exception {
        String endpoint;
        switch (component) {
            case PA:
                endpoint = "_opendistro/_performanceanalyzer/cluster/config";
                break;
            case RCA:
                endpoint = "_opendistro/_performanceanalyzer/rca/cluster/config";
                break;
            default:
                throw new IllegalArgumentException("Unrecognized component value " + component.toString());
        }
        Request request = new Request("POST", endpoint);
        request.setJsonEntity("{\"enabled\": true}");
        return client().performRequest(request);
    }


    /**
     * ensurePaAndRcaEnabled makes a best effort to enable PA and RCA on the test ES cluster
     * @throws Exception If the function is unable to enable PA and RCA
     */
    public void ensurePaAndRcaEnabled() throws Exception {
        // Attempt to enable PA and RCA on the cluster
        WaitFor.waitFor(() -> {
            try {
                Response paResp = enableComponent(Component.PA);
                Response rcaResp = enableComponent(Component.RCA);
                return paResp.getStatusLine().getStatusCode() == HttpStatus.SC_OK &&
                    rcaResp.getStatusLine().getStatusCode() == HttpStatus.SC_OK;
            } catch (Exception e) {
                return false;
            }
        }, 1, TimeUnit.MINUTES);

        // Sanity check that PA and RCA are enabled on the cluster
        Response resp = client().performRequest(
            new Request("GET", "_opendistro/_performanceanalyzer/cluster/config"));
        Map<String, Object> respMap = mapper
            .readValue(EntityUtils.toString(resp.getEntity(), "UTF-8"),
                new TypeReference<Map<String, Object>>() {
                });
        Integer state = (Integer) respMap.get("currentPerformanceAnalyzerClusterState");
        Assert.assertTrue("PA and RCA are not enabled on the target cluster!",
            PerformanceAnalyzerClusterSettingHandler.checkBit(state, PerformanceAnalyzerClusterSettingHandler.PA_ENABLED_BIT_POS) &&
                PerformanceAnalyzerClusterSettingHandler.checkBit(state, PerformanceAnalyzerClusterSettingHandler.RCA_ENABLED_BIT_POS));
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
            try {
                Response resp = paClient.performRequest(request);
                return Objects.equals(HttpStatus.SC_OK, resp.getStatusLine().getStatusCode());
            } catch (Exception e) { // 404, RCA context hasn't been set up yet
                return false;
            }
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
