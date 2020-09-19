package com.amazon.opendistro.elasticsearch.performanceanalyzer;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.PerformanceAnalyzerClusterSettings.PerformanceAnalyzerFeatureBits;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.handler.PerformanceAnalyzerClusterSettingHandler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.WaitFor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public abstract class PerformanceAnalyzerIntegTestBase extends ESRestTestCase {
    private static final Logger LOG = LogManager.getLogger(PerformanceAnalyzerIntegTestBase.class);
    protected static final String PERFORMANCE_ANALYZER_BASE_ENDPOINT = "/_opendistro/_performanceanalyzer";
    private int paPort;
    protected static final ObjectMapper mapper = new ObjectMapper();
    // TODO this must be initialized at construction time to avoid NPEs, we should find a way for subclasses to override this
    protected ITConfig config = new ITConfig();
    protected static RestClient paClient;
    protected static final String METHOD_GET = "GET";
    protected static final String METHOD_POST = "POST";

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

    protected List<HttpHost> getHosts(int port) {
        String cluster = config.getRestEndpoint();
        logger.info("Cluster is {}", cluster);
        if (cluster == null) {
            throw new RuntimeException("Must specify [tests.rest.cluster] system property with a comma delimited list of [host:port] "
                    + "to which to send REST requests");
        }
        return Collections.singletonList(
                new HttpHost(cluster.substring(0, cluster.lastIndexOf(":")), port, "http"));
    }

    @Before
    public void setupIT() throws Exception {
        paPort = config.getPaPort();
        List<HttpHost> hosts = getHosts(paPort);
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
                endpoint = PERFORMANCE_ANALYZER_BASE_ENDPOINT + "/cluster/config";
                break;
            case RCA:
                endpoint = PERFORMANCE_ANALYZER_BASE_ENDPOINT + "/rca/cluster/config";
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
            new Request("GET", PERFORMANCE_ANALYZER_BASE_ENDPOINT + "/cluster/config"));
        Map<String, Object> respMap = mapper
            .readValue(EntityUtils.toString(resp.getEntity(), "UTF-8"),
                new TypeReference<Map<String, Object>>() {
                });
        Integer state = (Integer) respMap.get("currentPerformanceAnalyzerClusterState");
        Assert.assertTrue("PA and RCA are not enabled on the target cluster!",
            PerformanceAnalyzerClusterSettingHandler.checkBit(state, PerformanceAnalyzerFeatureBits.PA_BIT.ordinal()) &&
                PerformanceAnalyzerClusterSettingHandler.checkBit(state, PerformanceAnalyzerFeatureBits.RCA_BIT.ordinal()));
    }


    @After
    public void closePaClient() throws Exception {
        ESRestTestCase.closeClients();
        paClient.close();
        LOG.debug("AfterClass has run");
    }

    protected static class TestUtils {
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
