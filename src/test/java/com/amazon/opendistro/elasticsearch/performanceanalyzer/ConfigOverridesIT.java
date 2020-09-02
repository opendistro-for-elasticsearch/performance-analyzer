package com.amazon.opendistro.elasticsearch.performanceanalyzer;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides.ConfigOverrides;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.WaitFor;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.HttpStatus;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ConfigOverridesIT extends PerformanceAnalyzerIntegTestBase {
    private static final String CONFIG_OVERRIDES_ENDPOINT = PERFORMANCE_ANALYZER_BASE_ENDPOINT + "/override/cluster/config";
    private static final String METHOD_GET = "GET";
    private static final String METHOD_POST = "POST";

    private static final List<String> EMPTY_LIST = Collections.emptyList();
    public static final String HOT_SHARD_RCA = "HotShardRca";
    public static final String HOT_NODE_CLUSTER_RCA = "HotNodeClusterRca";

    @Test
    public void testSimpleOverride() throws Exception {
        ensurePaAndRcaEnabled();
        final ConfigOverrides overrides = getOverrides(Arrays.asList(HOT_SHARD_RCA, HOT_NODE_CLUSTER_RCA),
                EMPTY_LIST, EMPTY_LIST, EMPTY_LIST, EMPTY_LIST, EMPTY_LIST);
        final Request postRequest = new Request(METHOD_POST, CONFIG_OVERRIDES_ENDPOINT);
        postRequest.setJsonEntity(mapper.writeValueAsString(overrides));

        try {
            final Response response = client().performRequest(postRequest);
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
        } catch (Exception e) {
            logger.error("Encountered exception: {}", e.getMessage(), e);
            fail("Failed to set overrides");
        }

        try {
            Map<String, Object> responseEntity = getAsMap(CONFIG_OVERRIDES_ENDPOINT);
            String serializedOverrides = (String) responseEntity.get("overrides");
            final ConfigOverrides computedOverrides = mapper.readValue(serializedOverrides, ConfigOverrides.class);
            Assert.assertTrue(areEqual(overrides, computedOverrides));
        } catch (Exception e) {
            logger.error("Encountered exception: {}", e.getMessage(), e);
            fail("Failed to retrieve or deserialize retrieved overrides");
        }
    }

    @Test
    public void testCompositeOverrides() throws Exception {
        ensurePaAndRcaEnabled();

        final ConfigOverrides initialOverrides = getOverrides(Arrays.asList(HOT_SHARD_RCA, HOT_NODE_CLUSTER_RCA), EMPTY_LIST,
                EMPTY_LIST, EMPTY_LIST, EMPTY_LIST, EMPTY_LIST);

        final Request postRequest = new Request(METHOD_POST, CONFIG_OVERRIDES_ENDPOINT);
        postRequest.setJsonEntity(mapper.writeValueAsString(initialOverrides));

        try {
            final Response response = client().performRequest(postRequest);
            Assert.assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
        } catch (Exception e) {
            logger.error("Encountered exception: {}", e.getMessage(), e);
            fail("Failed to set overrides");
        }

        WaitFor.waitFor(() -> {
            final Request getRequest = new Request(METHOD_GET, CONFIG_OVERRIDES_ENDPOINT);
            try {
                final Response response = client().performRequest(getRequest);
                Assert.assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
                JsonObject jsonResponse = (JsonObject) JsonParser.parseReader(new InputStreamReader(response.getEntity().getContent()));
                String serializedOverrides = jsonResponse.get("overrides").getAsString();
                final ConfigOverrides computedOverrides = mapper.readValue(serializedOverrides, ConfigOverrides.class);
                return areEqual(initialOverrides, computedOverrides);
            } catch (Exception e) {
                logger.error("Ran into: {}", e.getMessage(), e);
                return false;
            }
        }, 2, TimeUnit.MINUTES);

        final ConfigOverrides adjustedOverrides = getOverrides(EMPTY_LIST, EMPTY_LIST, EMPTY_LIST,
                Collections.singletonList(HOT_NODE_CLUSTER_RCA), EMPTY_LIST, EMPTY_LIST);

        final Request postRequestAdjusted = new Request(METHOD_POST, CONFIG_OVERRIDES_ENDPOINT);
        postRequestAdjusted.setJsonEntity(mapper.writeValueAsString(adjustedOverrides));

        try {
            final Response response = client().performRequest(postRequestAdjusted);
            assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
        } catch (Exception e) {
            logger.error("Encountered exception: {}", e.getMessage(), e);
            fail("Failed to set adjusted overrides");
        }

        final ConfigOverrides expectedOverrides = getOverrides(Collections.singletonList(HOT_SHARD_RCA), EMPTY_LIST, EMPTY_LIST,
                Collections.singletonList(HOT_NODE_CLUSTER_RCA), EMPTY_LIST, EMPTY_LIST);

        WaitFor.waitFor(() -> {
            final Request getRequest = new Request(METHOD_GET, CONFIG_OVERRIDES_ENDPOINT);
            try {
                final Response response = client().performRequest(getRequest);
                Assert.assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
                JsonObject jsonResponse = (JsonObject) JsonParser.parseReader(new InputStreamReader(response.getEntity().getContent()));
                String serializedOverrides = jsonResponse.get("overrides").getAsString();
                final ConfigOverrides computedOverrides = mapper.readValue(serializedOverrides, ConfigOverrides.class);
                return areEqual(expectedOverrides, computedOverrides);
            } catch (Exception e) {
                logger.error("Encountered exception: {}", e.getMessage(), e);
            }
            return false;
        }, 2, TimeUnit.MINUTES);
    }

    private boolean areEqual(final ConfigOverrides first, final ConfigOverrides second) {
        return areEqual(first.getEnable(), second.getEnable()) && areEqual(first.getDisable(), second.getDisable());
    }

    private boolean areEqual(final ConfigOverrides.Overrides first, final ConfigOverrides.Overrides second) {
        if (first != null) {
            assertNotNull(second);

            return areEqual(first.getRcas(), second.getRcas()) &&
                    areEqual(first.getDeciders(), second.getDeciders()) &&
                    areEqual(first.getActions(), second.getActions());
        } else {
            assertNull(second);
        }

        return true;
    }

    private boolean areEqual(final List<String> first, final List<String> second) {
        if (first != null) {
            assertNotNull(second);
            Set<String> firstSet = new HashSet<>(first);
            Set<String> secondSet = new HashSet<>(second);

            return firstSet.equals(secondSet);
        } else {
            assertNull(second);
        }

        return true;
    }

    private ConfigOverrides getOverrides(List<String> enableRcas, List<String> enableDeciders,
                                         List<String> enableActions, List<String> disableRcas,
                                         List<String> disableDeciders, List<String> disableActions) {
        final ConfigOverrides overrides = new ConfigOverrides();
        final ConfigOverrides.Overrides enableOverrides = new ConfigOverrides.Overrides();
        final ConfigOverrides.Overrides disableOverrides = new ConfigOverrides.Overrides();

        enableOverrides.setRcas(enableRcas);
        enableOverrides.setDeciders(enableDeciders);
        enableOverrides.setActions(enableActions);

        disableOverrides.setRcas(disableRcas);
        disableOverrides.setDeciders(disableDeciders);
        disableOverrides.setActions(disableActions);

        overrides.setEnable(enableOverrides);
        overrides.setDisable(disableOverrides);

        return overrides;
    }
}
