/*
 * Copyright <2019> Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.http_action.config;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ConfigStatus;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.ESResources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerPlugin;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

@SuppressWarnings("deprecation")
public class PerformanceAnalyzerConfigAction extends BaseRestHandler {
    private boolean featureEnabled;
    private static final Logger LOG = LogManager.getLogger(PerformanceAnalyzerConfigAction.class);
    private static final String ENABLED = "enabled";
    private static PerformanceAnalyzerConfigAction instance = null;
    private boolean isInitialized = false;
    private boolean featureEanbledDefaultValue = true;

    public static PerformanceAnalyzerConfigAction getInstance() {
        return instance;
    }

    public static void setInstance(PerformanceAnalyzerConfigAction performanceanalyzerConfigAction) {
        instance = performanceanalyzerConfigAction;
    }

    private static final String METRIC_ENABLED_CONF_FILENAME = "performance_analyzer_enabled.conf";
    @Inject
    public PerformanceAnalyzerConfigAction(RestController controller) {
        super();
        this.featureEnabled = getFeatureEnabledFromConf();
        LOG.info("PerformanceAnalyzer Enabled: {}", this.featureEnabled);
    }

    @Override
    public List<Route> routes() {
        return  unmodifiableList(asList(
                new Route(org.elasticsearch.rest.RestRequest.Method.GET, "/_opendistro/_performanceanalyzer/config"),
                new Route(org.elasticsearch.rest.RestRequest.Method.POST, "/_opendistro/_performanceanalyzer/config")
        ));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (request.method() == RestRequest.Method.POST && request.content().length() > 0) {
            // Let's try to find the name from the body
            Map<String, Object> map = XContentHelper.convertToMap(request.content(), false).v2();
            Object value = map.get(ENABLED);
            LOG.debug("PerformanceAnalyzer:Value (Object) Received as Part of Request: {} current value:", this.featureEnabled);
            if (value != null && value instanceof Boolean) {
                boolean bValue = (Boolean) value;
                LOG.debug("PerformanceAnalyzer:Value (Boolean) Received as Part of Request: {}  current value: {}",
                        bValue, this.featureEnabled);
                if (this.featureEnabled != bValue) {
                    this.featureEnabled = (Boolean) value;
                    saveFeatureEnabledToConf(this.featureEnabled);
                }
            }
        }

        return channel -> {
            try {
                XContentBuilder builder = channel.newBuilder();
                builder.startObject();
                builder.field(ENABLED, this.featureEnabled);
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            } catch (IOException ioe) {
                LOG.error("Error sending response", ioe);
            }
        };
    }

    public boolean isFeatureEnabled() {
        return featureEnabled() && ConfigStatus.INSTANCE.haveValidConfig();
    }

    private boolean featureEnabled() {
        return isInitialized ? featureEnabled : getFeatureEnabledFromConf();
    }

    @Override
    public String getName() {
        return "PerformanceAnalyzer_Config_Action";
    }

    private String getDataDirectory() {
        return new org.elasticsearch.env.Environment(ESResources.INSTANCE.getSettings(),
                ESResources.INSTANCE.getConfigPath()).dataFiles()[0].toFile().getPath();
    }

    private void saveFeatureEnabledToConf(boolean featureEnabled) {
        PerformanceAnalyzerPlugin.invokePrivileged(() -> {
            try {
                Files.write(
                        Paths.get(getDataDirectory() + File.separator + METRIC_ENABLED_CONF_FILENAME),
                        String.valueOf(featureEnabled).getBytes());
            } catch(Exception ex) {
                LOG.error(ex.toString(), ex);
            }
        });
    }

    private boolean getFeatureEnabledFromConf() {
        Path filePath = Paths.get(getDataDirectory(), METRIC_ENABLED_CONF_FILENAME);

        PerformanceAnalyzerPlugin.invokePrivileged(() -> {
            try (Scanner sc = new Scanner(filePath)) {
                String nextLine = sc.nextLine();
                featureEnabled = Boolean.parseBoolean(nextLine);
                isInitialized = true;
            } catch (java.nio.file.NoSuchFileException ex) {
                saveFeatureEnabledToConf(featureEanbledDefaultValue);
                isInitialized = true;
                featureEnabled = featureEanbledDefaultValue;
            } catch (Exception e) {
                LOG.error("Error reading Feature Enabled from Conf file", e);
                featureEnabled = featureEanbledDefaultValue;
            }
        });
        return featureEnabled;
    }

}
