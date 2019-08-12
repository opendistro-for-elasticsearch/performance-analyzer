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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Scanner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ConfigStatus;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.ESResources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerPlugin;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.SettingsHelper;

@SuppressWarnings("deprecation")
public class PerformanceAnalyzerConfigAction extends BaseRestHandler {
    private boolean featureEnabled;
    private static final Logger LOG = LogManager.getLogger(PerformanceAnalyzerConfigAction.class);
    private static final String ENABLED = "enabled";
    private static final String PA_ENABLED_CLUSTER_SETTING_PATH = "/_opendistro/_performanceanalyzer/cluster/config";
    private static final String CURRENT = "current";
    private static final String DESIRED = "desired";
    private static PerformanceAnalyzerConfigAction instance = null;
    private boolean isInitialized = false;
    private boolean featureEnabledDefaultValue = true;
    private ActionListener<ClusterStateResponse> clusterStateResponseHandler = new ClusterStateResponseHandler();
    private ActionListener<ClusterUpdateSettingsResponse> clusterUpdateSettingsResponseHandler = new ClusterUpdateSettingsResponseHandler();
    private ClusterStateListener clusterStateListener = new ClusterStateUpdateListener();

    public static PerformanceAnalyzerConfigAction getInstance() {
        return instance;
    }

    public static void setInstance(PerformanceAnalyzerConfigAction performanceanalyzerConfigAction) {
        instance = performanceanalyzerConfigAction;
    }

    private static final String METRIC_ENABLED_CONF_FILENAME = "performance_analyzer_enabled.conf";

    @Inject
    public PerformanceAnalyzerConfigAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(org.elasticsearch.rest.RestRequest.Method.GET, "/_opendistro/_performanceanalyzer/config", this);
        controller.registerHandler(org.elasticsearch.rest.RestRequest.Method.POST, "/_opendistro/_performanceanalyzer/config", this);

        controller.registerHandler(RestRequest.Method.POST, PA_ENABLED_CLUSTER_SETTING_PATH, this);
        controller.registerHandler(RestRequest.Method.GET, PA_ENABLED_CLUSTER_SETTING_PATH, this);

        this.featureEnabled = getFeatureEnabledFromConf();
        getFeatureEnabledFromClusterSettings();
        registerClusterUpdateSettingsListener();
        LOG.info("PerformanceAnalyzer Enabled: {}", this.featureEnabled);
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        if (request.method() == RestRequest.Method.POST && request.content().length() > 0) {
            // Let's try to find the name from the body
            Map<String, Object> map = XContentHelper.convertToMap(request.content(), false).v2();
            Object value = map.get(ENABLED);
            if (request.path().startsWith(PA_ENABLED_CLUSTER_SETTING_PATH)) {
                if (value instanceof Boolean) {
                    boolean bValue = (Boolean) value;
                    updatePerformanceAnalyzerClusterSetting(bValue);
                    return channel -> {
                        try {
                            XContentBuilder builder = channel.newBuilder();
                            builder.startObject();
                            builder.field(CURRENT, this.featureEnabled);
                            builder.field(DESIRED, bValue);
                            builder.endObject();
                            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                        } catch (IOException ioe) {
                            LOG.error("Error sending response", ioe);
                        }
                    };
                }
            } else {
                LOG.debug("PerformanceAnalyzer:Value (Object) Received as Part of Request: {} current value:", this.featureEnabled);
                if (value instanceof Boolean) {
                    boolean bValue = (Boolean) value;
                    LOG.debug("PerformanceAnalyzer:Value (Boolean) Received as Part of Request: {}  current value: {}",
                            bValue, this.featureEnabled);
                    if (this.featureEnabled != bValue) {
                        this.featureEnabled = (Boolean) value;
                        saveFeatureEnabledToConf(this.featureEnabled);
                    }
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
        return new org.elasticsearch.env.Environment(
                ESResources.INSTANCE.getSettings(),
                ESResources.INSTANCE.getConfigPath()).dataFiles()[0].toFile().getPath();
    }

    private void saveFeatureEnabledToConf(boolean featureEnabled) {
        PerformanceAnalyzerPlugin.invokePrivileged(() -> {
            try {
                Files.write(
                        Paths.get(getDataDirectory() + File.separator + METRIC_ENABLED_CONF_FILENAME),
                        String.valueOf(featureEnabled).getBytes());
            } catch (Exception ex) {
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
                saveFeatureEnabledToConf(featureEnabledDefaultValue);
                isInitialized = true;
                featureEnabled = featureEnabledDefaultValue;
            } catch (Exception e) {
                LOG.error("Error reading Feature Enabled from Conf file", e);
                featureEnabled = featureEnabledDefaultValue;
            }
        });
        return featureEnabled;
    }

    private void updatePerformanceAnalyzerClusterSetting(final Boolean value) {
        final ClusterUpdateSettingsRequest clusterUpdateSettingsRequest = new ClusterUpdateSettingsRequest();
        clusterUpdateSettingsRequest.persistentSettings(Settings.builder()
                                                                .put(SettingsHelper.getPerformanceAnalyzerClusterSetting().getKey(), value)
                                                                .build());

        ESResources.INSTANCE.getClient()
                            .admin()
                            .cluster()
                            .updateSettings(clusterUpdateSettingsRequest, clusterUpdateSettingsResponseHandler);
    }

    private void registerClusterUpdateSettingsListener() {
        ESResources.INSTANCE.getClusterService()
                            .getClusterSettings()
                            .addSettingsUpdateConsumer(
                                    SettingsHelper.getPerformanceAnalyzerClusterSetting(),
                                    isEnabled -> {
                                        this.featureEnabled = isEnabled;
                                        saveFeatureEnabledToConf(isEnabled);
                                    });
    }

    private void getFeatureEnabledFromClusterSettings() {
        try {
            final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
            clusterStateRequest.routingTable(false)
                               .nodes(false);

            ESResources.INSTANCE.getClient()
                                .admin()
                                .cluster()
                                .state(clusterStateRequest, clusterStateResponseHandler);
        } catch (Exception | AssertionError e) {
            LOG.warn("Unable to retrieve cluster metadata at this time. Re-registering listener. {}", e.getMessage());
            registerClusterStateUpdateHandler();
        }
    }

    private void registerClusterStateUpdateHandler() {
        ESResources.INSTANCE.getClusterService().addListener(clusterStateListener);
    }

    private void updateFeatureEnabled(final ClusterStateResponse clusterStateResponse) {
        this.featureEnabled = clusterStateResponse.getState()
                                                  .getMetaData()
                                                  .persistentSettings()
                                                  .getAsBoolean(SettingsHelper.getPerformanceAnalyzerClusterSetting()
                                                                              .getKey(), false);
        saveFeatureEnabledToConf(featureEnabled);
    }

    private void updateFeatureEnabled(final ClusterUpdateSettingsResponse clusterUpdateSettingsResponse) {
        if (clusterUpdateSettingsResponse.isAcknowledged()) {
            this.featureEnabled = clusterUpdateSettingsResponse.getPersistentSettings()
                                                               .getAsBoolean(SettingsHelper.getPerformanceAnalyzerClusterSetting()
                                                                                           .getKey(), false);
            saveFeatureEnabledToConf(featureEnabled);
        } else {
            LOG.debug("Cluster setting update was not acknowledged. Not applying the update.");
        }
    }

    /**
     * Class that listens to cluster state changes. Used only during initialization
     * when the plugin is started before a cluster state is set and we need to query
     * the cluster settings to fetch PA setting for the first time.
     */
    private class ClusterStateUpdateListener implements ClusterStateListener {

        /**
         * Called when cluster state changes.
         * Attempt to read cluster settings.
         *
         * @param event Cluster changed event.
         */
        @Override
        public void clusterChanged(final ClusterChangedEvent event) {
            ESResources.INSTANCE.getClusterService().removeListener(clusterStateListener);
            getFeatureEnabledFromClusterSettings();
        }
    }

    /**
     * Class that handles the response to GET /_cluster/settings request.
     */
    private class ClusterStateResponseHandler implements ActionListener<ClusterStateResponse> {

        /**
         * Handle action response. This response may constitute a failure or a
         * success but it is up to the listener to make that decision.
         *
         * @param clusterStateResponse The response that contains settings and cluster metadata.
         */
        @Override
        public void onResponse(final ClusterStateResponse clusterStateResponse) {
            ESResources.INSTANCE.getClusterService().removeListener(clusterStateListener);
            updateFeatureEnabled(clusterStateResponse);
        }

        /**
         * A failure caused by an exception at some phase of the task.
         *
         * @param e
         */
        @Override
        public void onFailure(final Exception e) {
            registerClusterStateUpdateHandler();
            LOG.warn("Unable fetch cluster settings. Exception: {}", e.getMessage());
        }
    }

    /**
     * Class that handles response to POST /_cluster/settings request.
     */
    private class ClusterUpdateSettingsResponseHandler implements ActionListener<ClusterUpdateSettingsResponse> {

        /**
         * Handle action response. This response may constitute a failure or a
         * success but it is up to the listener to make that decision.
         *
         * @param clusterUpdateSettingsResponse Response from the cluster applier service.
         */
        @Override
        public void onResponse(final ClusterUpdateSettingsResponse clusterUpdateSettingsResponse) {
            updateFeatureEnabled(clusterUpdateSettingsResponse);
        }

        /**
         * A failure caused by an exception at some phase of the task.
         *
         * @param e
         */
        @Override
        public void onFailure(final Exception e) {
            LOG.warn("Unable to update cluster setting. Exception: {}", e.getMessage());
        }
    }
}
