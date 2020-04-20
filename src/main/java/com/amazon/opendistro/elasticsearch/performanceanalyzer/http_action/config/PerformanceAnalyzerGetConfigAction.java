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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PerformanceAnalyzerController;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.PerformanceAnalyzerConfigSettings.MUTED_RCAS;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.PerformanceAnalyzerConfigSettings.NODE_CONFIG_PATH;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.PerformanceAnalyzerConfigSettings.PA_ENABLED;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.PerformanceAnalyzerConfigSettings.PA_LOGGING_ENABLED;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.PerformanceAnalyzerConfigSettings.RCA_ENABLED;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.PerformanceAnalyzerConfigSettings.SHARDS_PER_COLLECTION;

/**
 * GET Rest request handler for handling node-level performance analyzer and RCA config settings, which include :
 * 1. Getting the status of PA, RCA and Logging Enabled
 * 2. Getting Shards_Per_Collection setting value
 * 3. Getting Muted_RCA_List setting value
 *
 */
@SuppressWarnings("deprecation")
public class PerformanceAnalyzerGetConfigAction extends BaseRestHandler {
    private static final Logger LOG = LogManager.getLogger(PerformanceAnalyzerGetConfigAction.class);
    private final PerformanceAnalyzerController performanceAnalyzerController;
    private static PerformanceAnalyzerGetConfigAction instance = null;

    public static PerformanceAnalyzerGetConfigAction getInstance() {
        return instance;
    }

    public static void setInstance(PerformanceAnalyzerGetConfigAction PerformanceAnalyzerGetConfigAction) {
        instance = PerformanceAnalyzerGetConfigAction;
    }

    public PerformanceAnalyzerGetConfigAction(final Settings settings,
                                              final RestController controller,
                                              final PerformanceAnalyzerController performanceAnalyzerController) {
        super(settings);
        this.performanceAnalyzerController = performanceAnalyzerController;
        registerHandlers(controller);
    }

    private void registerHandlers(final RestController controller) {
        controller.registerHandler(RestRequest.Method.GET, NODE_CONFIG_PATH, this);
    }

    @Override
    public String getName() {
        return "PerformanceAnalyzer_Config_Action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {
        return channel -> {
            try {
                channel.sendResponse(buildRestResponse(channel));
            } catch (IOException ioe) {
                LOG.error("Error sending response", ioe);
            }
        };
    }

    private BytesRestResponse buildRestResponse(final RestChannel channel) throws IOException {
        LOG.info("PerformanceAnalyzer: current value for PA enabled: {},  RCA enabled: {}, logging enabled: {}, "
                        + "shards per collection: {} and muted RCAs: {}",
                performanceAnalyzerController.isPerformanceAnalyzerEnabled(), performanceAnalyzerController.isRcaEnabled(),
                performanceAnalyzerController.isLoggingEnabled(), performanceAnalyzerController.getNodeStatsShardsPerCollection(),
                performanceAnalyzerController.getMutedRcas());

        XContentBuilder builder = channel.newBuilder();
        builder.startObject();
        builder.field(PA_ENABLED, performanceAnalyzerController.isPerformanceAnalyzerEnabled());
        builder.field(RCA_ENABLED, performanceAnalyzerController.isRcaEnabled());
        builder.field(PA_LOGGING_ENABLED, performanceAnalyzerController.isLoggingEnabled());
        builder.field(SHARDS_PER_COLLECTION, performanceAnalyzerController.getNodeStatsShardsPerCollection());
        builder.field(MUTED_RCAS, performanceAnalyzerController.getMutedRcas());
        builder.endObject();
        return new BytesRestResponse(RestStatus.OK, builder);
    }
}
