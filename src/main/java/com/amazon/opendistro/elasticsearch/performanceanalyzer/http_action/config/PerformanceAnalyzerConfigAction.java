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

import java.io.IOException;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PerformanceAnalyzerController;

@SuppressWarnings("deprecation")
public class PerformanceAnalyzerConfigAction extends BaseRestHandler {
    private static final Logger LOG = LogManager.getLogger(PerformanceAnalyzerConfigAction.class);
    private static final String ENABLED = "enabled";
    private static final String SHARDS_PER_COLLECTION = "shardsPerCollection";
    private static final String PA_ENABLED = "performanceAnalyzerEnabled";
    private static final String RCA_ENABLED = "rcaEnabled";
    private static final String PA_LOGGING_ENABLED = "loggingEnabled";
    private static final String BATCH_METRICS_ENABLED = "batchMetricsEnabled";
    private static PerformanceAnalyzerConfigAction instance = null;
    private final PerformanceAnalyzerController performanceAnalyzerController;
    private static final String RCA_CONFIG_PATH = "/_opendistro/_performanceanalyzer/rca/config";
    private static final String PA_CONFIG_PATH = "/_opendistro/_performanceanalyzer/config";
    private static final String LOGGING_CONFIG_PATH = "/_opendistro/_performanceanalyzer/logging/config";
    private static final String BATCH_METRICS_CONFIG_PATH = "/_opendistro/_performanceanalyzer/batch/config";

    public static PerformanceAnalyzerConfigAction getInstance() {
        return instance;
    }

    public static void setInstance(PerformanceAnalyzerConfigAction performanceanalyzerConfigAction) {
        instance = performanceanalyzerConfigAction;
    }

    @Inject
    public PerformanceAnalyzerConfigAction(final Settings settings,
                                           final RestController controller,
                                           final PerformanceAnalyzerController performanceAnalyzerController) {
        super(settings);
        this.performanceAnalyzerController = performanceAnalyzerController;
        registerHandlers(controller);
        LOG.info("PerformanceAnalyzer Enabled: {}", performanceAnalyzerController::isPerformanceAnalyzerEnabled);
    }

    private void registerHandlers(final RestController controller) {
        controller.registerHandler(RestRequest.Method.GET, PA_CONFIG_PATH, this);
        controller.registerHandler(RestRequest.Method.POST, PA_CONFIG_PATH, this);
        controller.registerHandler(RestRequest.Method.GET, RCA_CONFIG_PATH, this);
        controller.registerHandler(RestRequest.Method.POST, RCA_CONFIG_PATH, this);
        controller.registerHandler(RestRequest.Method.GET, LOGGING_CONFIG_PATH, this);
        controller.registerHandler(RestRequest.Method.POST, LOGGING_CONFIG_PATH, this);
        controller.registerHandler(RestRequest.Method.GET, BATCH_METRICS_CONFIG_PATH, this);
        controller.registerHandler(RestRequest.Method.POST, BATCH_METRICS_CONFIG_PATH, this);
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        if (request.method() == RestRequest.Method.POST && request.content().length() > 0) {
            // Let's try to find the name from the body
            Map<String, Object> map = XContentHelper.convertToMap(request.content(), false).v2();
            Object value = map.get(ENABLED);
            LOG.debug("PerformanceAnalyzer:Value (Object) Received as Part of Request: {} current value: {}", value,
                    performanceAnalyzerController.isPerformanceAnalyzerEnabled());
            if (value instanceof Boolean) {
                boolean shouldEnable = (Boolean) value;
                if (request.path().contains(RCA_CONFIG_PATH)) {
                    // If RCA needs to be turned on, we need to have PA turned on also.
                    // If this is not the case, return error.
                    if (shouldEnable && !performanceAnalyzerController.isPerformanceAnalyzerEnabled()) {
                        return getChannelConsumerWithError("Error: PA not enabled. Enable PA before turning RCA on");
                    }

                    performanceAnalyzerController.updateRcaState(shouldEnable);
                } else if (request.path().contains(LOGGING_CONFIG_PATH)) {
                    if (shouldEnable && !performanceAnalyzerController.isPerformanceAnalyzerEnabled()) {
                        return getChannelConsumerWithError("Error: PA not enabled. Enable PA before turning Logging on");
                    }

                    performanceAnalyzerController.updateLoggingState(shouldEnable);
                } else if (request.path().contains(BATCH_METRICS_CONFIG_PATH)) {
                    if (shouldEnable && !performanceAnalyzerController.isPerformanceAnalyzerEnabled()) {
                        return getChannelConsumerWithError("Error: PA not enabled. Enable PA before turning Batch Metrics on");
                    }

                    performanceAnalyzerController.updateBatchMetricsState(shouldEnable);
                } else {
                    // Disabling Performance Analyzer should disable the RCA framework as well.
                    if (!shouldEnable) {
                        performanceAnalyzerController.updateRcaState(false);
                        performanceAnalyzerController.updateLoggingState(false);
                        performanceAnalyzerController.updateBatchMetricsState(false);
                    }
                    performanceAnalyzerController.updatePerformanceAnalyzerState(shouldEnable);
                }
            }
            // update node stats setting if exists
            if (map.containsKey(SHARDS_PER_COLLECTION)) {
                Object shardPerCollectionValue = map.get(SHARDS_PER_COLLECTION);
                if (shardPerCollectionValue instanceof Integer) {
                    performanceAnalyzerController.updateNodeStatsShardsPerCollection((Integer)shardPerCollectionValue);
                }
            }
        }

        return channel -> {
            try {
                XContentBuilder builder = channel.newBuilder();
                builder.startObject();
                builder.field(PA_ENABLED, performanceAnalyzerController.isPerformanceAnalyzerEnabled());
                builder.field(RCA_ENABLED, performanceAnalyzerController.isRcaEnabled());
                builder.field(PA_LOGGING_ENABLED, performanceAnalyzerController.isLoggingEnabled());
                builder.field(SHARDS_PER_COLLECTION, performanceAnalyzerController.getNodeStatsShardsPerCollection());
                builder.field(BATCH_METRICS_ENABLED, performanceAnalyzerController.isBatchMetricsEnabled());
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            } catch (IOException ioe) {
                LOG.error("Error sending response", ioe);
            }
        };
    }

    @Override
    public String getName() {
        return "PerformanceAnalyzer_Config_Action";
    }

    private RestChannelConsumer getChannelConsumerWithError(String error) {
        return restChannel -> {
            XContentBuilder builder = restChannel.newErrorBuilder();
            builder.startObject();
            builder.field(error);
            builder.endObject();
            restChannel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, builder));
        };
    }
}
