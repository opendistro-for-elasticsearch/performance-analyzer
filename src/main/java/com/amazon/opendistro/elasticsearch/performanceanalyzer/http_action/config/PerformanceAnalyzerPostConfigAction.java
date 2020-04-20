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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.util.Map;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.PerformanceAnalyzerConfigSettings.MUTED_RCAS;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.PerformanceAnalyzerConfigSettings.NODE_CONFIG_PATH;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.PerformanceAnalyzerConfigSettings.PA_ENABLED;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.PerformanceAnalyzerConfigSettings.PA_LOGGING_ENABLED;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.PerformanceAnalyzerConfigSettings.RCA_ENABLED;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.PerformanceAnalyzerConfigSettings.SHARDS_PER_COLLECTION;

/**
 * POST Rest request handler for handling node-level performance analyzer and RCA config settings, which include :
 * 1. Enable and Disable support for PA, RCA and PA Logging
 * 2. Setting Shards_Per_Collection setting value
 * 3. Setting Muted_RCA_List setting value
 *
 */
@SuppressWarnings("deprecation")
public class PerformanceAnalyzerPostConfigAction extends BaseRestHandler {
    private static final Logger LOG = LogManager.getLogger(PerformanceAnalyzerPostConfigAction.class);
    private static PerformanceAnalyzerPostConfigAction instance = null;
    private final PerformanceAnalyzerController performanceAnalyzerController;

    public static PerformanceAnalyzerPostConfigAction getInstance() {
        return instance;
    }

    public static void setInstance(PerformanceAnalyzerPostConfigAction performanceanalyzerConfigAction) {
        instance = performanceanalyzerConfigAction;
    }

    @Inject
    public PerformanceAnalyzerPostConfigAction(final Settings settings,
                                               final RestController controller,
                                               final PerformanceAnalyzerController performanceAnalyzerController) {
        super(settings);
        this.performanceAnalyzerController = performanceAnalyzerController;
        registerHandlers(controller);
    }

    private void registerHandlers(final RestController controller) {
        controller.registerHandler(RestRequest.Method.POST, NODE_CONFIG_PATH, this);
    }

    @Override
    public String getName() {
        return "PerformanceAnalyzer_Config_Action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {
        return channel -> {
            // Parse the request body to get the config setting name and value
            final Map<String, Object> paramMap = XContentHelper.convertToMap(request.content(), false).v2();
            channel.sendResponse(buildRestResponse(channel, paramMap));
        };
    }

    private BytesRestResponse buildRestResponse(final RestChannel channel, Map<String, Object> paramMap) throws Exception {
        XContentBuilder builder = channel.newBuilder();
        builder.startObject();
        RestStatus status = RestStatus.OK;
        LOG.info("PerformanceAnalyzer: parameters received as part of Request: {}, current value for PA enabled: {}, "
                + "RCA enabled: {}, logging enabled: {}, shards per collection: {} and muted RCAs: {}", paramMap.toString(),
                performanceAnalyzerController.isPerformanceAnalyzerEnabled(), performanceAnalyzerController.isRcaEnabled(),
                performanceAnalyzerController.isLoggingEnabled(), performanceAnalyzerController.getNodeStatsShardsPerCollection(),
                performanceAnalyzerController.getMutedRcas());

        {
            // 1. Performance Analyzer enable/disable setting
            final boolean paEnabledPresent = (paramMap.get(PA_ENABLED) != null);
            if (paEnabledPresent) {
                final boolean paEnabledValue = (boolean) paramMap.get(PA_ENABLED);
                if (!paEnabledValue) {
                    performanceAnalyzerController.updateRcaState(false);
                    performanceAnalyzerController.updateLoggingState(false);
                }
                performanceAnalyzerController.updatePerformanceAnalyzerState(paEnabledValue);
            }
            builder.field(PA_ENABLED, performanceAnalyzerController.isPerformanceAnalyzerEnabled());

            // 2. RCA enable/disable setting
            final boolean rcaEnabledPresent = (paramMap.get(RCA_ENABLED) != null);
            if (rcaEnabledPresent) {
                // If RCA needs to be turned on, we need to have PA turned on also.
                // If this is not the case, return error.
                final boolean rcaEnabledValue = (boolean) paramMap.get(RCA_ENABLED);
                if (rcaEnabledValue && !performanceAnalyzerController.isPerformanceAnalyzerEnabled()) {
                    status = RestStatus.BAD_REQUEST;
                    builder.field("error", "Error: PA not enabled. Enable PA before turning RCA on");
                }
                performanceAnalyzerController.updateRcaState(rcaEnabledValue);
            }
            builder.field(RCA_ENABLED, performanceAnalyzerController.isRcaEnabled());

            // 3. Logging enable/disable setting
            final boolean loggingEnabledPresent = (paramMap.get(PA_LOGGING_ENABLED) != null);
            if (loggingEnabledPresent) {
                final boolean loggingEnabledValue = (boolean) paramMap.get(PA_LOGGING_ENABLED);
                if (loggingEnabledValue && !performanceAnalyzerController.isPerformanceAnalyzerEnabled()) {
                    status = RestStatus.BAD_REQUEST;
                    builder.field("error", "Error: PA not enabled. Enable PA before turning Logging on");
                }
                performanceAnalyzerController.updateLoggingState(loggingEnabledValue);
            }
            builder.field(PA_LOGGING_ENABLED, performanceAnalyzerController.isLoggingEnabled());

            // 4. Shards Per Collection node stat setting
            final boolean shardsPerCollectionPresent = (paramMap.get(SHARDS_PER_COLLECTION) != null);
            if (shardsPerCollectionPresent) {
                final int shardsPerCollectionValue = (Integer) paramMap.get(SHARDS_PER_COLLECTION);
                performanceAnalyzerController.updateNodeStatsShardsPerCollection(shardsPerCollectionValue);
            }
            builder.field(SHARDS_PER_COLLECTION, performanceAnalyzerController.getNodeStatsShardsPerCollection());

            // 5. Muted RCAs setting
            final boolean mutedRcasPresent = (paramMap.get(MUTED_RCAS) != null);
            if (mutedRcasPresent) {
                if(!performanceAnalyzerController.isPerformanceAnalyzerEnabled() || !performanceAnalyzerController.isRcaEnabled()) {
                    status = RestStatus.BAD_REQUEST;
                    builder.field("error", "Error: PA or RCA not enabled. Enable PA and RCA before setting Muted RCAs");
                }
                final String mutedRcasValue = (String) paramMap.get(MUTED_RCAS);
                performanceAnalyzerController.updateMutedRcasState(mutedRcasValue);
            }
        }
        builder.field(MUTED_RCAS, performanceAnalyzerController.getMutedRcas());
        builder.endObject();
        return new BytesRestResponse(status, builder);
    }
}
