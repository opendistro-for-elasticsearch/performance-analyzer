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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.handler.MutedRcasSettingHandler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.handler.NodeStatsSettingHandler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.handler.PerformanceAnalyzerClusterSettingHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Map;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.http_action.config.PerformanceAnalyzerConfigParams.CLUSTER_CONFIG_PATH;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.http_action.config.PerformanceAnalyzerConfigParams.MUTED_RCAS;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.http_action.config.PerformanceAnalyzerConfigParams.PA_ENABLED;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.http_action.config.PerformanceAnalyzerConfigParams.PA_LOGGING_ENABLED;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.http_action.config.PerformanceAnalyzerConfigParams.RCA_ENABLED;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.http_action.config.PerformanceAnalyzerConfigParams.SHARDS_PER_COLLECTION;

/**
 * POST Rest request handler for handling cluster-wide performance analyzer and RCA config settings, which include :
 * 1. Enable and Disable support for PA, RCA and PA Logging
 * 2. Setting Shards_Per_Collection setting value
 * 3. Setting Muted_RCA_List setting value
 *
 */
public class PerformanceAnalyzerPostClusterConfigAction extends BaseRestHandler {
    private static final Logger LOG = LogManager.getLogger(PerformanceAnalyzerPostClusterConfigAction.class);
    private static final String CURRENT = "currentPerformanceAnalyzerClusterState";

    private final PerformanceAnalyzerClusterSettingHandler clusterSettingHandler;
    private final NodeStatsSettingHandler nodeStatsSettingHandler;
    private final MutedRcasSettingHandler mutedRcasSettingHandler;

    public PerformanceAnalyzerPostClusterConfigAction(final Settings settings, final RestController restController,
                                                      final PerformanceAnalyzerClusterSettingHandler clusterSettingHandler,
                                                      final NodeStatsSettingHandler nodeStatsSettingHandler,
                                                      final MutedRcasSettingHandler mutedRcasSettingHandler) {
        super(settings);
        this.clusterSettingHandler = clusterSettingHandler;
        this.nodeStatsSettingHandler = nodeStatsSettingHandler;
        this.mutedRcasSettingHandler = mutedRcasSettingHandler;
        registerHandlers(restController);
    }

    private void registerHandlers(final RestController controller) {
        controller.registerHandler(RestRequest.Method.POST, CLUSTER_CONFIG_PATH, this);
    }

    /**
     * @return the name of this handler. The name should be human readable and
     * should describe the action that will performed when this API is
     * called.
     */
    @Override
    public String getName() {
        return PerformanceAnalyzerPostClusterConfigAction.class.getSimpleName();
    }

    /**
     * Prepare the request for execution. Implementations should consume all request params before
     * returning the runnable for actual execution. Unconsumed params will immediately terminate
     * execution of the request. However, some params are only used in processing the response;
     * implementations can override {@link BaseRestHandler#responseParams()} to indicate such
     * params.
     *
     * @param request the request to execute
     * @param client  client for executing actions on the local node
     * @return the action to execute
     * @throws IOException if an I/O exception occurred parsing the request and preparing for
     *                     execution
     */
    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        return channel -> {
            // Parse the request body to get the config setting name and value
            final Map<String, Object> paramMap = XContentHelper.convertToMap(request.content(), false, XContentType.JSON).v2();
            try {
                channel.sendResponse(buildRestResponse(channel, paramMap));
            } catch (IOException ioe) {
                LOG.error("Error sending response", ioe);
            }
        };
    }

    private BytesRestResponse buildRestResponse(final RestChannel channel, Map<String, Object> paramMap) throws Exception {
        XContentBuilder builder = channel.newBuilder();
        builder.startObject();
        RestStatus status = RestStatus.OK;

        LOG.info("PerformanceAnalyzer: parameters received as part of Request: {}, " +
                        "current value for ClusterSetting: {}," +
                        "current value for shardsPerCollection: {}" +
                        "current value for mutedRCAs: {}",
                paramMap.toString(), clusterSettingHandler.getCurrentClusterSettingValue(),
                nodeStatsSettingHandler.getNodeStatsSetting(), mutedRcasSettingHandler.getMutedRcasSetting());

        {
            // 1. Performance Analyzer enable/disable setting
            final boolean paEnabledPresent = (paramMap.get(PA_ENABLED) != null);
            if (paEnabledPresent) {
                final boolean paEnabledValue = (boolean) paramMap.get(PA_ENABLED);
                clusterSettingHandler.updatePerformanceAnalyzerSetting(paEnabledValue);
            }


            // 2. RCA enable/disable setting
            final boolean rcaEnabledPresent = (paramMap.get(RCA_ENABLED) != null);
            if (rcaEnabledPresent) {
                final boolean rcaEnabledValue = (boolean) paramMap.get(RCA_ENABLED);
                clusterSettingHandler.updateRcaSetting(rcaEnabledValue);
            }

            // 3. Logging enable/disable setting
            final boolean loggingEnabledPresent = (paramMap.get(PA_LOGGING_ENABLED) != null);
            if (loggingEnabledPresent) {
                final boolean loggingEnabledValue = (boolean) paramMap.get(PA_LOGGING_ENABLED);
                clusterSettingHandler.updateLoggingSetting(loggingEnabledValue);
            }

            // 4. Shards Per Collection node stat setting
            final boolean shardsPerCollectionPresent = (paramMap.get(SHARDS_PER_COLLECTION) != null);
            if (shardsPerCollectionPresent) {
                final int shardsPerCollectionValue = (Integer) paramMap.get(SHARDS_PER_COLLECTION);
                nodeStatsSettingHandler.updateNodeStatsSetting(shardsPerCollectionValue);
            }

            // 5. Muted RCAs setting
            final boolean mutedRcasPresent = (paramMap.get(MUTED_RCAS) != null);
            if (mutedRcasPresent) {
                final String mutedRcasValue = (String) paramMap.get(MUTED_RCAS);
                mutedRcasSettingHandler.updateMutedRcasSetting(mutedRcasValue);
            }
        }
        builder.field(CURRENT, clusterSettingHandler.getCurrentClusterSettingValue());
        builder.field(SHARDS_PER_COLLECTION, nodeStatsSettingHandler.getNodeStatsSetting());
        builder.field(MUTED_RCAS, mutedRcasSettingHandler.getMutedRcasSetting());
        builder.endObject();
        return new BytesRestResponse(status, builder);
    }
}
