package com.amazon.opendistro.elasticsearch.performanceanalyzer.http_action.config;

import java.io.IOException;
import java.util.Map;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.handler.NodeStatsSettingHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.handler.PerformanceAnalyzerClusterSettingHandler;

/**
 * Rest request handler for handling cluster-wide enabling and disabling of performance analyzer features.
 */
public class PerformanceAnalyzerClusterConfigAction extends BaseRestHandler {
    private static final Logger LOG = LogManager.getLogger(PerformanceAnalyzerClusterConfigAction.class);
    private static final String PA_CLUSTER_CONFIG_PATH = "/_opendistro/_performanceanalyzer/cluster/config";
    private static final String RCA_CLUSTER_CONFIG_PATH = "/_opendistro/_performanceanalyzer/rca/cluster/config";
    private static final String LOGGING_CLUSTER_CONFIG_PATH = "/_opendistro/_performanceanalyzer/logging/cluster/config";
    private static final String BATCH_METRICS_CLUSTER_CONFIG_PATH = "/_opendistro/_performanceanalyzer/batch/cluster/config";
    private static final String ENABLED = "enabled";
    private static final String SHARDS_PER_COLLECTION = "shardsPerCollection";
    private static final String CURRENT = "currentPerformanceAnalyzerClusterState";
    private static final String NAME = "PerformanceAnalyzerClusterConfigAction";

    private final PerformanceAnalyzerClusterSettingHandler clusterSettingHandler;
    private final NodeStatsSettingHandler nodeStatsSettingHandler;

    public PerformanceAnalyzerClusterConfigAction(final Settings settings, final RestController restController,
                                                  final PerformanceAnalyzerClusterSettingHandler clusterSettingHandler,
                                                  final NodeStatsSettingHandler nodeStatsSettingHandler) {
        super(settings);
        this.clusterSettingHandler = clusterSettingHandler;
        this.nodeStatsSettingHandler = nodeStatsSettingHandler;
        registerHandlers(restController);
    }

    private void registerHandlers(final RestController controller) {
        controller.registerHandler(RestRequest.Method.GET, PA_CLUSTER_CONFIG_PATH, this);
        controller.registerHandler(RestRequest.Method.POST, PA_CLUSTER_CONFIG_PATH, this);
        controller.registerHandler(RestRequest.Method.GET, RCA_CLUSTER_CONFIG_PATH, this);
        controller.registerHandler(RestRequest.Method.POST, RCA_CLUSTER_CONFIG_PATH, this);
        controller.registerHandler(RestRequest.Method.GET, LOGGING_CLUSTER_CONFIG_PATH, this);
        controller.registerHandler(RestRequest.Method.POST, LOGGING_CLUSTER_CONFIG_PATH, this);
        controller.registerHandler(RestRequest.Method.GET, BATCH_METRICS_CLUSTER_CONFIG_PATH, this);
        controller.registerHandler(RestRequest.Method.POST, BATCH_METRICS_CLUSTER_CONFIG_PATH, this);
    }

    /**
     * @return the name of this handler. The name should be human readable and
     * should describe the action that will performed when this API is
     * called.
     */
    @Override
    public String getName() {
        return PerformanceAnalyzerClusterConfigAction.class.getSimpleName();
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
        if (request.method() == RestRequest.Method.POST && request.content().length() > 0) {
            Map<String, Object> map = XContentHelper.convertToMap(request.content(), false, XContentType.JSON).v2();
            Object value = map.get(ENABLED);
            LOG.debug("PerformanceAnalyzer:Value (Object) Received as Part of Request: {} current value: {}", value,
                    clusterSettingHandler.getCurrentClusterSettingValue());

            if (value instanceof Boolean) {
                if (request.path().contains(RCA_CLUSTER_CONFIG_PATH)) {
                    clusterSettingHandler.updateRcaSetting((Boolean) value);
                } else if (request.path().contains(LOGGING_CLUSTER_CONFIG_PATH)) {
                    clusterSettingHandler.updateLoggingSetting((Boolean) value);
                } else if (request.path().contains(BATCH_METRICS_CLUSTER_CONFIG_PATH)) {
                    clusterSettingHandler.updateBatchMetricsSetting((Boolean) value);
                } else {
                    clusterSettingHandler.updatePerformanceAnalyzerSetting((Boolean) value);
                }
            }
            // update node stats setting if exists
            if (map.containsKey(SHARDS_PER_COLLECTION)) {
                Object shardPerCollectionValue = map.get(SHARDS_PER_COLLECTION);
                if (shardPerCollectionValue instanceof Integer) {
                    nodeStatsSettingHandler.updateNodeStatsSetting((Integer)shardPerCollectionValue);
                }
            }
        }

        return channel -> {
            try {
                XContentBuilder builder = channel.newBuilder();
                builder.startObject();
                builder.field(CURRENT, clusterSettingHandler.getCurrentClusterSettingValue());
                builder.field(SHARDS_PER_COLLECTION, nodeStatsSettingHandler.getNodeStatsSetting());
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            } catch (IOException ioe) {
                LOG.error("Error sending response", ioe);
            }
        };
    }
}
