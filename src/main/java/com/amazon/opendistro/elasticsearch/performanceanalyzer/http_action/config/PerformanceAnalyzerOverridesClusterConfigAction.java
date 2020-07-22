package com.amazon.opendistro.elasticsearch.performanceanalyzer.http_action.config;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides.ConfigOverrides;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides.ConfigOverridesHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides.ConfigOverridesWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.handler.ConfigOverridesClusterSettingHandler;
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

import java.io.IOException;
import java.util.Collections;

/**
 * Rest request handler for handling config overrides for various performance analyzer features.
 */
public class PerformanceAnalyzerOverridesClusterConfigAction extends BaseRestHandler {

    private static final Logger LOG = LogManager.getLogger(PerformanceAnalyzerOverridesClusterConfigAction.class);
    private static final String PA_CONFIG_OVERRIDES_PATH = "/_opendistro/_performanceanalyzer/override/cluster/config";
    private static final String OVERRIDES_FIELD = "overrides";
    private static final String REASON_FIELD = "reason";
    private static final String OVERRIDE_TRIGGERED_FIELD = "override triggered";

    private final ConfigOverridesClusterSettingHandler configOverridesClusterSettingHandler;
    private final ConfigOverridesWrapper overridesWrapper;

    public PerformanceAnalyzerOverridesClusterConfigAction(
            final Settings settings, final RestController restController,
            final ConfigOverridesClusterSettingHandler configOverridesClusterSettingHandler,
            final ConfigOverridesWrapper overridesWrapper) {
        super(settings);
        this.configOverridesClusterSettingHandler = configOverridesClusterSettingHandler;
        this.overridesWrapper = overridesWrapper;
        registerHandlers(restController);
    }

    private void registerHandlers(final RestController restController) {
        restController.registerHandler(RestRequest.Method.GET, PA_CONFIG_OVERRIDES_PATH, this);
        restController.registerHandler(RestRequest.Method.POST, PA_CONFIG_OVERRIDES_PATH, this);
    }

    /**
     * @return the name of this handler.
     */
    @Override
    public String getName() {
        return PerformanceAnalyzerOverridesClusterConfigAction.class.getSimpleName();
    }

    /**
     * Prepare the request for execution. Implementations should consume all request params before
     * returning the runnable for actual execution. Unconsumed params will immediately terminate
     * execution of the request. However, some params are only used in processing the response;
     * implementations can override {@link BaseRestHandler#responseParams()} to indicate such params.
     *
     * @param request the request to execute
     * @param client  client for executing actions on the local node
     * @return the action to execute
     * @throws IOException if an I/O exception occurred parsing the request and preparing for
     *                     execution
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client)
            throws IOException {
        RestChannelConsumer consumer;
        if (request.method() == RestRequest.Method.GET) {
            consumer = handleGet();
        } else if (request.method() == RestRequest.Method.POST) {
            consumer = handlePost(request);
        } else {
            String reason = "Unsupported method:" + request.method().toString() + " Supported: [GET, POST]";
            consumer = sendErrorResponse(reason, RestStatus.METHOD_NOT_ALLOWED);
        }

        return consumer;
    }

    /**
     * Handler for the GET method.
     *
     * @return RestChannelConsumer that sends the current config overrides when run.
     */
    private RestChannelConsumer handleGet() {
        return channel -> {
            try {
                final ConfigOverrides overrides = overridesWrapper.getCurrentClusterConfigOverrides();
                XContentBuilder builder = channel.newBuilder();
                builder.startObject();
                builder.field(OVERRIDES_FIELD, ConfigOverridesHelper.serialize(overrides));
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            } catch (IOException ioe) {
                LOG.error("Error sending response", ioe);
            }
        };
    }

    /**
     * Handler for the POST method.
     *
     * @param request The POST request.
     * @return RestChannelConsumer that updates the cluster setting with the requested config
     * overrides when run.
     * @throws IOException if an exception occurs trying to parse or execute the request.
     */
    private RestChannelConsumer handlePost(final RestRequest request) throws IOException {
        String jsonString = XContentHelper.convertToJson(request.content(), false, XContentType.JSON);
        ConfigOverrides requestedOverrides = ConfigOverridesHelper.deserialize(jsonString);

        if (!validateOverrides(requestedOverrides)) {
            String reason = "enable set and disable set should be disjoint";
            return sendErrorResponse(reason, RestStatus.BAD_REQUEST);
        }

        configOverridesClusterSettingHandler.updateConfigOverrides(requestedOverrides);
        return channel -> {
            XContentBuilder builder = channel.newBuilder();
            builder.startObject();
            builder.field(OVERRIDE_TRIGGERED_FIELD, true);
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        };
    }

    private boolean validateOverrides(final ConfigOverrides requestedOverrides) {
        boolean isValid = true;

        // Check if we have both enable and disable components
        if (requestedOverrides.getDisable() == null || requestedOverrides.getEnable() == null) {
            return true;
        }

        // Check if any RCA nodes are present in both enabled and disabled lists.
        if (requestedOverrides.getEnable().getRcas() != null
                && requestedOverrides.getDisable().getRcas() != null) {
            isValid = Collections.disjoint(requestedOverrides.getEnable().getRcas(), requestedOverrides.getDisable().getRcas());
        }

        // Check if any deciders are present in both enabled and disabled lists.
        if (isValid
                && requestedOverrides.getEnable().getDeciders() != null
                && requestedOverrides.getDisable().getDeciders() != null) {
            isValid = Collections.disjoint(requestedOverrides.getEnable().getDeciders(), requestedOverrides.getDisable().getDeciders());
        }

        // Check if any remediation actions are in both enabled and disabled lists.
        if (isValid
                && requestedOverrides.getEnable().getActions() != null
                && requestedOverrides.getDisable().getActions() != null) {
            isValid = Collections.disjoint(requestedOverrides.getEnable().getActions(), requestedOverrides.getDisable().getActions());
        }

        return isValid;
    }

    private RestChannelConsumer sendErrorResponse(final String reason, final RestStatus status) {
        return channel -> {
            XContentBuilder errorBuilder = channel.newErrorBuilder();
            errorBuilder.startObject();
            errorBuilder.field(REASON_FIELD, reason);
            errorBuilder.endObject();

            channel.sendResponse(new BytesRestResponse(status, errorBuilder));
        };
    }
}
