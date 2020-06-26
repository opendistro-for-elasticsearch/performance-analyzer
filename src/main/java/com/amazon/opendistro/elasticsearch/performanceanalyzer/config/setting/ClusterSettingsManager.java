package com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatExceptionCode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ESResources;

/**
 * Class that handles updating cluster settings,
 * and notifying the listeners when cluster settings change.
 */
public class ClusterSettingsManager implements ClusterStateListener {
    private static final Logger LOG = LogManager.getLogger(ClusterSettingsManager.class);
    private Map<Setting<Integer>, List<ClusterSettingListener<Integer>>> listenerMap = new HashMap<>();
    private final List<Setting<Integer>> managedSettings = new ArrayList<>();
    private final ClusterSettingsResponseHandler clusterSettingsResponseHandler;

    private boolean initialized = false;

    public ClusterSettingsManager(List<Setting<Integer>> initialSettings) {
        managedSettings.addAll(initialSettings);
        this.clusterSettingsResponseHandler = new ClusterSettingsResponseHandler();
    }

    /**
     * Adds a listener that will be called when the requested setting's value changes.
     *
     * @param setting  The setting that needs to be listened to.
     * @param listener The listener object that will be called when the setting changes.
     */
    public void addSubscriberForSetting(Setting<Integer> setting, ClusterSettingListener<Integer> listener) {
        if (listenerMap.containsKey(setting)) {
            final List<ClusterSettingListener<Integer>> currentListeners = listenerMap.get(setting);
            if (!currentListeners.contains(listener)) {
                currentListeners.add(listener);
                listenerMap.put(setting, currentListeners);
            }
        } else {
            listenerMap.put(setting, Collections.singletonList(listener));
        }
    }

    /**
     * Bootstraps the listeners and tries to read initial values for cluster settings.
     */
    public void initialize() {
        if (!initialized) {
            // When an ES node is just started, the plugin initialization happens
            // before there is any cluster state set. So, check if there is a cluster
            // state first, if there is no cluster state, register a ClusterStateListener
            // before trying to read the initial cluster setting values.
            if (clusterStatePresent()) {
                registerSettingUpdateListener();
                readAllManagedClusterSettings();
            } else {
                registerClusterStateListener(this);
            }

            initialized = true;
        }
    }

    /**
     * Updates the requested setting with the requested value across the cluster.
     *
     * @param setting  The setting that needs to be updated.
     * @param newValue The new value for the setting.
     */
    public void updateSetting(final Setting<Integer> setting, final Integer newValue) {
        final ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
        request.persistentSettings(Settings.builder()
                                           .put(setting.getKey(), newValue)
                                           .build());
        ESResources.INSTANCE.getClient().admin().cluster().updateSettings(request);
    }

    /**
     * Registers a setting update listener for all the settings managed by this instance.
     */
    private void registerSettingUpdateListener() {
        for (Setting<Integer> setting : managedSettings) {
            ESResources.INSTANCE.getClusterService()
                                .getClusterSettings()
                                .addSettingsUpdateConsumer(setting, updatedVal -> callListeners(setting, updatedVal));
        }
    }

    /**
     * Checks if there is a cluster state set.
     *
     * @return true if a cluster state is set, false otherwise.
     */
    private boolean clusterStatePresent() {
        try {
            final ClusterState clusterState = ESResources.INSTANCE.getClusterService().state();
            return clusterState != null;
        } catch (Exception | AssertionError t) {
            LOG.error("Unable to retrieve cluster state: Exception: {}", t.getMessage());
            LOG.error(t);
        }

        return false;
    }

    /**
     * Reads all the cluster settings managed by this instance.
     */
    private void readAllManagedClusterSettings() {
        LOG.debug("Trying to read initial cluster settings");
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.routingTable(false)
                           .nodes(false);
        ESResources.INSTANCE.getClient()
                            .admin()
                            .cluster()
                            .state(clusterStateRequest, clusterSettingsResponseHandler);
    }

    /**
     * Registers a ClusterStateListener with the cluster service. The listener is called
     * when the cluster state changes.
     *
     * @param clusterStateListener The listener to be registered.
     */
    private void registerClusterStateListener(final ClusterStateListener clusterStateListener) {
        ESResources.INSTANCE.getClusterService().addListener(clusterStateListener);
    }

    /**
     * Called when cluster state changes.
     *
     * @param event The cluster change event.
     */
    @Override
    public void clusterChanged(final ClusterChangedEvent event) {
        // Check if cluster state is set, if set, remove the listener and try to read cluster settings.
        final ClusterState state = event.state();

        if (state != null) {
            ESResources.INSTANCE.getClusterService().removeListener(this);
            registerSettingUpdateListener();
            readAllManagedClusterSettings();
        }
    }

    /**
     * Calls all the listeners for the specified setting with the requested value.
     *
     * @param setting      The setting whose listeners need to be notified.
     * @param settingValue The new value for the setting.
     */
    private void callListeners(final Setting<Integer> setting, int settingValue) {
        try {
            final List<ClusterSettingListener<Integer>> listeners = listenerMap.get(setting);
            if (listeners != null) {
                for (ClusterSettingListener<Integer> listener : listeners) {
                    listener.onSettingUpdate(settingValue);
                }
            }
        } catch(Exception ex) {
            LOG.error(ex);
            StatsCollector.instance().logException(StatExceptionCode.ES_REQUEST_INTERCEPTOR_ERROR);
        }
    }

    /**
     * Class that handles response to GET /_cluster/settings
     */
    private class ClusterSettingsResponseHandler implements ActionListener<ClusterStateResponse> {
        /**
         * Handle action response. This response may constitute a failure or a
         * success but it is up to the listener to make that decision.
         *
         * @param clusterStateResponse
         */
        @Override
        public void onResponse(final ClusterStateResponse clusterStateResponse) {
            final Settings clusterSettings = clusterStateResponse.getState()
                                                                 .getMetadata()
                                                                 .persistentSettings();

            for (final Setting<Integer> setting : managedSettings) {
                Integer settingValue = clusterSettings.getAsInt(setting.getKey(), null);
                if (settingValue != null) {
                    callListeners(setting, settingValue);
                }
            }
        }

        /**
         * A failure caused by an exception at some phase of the task.
         *
         * @param e
         */
        @Override
        public void onFailure(final Exception e) {
            LOG.error("Unable to read cluster settings. Exception: {}", e.getMessage());
            LOG.error(e);
        }
    }
}
