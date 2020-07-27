/*
 * Copyright <2020> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.handler;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides.ConfigOverrides;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides.ConfigOverridesHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides.ConfigOverridesWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.ClusterSettingListener;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.ClusterSettingsManager;
import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Setting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class ConfigOverridesClusterSettingHandler implements ClusterSettingListener<String> {

    private static final Logger LOG = LogManager.getLogger(ConfigOverridesClusterSettingHandler.class);

    private final ClusterSettingsManager clusterSettingsManager;
    private final ConfigOverridesWrapper overridesHolder;
    private final Setting<String> setting;

    public ConfigOverridesClusterSettingHandler(final ConfigOverridesWrapper overridesHolder,
                                                final ClusterSettingsManager clusterSettingsManager,
                                                final Setting<String> setting) {
        this.clusterSettingsManager = clusterSettingsManager;
        this.overridesHolder = overridesHolder;
        this.setting = setting;
    }

    /**
     * Handler that gets called when there is a new value for the setting that this listener is
     * listening to.
     *
     * @param newSettingValue The value of the new setting.
     */
    @Override
    public void onSettingUpdate(String newSettingValue) {
        try {
            final ConfigOverrides newOverrides = ConfigOverridesHelper.deserialize(newSettingValue);
            overridesHolder.setCurrentClusterConfigOverrides(newOverrides);
            overridesHolder.setLastUpdatedTimestamp(System.currentTimeMillis());
        } catch (IOException e) {
            LOG.error("Unable to apply received cluster setting update: " + newSettingValue, e);
        }
    }

    /**
     * Updates the cluster setting with the new set of config overrides.
     *
     * @param newOverrides The new set of overrides that need to be applied.
     * @throws IOException if unable to serialize the setting.
     */
    public void updateConfigOverrides(final ConfigOverrides newOverrides) throws IOException {
        String newClusterSettingValue = buildClusterSettingValue(newOverrides);
        // TODO: @ktkrg - Change to debug
        LOG.error("Updating cluster setting with new overrides string: {}", newClusterSettingValue);
        clusterSettingsManager.updateSetting(setting, newClusterSettingValue);
    }

    /**
     * Generates a string representation of overrides.
     *
     * @param newOverrides The new overrides that need to be merged with the existing
     *                     overrides.
     * @return String value of the merged config overrides.
     */
    private String buildClusterSettingValue(final ConfigOverrides newOverrides) throws IOException {
        final ConfigOverrides mergedConfigOverrides = merge(overridesHolder.getCurrentClusterConfigOverrides(), newOverrides);

        return ConfigOverridesHelper.serialize(mergedConfigOverrides);
    }

    /**
     * Merges the current set of overrides with the new set and returns a new instance
     * of the merged config overrides.
     *
     * @param other the other ConfigOverrides to merge from.
     * @return A new instance of the ConfigOverrides representing the merged config
     * override.
     */
    private ConfigOverrides merge(final ConfigOverrides current, final ConfigOverrides other) {
        final ConfigOverrides merged = new ConfigOverrides();
        ConfigOverrides.Overrides optionalCurrentEnabled = Optional.ofNullable(current.getEnable())
                .orElseGet(ConfigOverrides.Overrides::new);
        ConfigOverrides.Overrides optionalCurrentDisabled = Optional.ofNullable(current.getDisable())
                .orElseGet(ConfigOverrides.Overrides::new);
        ConfigOverrides.Overrides optionalNewEnable = Optional.ofNullable(other.getEnable())
                .orElseGet(ConfigOverrides.Overrides::new);
        ConfigOverrides.Overrides optionalNewDisable = Optional.ofNullable(other.getDisable())
                .orElseGet(ConfigOverrides.Overrides::new);

        mergeRcas(merged, optionalCurrentEnabled, optionalNewEnable, optionalCurrentDisabled, optionalNewDisable);
        mergeDeciders(merged, optionalCurrentEnabled, optionalNewEnable, optionalCurrentDisabled, optionalNewDisable);
        mergeActions(merged, optionalCurrentEnabled, optionalNewEnable, optionalCurrentDisabled, optionalNewDisable);

        return merged;
    }

    private void mergeRcas(final ConfigOverrides merged,
                           final ConfigOverrides.Overrides baseEnabled,
                           final ConfigOverrides.Overrides newEnabled,
                           final ConfigOverrides.Overrides baseDisabled,
                           final ConfigOverrides.Overrides newDisabled) {
        List<String> currentRcaEnabled = Optional.ofNullable(baseEnabled.getRcas())
                .orElseGet(ArrayList::new);
        List<String> currentRcaDisabled = Optional.ofNullable(baseDisabled.getRcas())
                .orElseGet(ArrayList::new);
        List<String> requestedRcasEnabled = Optional.ofNullable(newEnabled.getRcas())
                .orElseGet(ArrayList::new);
        List<String> requestedRcasDisabled = Optional.ofNullable(newDisabled.getRcas())
                .orElseGet(ArrayList::new);

        List<String> mergedRcasEnabled = combineLists(currentRcaEnabled, requestedRcasEnabled, requestedRcasDisabled);
        List<String> mergedRcasDisabled = combineLists(currentRcaDisabled, requestedRcasDisabled, requestedRcasEnabled);

        merged.getEnable().setRcas(mergedRcasEnabled);
        merged.getDisable().setRcas(mergedRcasDisabled);
    }

    private void mergeDeciders(final ConfigOverrides merged,
                               final ConfigOverrides.Overrides baseEnabled,
                               final ConfigOverrides.Overrides newEnabled,
                               final ConfigOverrides.Overrides baseDisabled,
                               final ConfigOverrides.Overrides newDisabled) {
        List<String> currentDecidersEnabled = Optional.ofNullable(baseEnabled.getDeciders())
                .orElseGet(ArrayList::new);
        List<String> currentDecidersDisabled = Optional.ofNullable(baseDisabled.getDeciders())
                .orElseGet(ArrayList::new);
        List<String> requestedDecidersEnabled = Optional.ofNullable(newEnabled.getDeciders())
                .orElseGet(ArrayList::new);
        List<String> requestedDecidersDisabled = Optional.ofNullable(newDisabled.getDeciders())
                .orElseGet(ArrayList::new);

        List<String> mergedDecidersEnabled = combineLists(currentDecidersEnabled, requestedDecidersEnabled, requestedDecidersDisabled);
        List<String> mergedDecidersDisabled = combineLists(currentDecidersDisabled, requestedDecidersDisabled, requestedDecidersEnabled);

        merged.getEnable().setDeciders(mergedDecidersEnabled);
        merged.getDisable().setDeciders(mergedDecidersDisabled);
    }

    private void mergeActions(final ConfigOverrides merged,
                              final ConfigOverrides.Overrides baseEnabled,
                              final ConfigOverrides.Overrides newEnabled,
                              final ConfigOverrides.Overrides baseDisabled,
                              final ConfigOverrides.Overrides newDisabled) {
        List<String> currentActionsEnabled = Optional.ofNullable(baseEnabled.getActions())
                .orElseGet(ArrayList::new);
        List<String> currentActionsDisabled = Optional.ofNullable(baseDisabled.getActions())
                .orElseGet(ArrayList::new);
        List<String> requestedActionsEnabled = Optional.ofNullable(newEnabled.getActions())
                .orElseGet(ArrayList::new);
        List<String> requestedActionsDisabled = Optional.ofNullable(newDisabled.getActions())
                .orElseGet(ArrayList::new);

        List<String> mergedActionsEnabled = combineLists(currentActionsEnabled, requestedActionsEnabled, requestedActionsDisabled);
        List<String> mergedActionsDisabled = combineLists(currentActionsDisabled, requestedActionsDisabled, requestedActionsEnabled);

        merged.getEnable().setActions(mergedActionsEnabled);
        merged.getDisable().setActions(mergedActionsDisabled);
    }

    /**
     * Combines three lists by adding all elements in the addList to the base list and
     * removing all elements in the remove list from the combined list.
     * // TODO: Add example here to clarify
     *
     * @param baseList   The base list.
     * @param addList    The list whose contents need to added to the base list.
     * @param removeList The list whose contents should be removed from the base list if present.
     * @return The combined list as an immutable list.
     */
    private List<String> combineLists(List<String> baseList, List<String> addList, List<String> removeList) {
        Set<String> combinedEnabled = new HashSet<>(baseList);
        combinedEnabled.addAll(addList);
        combinedEnabled.removeAll(removeList);

        return ImmutableList.copyOf(combinedEnabled);
    }

}
