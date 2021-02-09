/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PerformanceAnalyzerController;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.ClusterSettingListener;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.ClusterSettingsManager;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.PerformanceAnalyzerClusterSettings.PA_NODE_STATS_SETTING;

public class NodeStatsSettingHandler implements ClusterSettingListener<Integer> {
    private final PerformanceAnalyzerController controller;
    private final ClusterSettingsManager clusterSettingsManager;

    private Integer currentClusterSetting = PerformanceAnalyzerController.DEFAULT_NUM_OF_SHARDS_PER_COLLECTION;

    public NodeStatsSettingHandler(final PerformanceAnalyzerController controller,
                                   final ClusterSettingsManager clusterSettingsManager) {
        this.controller = controller;
        this.clusterSettingsManager = clusterSettingsManager;
    }


    /**
     * Updates the PA Node Stats setting across the cluster.
     *
     * @param value The desired num of shards value for node stats.
     */
    public void updateNodeStatsSetting(final int value) {
        clusterSettingsManager.updateSetting(PA_NODE_STATS_SETTING, value);
    }

    /**
     * Handler that gets called when there is a new value for the setting that this listener
     * is listening to.
     *
     * @param newSettingValue The value of the new setting.
     */
    @Override
    public void onSettingUpdate(final Integer newSettingValue) {
        if (newSettingValue != null) {
            currentClusterSetting = newSettingValue;
            controller.updateNodeStatsShardsPerCollection(newSettingValue);
        }
    }

    /**
     * Gets the current(last seen) cluster setting value.
     * @return integer value for setting.
     */
    public int getNodeStatsSetting() {
        return currentClusterSetting;
    }
}
