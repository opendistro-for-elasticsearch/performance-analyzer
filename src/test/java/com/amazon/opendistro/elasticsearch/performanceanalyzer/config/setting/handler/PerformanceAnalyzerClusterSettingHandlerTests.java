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

import static org.junit.Assert.assertEquals;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.powermock.api.mockito.PowerMockito.when;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PerformanceAnalyzerController;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.ClusterSettingsManager;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class PerformanceAnalyzerClusterSettingHandlerTests {
    private static final Boolean DISABLED_STATE = Boolean.FALSE;
    private static final Boolean ENABLED_STATE = Boolean.TRUE;

    private PerformanceAnalyzerClusterSettingHandler clusterSettingHandler;

    @Mock private PerformanceAnalyzerController mockPerformanceAnalyzerController;
    @Mock private ClusterSettingsManager mockClusterSettingsManager;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void disabledClusterStateTest() {
        setControllerValues(DISABLED_STATE, DISABLED_STATE, DISABLED_STATE, DISABLED_STATE);
        clusterSettingHandler =
            new PerformanceAnalyzerClusterSettingHandler(
                mockPerformanceAnalyzerController, mockClusterSettingsManager);
        assertEquals(0, clusterSettingHandler.getCurrentClusterSettingValue());
    }

    @Test
    public void enabledClusterStateTest() {
        setControllerValues(ENABLED_STATE, ENABLED_STATE, ENABLED_STATE, ENABLED_STATE);
        clusterSettingHandler =
                new PerformanceAnalyzerClusterSettingHandler(
                        mockPerformanceAnalyzerController, mockClusterSettingsManager);
        assertEquals(15, clusterSettingHandler.getCurrentClusterSettingValue());
    }

    @Test
    public void paDisabledClusterStateTest() {
        setControllerValues(DISABLED_STATE, ENABLED_STATE, ENABLED_STATE, ENABLED_STATE);
        clusterSettingHandler =
                new PerformanceAnalyzerClusterSettingHandler(
                        mockPerformanceAnalyzerController, mockClusterSettingsManager);
        assertEquals(0, clusterSettingHandler.getCurrentClusterSettingValue());
    }

    @Test
    public void updateClusterStateTest() {
        setControllerValues(ENABLED_STATE, ENABLED_STATE, DISABLED_STATE, DISABLED_STATE);
        clusterSettingHandler =
                new PerformanceAnalyzerClusterSettingHandler(
                        mockPerformanceAnalyzerController, mockClusterSettingsManager);
        assertEquals(3, clusterSettingHandler.getCurrentClusterSettingValue());
        clusterSettingHandler.onSettingUpdate(0);
        assertEquals(0, clusterSettingHandler.getCurrentClusterSettingValue());
    }

    private void setControllerValues(final Boolean paEnabled, final Boolean rcaEnabled, final Boolean loggingEnabled,
                                     final Boolean batchMetricsEnabled) {
        when(mockPerformanceAnalyzerController.isPerformanceAnalyzerEnabled()).thenReturn(paEnabled);
        when(mockPerformanceAnalyzerController.isRcaEnabled()).thenReturn(rcaEnabled);
        when(mockPerformanceAnalyzerController.isLoggingEnabled()).thenReturn(loggingEnabled);
        when(mockPerformanceAnalyzerController.isBatchMetricsEnabled()).thenReturn(batchMetricsEnabled);
    }
}
