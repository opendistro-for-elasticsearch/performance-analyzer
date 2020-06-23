package com.amazon.opendistro.elasticsearch.performanceanalyzer.config;

import static org.junit.Assert.assertEquals;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.powermock.api.mockito.PowerMockito.when;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.ClusterSettingsManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.handler.PerformanceAnalyzerClusterSettingHandler;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class PerformanceAnalyzerClusterSettingHandlerTest {
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
        setControllerValues(DISABLED_STATE, DISABLED_STATE, DISABLED_STATE);
        clusterSettingHandler =
            new PerformanceAnalyzerClusterSettingHandler(
                mockPerformanceAnalyzerController, mockClusterSettingsManager);
        assertEquals(0, clusterSettingHandler.getCurrentClusterSettingValue());
    }

    @Test
    public void enabledClusterStateTest() {
        setControllerValues(ENABLED_STATE, ENABLED_STATE, ENABLED_STATE);
        clusterSettingHandler =
                new PerformanceAnalyzerClusterSettingHandler(
                        mockPerformanceAnalyzerController, mockClusterSettingsManager);
        assertEquals(7, clusterSettingHandler.getCurrentClusterSettingValue());
    }

    @Test
    public void paDisabledClusterStateTest() {
        setControllerValues(DISABLED_STATE, ENABLED_STATE, ENABLED_STATE);
        clusterSettingHandler =
                new PerformanceAnalyzerClusterSettingHandler(
                        mockPerformanceAnalyzerController, mockClusterSettingsManager);
        assertEquals(0, clusterSettingHandler.getCurrentClusterSettingValue());
    }

    private void setControllerValues(final Boolean paEnabled, final Boolean rcaEnabled, final Boolean loggingEnabled) {
        when(mockPerformanceAnalyzerController.isPerformanceAnalyzerEnabled()).thenReturn(paEnabled);
        when(mockPerformanceAnalyzerController.isRcaEnabled()).thenReturn(rcaEnabled);
        when(mockPerformanceAnalyzerController.isLoggingEnabled()).thenReturn(loggingEnabled);
    }
}
