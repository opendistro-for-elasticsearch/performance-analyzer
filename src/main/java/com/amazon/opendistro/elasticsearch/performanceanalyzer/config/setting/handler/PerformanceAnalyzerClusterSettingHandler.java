package com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.handler;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PerformanceAnalyzerController;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.ClusterSettingListener;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.ClusterSettingsManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.PerformanceAnalyzerClusterSettings.PerformanceAnalyzerFeatureBits;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.PerformanceAnalyzerClusterSettings.COMPOSITE_PA_SETTING;

public class PerformanceAnalyzerClusterSettingHandler implements ClusterSettingListener<Integer> {
    private static final int UNSET_CLUSTER_SETTING_VALUE = -1;
    private static final int BIT_ONE = 1;
    private static final int CLUSTER_SETTING_DISABLED_VALUE = 0;
    private static final int ENABLED_VALUE = 1;
    private static final int RCA_ENABLED_BIT_POS = PerformanceAnalyzerFeatureBits.RCA_BIT.ordinal();
    private static final int PA_ENABLED_BIT_POS = PerformanceAnalyzerFeatureBits.PA_BIT.ordinal();
    private static final int MAX_ALLOWED_BIT_POS = Math.min(PerformanceAnalyzerFeatureBits.values().length, Integer.SIZE - 1);

    private final PerformanceAnalyzerController controller;
    private final ClusterSettingsManager clusterSettingsManager;

    private Integer currentClusterSetting = UNSET_CLUSTER_SETTING_VALUE;

    public PerformanceAnalyzerClusterSettingHandler(final PerformanceAnalyzerController controller,
                                                    final ClusterSettingsManager clusterSettingsManager) {
        this.controller = controller;
        this.clusterSettingsManager = clusterSettingsManager;
    }

    /**
     * Updates the Performance Analyzer setting across the cluster.
     *
     * @param state The desired state for performance analyzer.
     */
    public void updatePerformanceAnalyzerSetting(final boolean state) {
        final Integer settingIntValue = getPASettingValueFromState(state);
        clusterSettingsManager.updateSetting(COMPOSITE_PA_SETTING, settingIntValue);
    }

    /**
     * Updates the RCA setting across the cluster.
     *
     * @param state The desired state for RCA.
     */
    public void updateRcaSetting(final boolean state) {
        final Integer settingIntValue = getRcaSettingValueFromState(state);
        clusterSettingsManager.updateSetting(COMPOSITE_PA_SETTING, settingIntValue);
    }

    /**
     * Handler that gets called when there is a new value for the setting that this listener
     * is listening to.
     *
     * @param newSettingValue The value of the new setting.
     */
    @Override
    public void onSettingUpdate(final Integer newSettingValue) {
        currentClusterSetting = newSettingValue;
        if (newSettingValue != null) {
            controller.updatePerformanceAnalyzerState(getPAStateFromSetting(newSettingValue));
            controller.updateRcaState(getRcaStateFromSetting(newSettingValue));
        }
    }

    /**
     * Gets the current(last seen) cluster setting value.
     *
     * @return the current cluster setting value if exists. -1 otherwise.
     */
    public int getCurrentClusterSettingValue() {
        return currentClusterSetting;
    }

    /**
     * Extracts the boolean value for performance analyzer from the cluster setting.
     *
     * @param settingValue The composite setting value.
     * @return true if the PA_ENABLED bit is set, false otherwise.
     */
    private boolean getPAStateFromSetting(final int settingValue) {
        return ((settingValue >> PA_ENABLED_BIT_POS) & BIT_ONE) == ENABLED_VALUE;
    }

    /**
     * Converts the boolean PA state to composite cluster setting.
     * If Performance Analyzer is being turned off, it will also turn RCA off.
     *
     * @param state the state of performance analyzer. Will enable performance analyzer if true,
     *              disables both RCA and performance analyzer if false.
     * @return composite cluster setting as an integer.
     */
    private Integer getPASettingValueFromState(final boolean state) {
        int clusterSetting = currentClusterSetting != UNSET_CLUSTER_SETTING_VALUE ? currentClusterSetting : CLUSTER_SETTING_DISABLED_VALUE;

        if (state) {
            return setBit(clusterSetting, PA_ENABLED_BIT_POS);
        } else {
            return resetBit(resetBit(clusterSetting, PA_ENABLED_BIT_POS), RCA_ENABLED_BIT_POS);
        }
    }

    /**
     * Extracts the boolean value for RCA state from the cluster setting.
     *
     * @param settingValue The composite setting value.
     * @return true if the RCA_ENABLED bit is set, false otherwise.
     */
    private boolean getRcaStateFromSetting(final int settingValue) {
        return ((settingValue >> RCA_ENABLED_BIT_POS) & BIT_ONE) == ENABLED_VALUE;
    }

    /**
     * Converts the boolean RCA state to composite cluster setting.
     * Enables RCA only if performance analyzer is also set. Otherwise, results in a no-op.
     *
     * @param shouldEnable the state of rca. Will try to enable if true, disables RCA if false.
     * @return composite cluster setting as an integer.
     */
    private Integer getRcaSettingValueFromState(final boolean shouldEnable) {
        int clusterSetting = currentClusterSetting != UNSET_CLUSTER_SETTING_VALUE ? currentClusterSetting : CLUSTER_SETTING_DISABLED_VALUE;

        if (shouldEnable) {
            return controller.isPerformanceAnalyzerEnabled() ? setBit(clusterSetting, RCA_ENABLED_BIT_POS) : clusterSetting;
        } else {
            return resetBit(clusterSetting, RCA_ENABLED_BIT_POS);
        }
    }

    /**
     * Sets the bit at the specified position.
     *
     * @param number      The number in which a needs to be set.
     * @param bitPosition The position of the bit in the number
     * @return number with the bit set at the specified position.
     */
    private int setBit(int number, int bitPosition) {
        return bitPosition < MAX_ALLOWED_BIT_POS ? (number | (1 << bitPosition)) : number;
    }

    /**
     * Resets the bit at the specified position.
     *
     * @param number      The number in which a needs to be reset.
     * @param bitPosition The position of the bit in the number
     * @return number with the bit reset at the specified position.
     */
    private int resetBit(int number, int bitPosition) {
        return bitPosition < MAX_ALLOWED_BIT_POS ? (number & ~(1 << bitPosition)) : number;
    }
}
