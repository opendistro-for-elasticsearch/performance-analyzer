package com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting;

import org.elasticsearch.common.settings.Setting;

public final class PerformanceAnalyzerClusterSettings {
    /**
     * Cluster setting that controls state for various PA components.
     * Bit 0: Perf Analyzer enabled/disabled
     * Bit 1: RCA enabled/disabled
     * Bit 2: Logging enabled/disabled
     */
    public static final Setting<Integer> COMPOSITE_PA_SETTING = Setting.intSetting(
            "cluster.metadata.perf_analyzer.state",
            0,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
            );

    public enum PerformanceAnalyzerFeatureBits {
        PA_BIT,
        RCA_BIT,
        LOGGING_BIT
    }
}
