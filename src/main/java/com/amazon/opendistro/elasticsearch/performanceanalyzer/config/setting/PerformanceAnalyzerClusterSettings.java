package com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting;

import org.elasticsearch.common.settings.Setting;

public final class PerformanceAnalyzerClusterSettings {
    /**
     * Cluster setting that controls state for various PA components.
     * Bit 0: Perf Analyzer enabled/disabled
     * Bit 1: RCA enabled/disabled
     * Bit 2: Logging enabled/disabled
     * Bit 3: Batch Metrics enabled/disabled
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
        LOGGING_BIT,
        BATCH_METRICS_BIT
    }

    /**
     * node stats collector enabled/disabled
     * less than or equal to 0 : disabled
     * greater than 0 : enabled, and this set the number of shards per collection
     */
    public static final Setting<Integer> PA_NODE_STATS_SETTING = Setting.intSetting(
            "cluster.metadata.perf_analyzer.pa_node_stats_setting",
            1,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
    );

    /**
     * Cluster setting controlling the config overrides to be applied on performance
     * analyzer components.
     */
    public static final Setting<String> CONFIG_OVERRIDES_SETTING = Setting.simpleString(
            "cluster.metadata.perf_analyzer.config.overrides",
            "",
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
    );
}
