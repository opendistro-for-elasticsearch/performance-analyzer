package com.amazon.opendistro.elasticsearch.performanceanalyzer.util;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.CacheConfigMetricsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.CircuitBreakerCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.MasterServiceEventMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.MasterServiceMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.NodeDetailsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.NodeStatsAllShardsMetricsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.NodeStatsFewShardsMetricsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.ThreadPoolMetricsCollector;

public class Utils {

    public static void configureMetrics() {
        MetricsConfiguration.MetricConfig cdefault = MetricsConfiguration.cdefault ;
        MetricsConfiguration.CONFIG_MAP.put(CacheConfigMetricsCollector.class, cdefault);
        MetricsConfiguration.CONFIG_MAP.put(CircuitBreakerCollector.class, cdefault);
        MetricsConfiguration.CONFIG_MAP.put(ThreadPoolMetricsCollector.class, cdefault);
        MetricsConfiguration.CONFIG_MAP.put(NodeDetailsCollector.class, cdefault);
        MetricsConfiguration.CONFIG_MAP.put(NodeStatsAllShardsMetricsCollector.class, cdefault);
        MetricsConfiguration.CONFIG_MAP.put(NodeStatsFewShardsMetricsCollector.class, cdefault);
        MetricsConfiguration.CONFIG_MAP.put(MasterServiceEventMetrics.class, new MetricsConfiguration.MetricConfig(1000, 0, 0));
        MetricsConfiguration.CONFIG_MAP.put(MasterServiceMetrics.class, cdefault);
        
    }

}
