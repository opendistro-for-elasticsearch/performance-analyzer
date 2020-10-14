package com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.CustomMetricsLocationTestBase;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import junit.framework.TestCase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FaultDetectionMetricsCollectorTest extends CustomMetricsLocationTestBase {
    @Test
    public void testShardsStateMetrics() {
        MetricsConfiguration.CONFIG_MAP.put(FaultDetectionMetricsCollector.class, MetricsConfiguration.cdefault);
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");
        long startTimeInMills = 1153721339;
        FaultDetectionMetricsCollector faultDetectionMetricsCollector = new FaultDetectionMetricsCollector();
        faultDetectionMetricsCollector.saveMetricValues("fault_detection", startTimeInMills,
                "follower_check", "65432", "start");
        String fetchedValue = PerformanceAnalyzerMetrics.getMetric(PluginSettings.instance().getMetricsLocation()
                + PerformanceAnalyzerMetrics.getTimeInterval(startTimeInMills)+"/fault_detection/");
        PerformanceAnalyzerMetrics.removeMetrics(PluginSettings.instance().getMetricsLocation()
                + PerformanceAnalyzerMetrics.getTimeInterval(startTimeInMills));
        assertEquals("fault_detection", fetchedValue);

        try {
            faultDetectionMetricsCollector.saveMetricValues("shard_state_metrics", startTimeInMills);
            assertTrue("Negative scenario test: Should have been a RuntimeException", true);
        } catch (RuntimeException ex) {
            //- expecting exception...0 values passed; 3 expected
        }

        try {
            faultDetectionMetricsCollector.saveMetricValues("shard_state_metrics", startTimeInMills,
                    "leader_check");
            assertTrue("Negative scenario test: Should have been a RuntimeException", true);
        } catch (RuntimeException ex) {
            //- expecting exception...1 values passed; 3 expected
        }

        try {
            faultDetectionMetricsCollector.saveMetricValues("shard_state_metrics", startTimeInMills,
                    "leader_check", "823765423");
            assertTrue("Negative scenario test: Should have been a RuntimeException", true);
        } catch (RuntimeException ex) {
            //- expecting exception...2 values passed; 0 expected
        }
    }
}

