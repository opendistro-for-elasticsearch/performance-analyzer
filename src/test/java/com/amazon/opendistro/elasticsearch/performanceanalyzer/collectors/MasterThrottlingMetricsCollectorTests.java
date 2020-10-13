package com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.CustomMetricsLocationTestBase;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MasterThrottlingMetricsCollectorTests extends CustomMetricsLocationTestBase {

    @Test
    public void testMasterThrottlingMetrics() {
        MetricsConfiguration.CONFIG_MAP.put(MasterThrottlingMetricsCollector.class, MetricsConfiguration.cdefault);
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");

        long startTimeInMills = 1153721339;
        MasterThrottlingMetricsCollector throttlingMetricsCollectorCollector = new MasterThrottlingMetricsCollector();
        throttlingMetricsCollectorCollector.saveMetricValues("testMetric", startTimeInMills);

        List<Event> metrics =  new ArrayList<>();
        PerformanceAnalyzerMetrics.metricQueue.drainTo(metrics);
        assertEquals(1, metrics.size());
        assertEquals("testMetric", metrics.get(0).value);

        try {
            throttlingMetricsCollectorCollector.saveMetricValues("throttled_pending_tasks", startTimeInMills, "123");
            assertTrue("Negative scenario test: Should have been a RuntimeException", true);
        } catch (RuntimeException ex) {
            //- expecting exception...1 values passed; 0 expected
        }
    }
}
