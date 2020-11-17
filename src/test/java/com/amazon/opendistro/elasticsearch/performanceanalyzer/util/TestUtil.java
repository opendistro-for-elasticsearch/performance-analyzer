package com.amazon.opendistro.elasticsearch.performanceanalyzer.util;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;
import java.util.ArrayList;
import java.util.List;

public class TestUtil {
  public static List<Event> readEvents() {
    List<Event> metrics = new ArrayList<>();
    PerformanceAnalyzerMetrics.metricQueue.drainTo(metrics);
    return metrics;
  }
}
