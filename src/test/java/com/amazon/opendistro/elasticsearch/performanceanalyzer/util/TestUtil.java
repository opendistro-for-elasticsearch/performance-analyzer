package com.amazon.opendistro.elasticsearch.performanceanalyzer.util;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestUtil {
  public static List<Event> readEvents() {
    List<Event> metrics = new ArrayList<>();
    PerformanceAnalyzerMetrics.metricQueue.drainTo(metrics);
    return metrics;
  }

  public static List<String> readMetricsInJsonString(int length) {
    List<Event> metrics = readEvents();
    assert metrics.size() == 1;
    String[] jsonStrs = metrics.get(0).value.split("\n");
    assert jsonStrs.length == length;
    return Arrays.asList(jsonStrs).subList(1, jsonStrs.length);
  }
}
