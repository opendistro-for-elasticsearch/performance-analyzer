/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
