/*
 * Copyright <2020> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.listener;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.MockitoAnnotations.initMocks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatExceptionCode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PerformanceAnalyzerController;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.jvm.ThreadList;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.TestUtil;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.SystemUtils;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

public class PerformanceAnalyzerSearchListenerTests {
  private static final long TOOK_IN_NANOS = 10;
  private static final String EXCEPTION = StatExceptionCode.ES_REQUEST_INTERCEPTOR_ERROR.toString();

  private PerformanceAnalyzerSearchListener searchListener;
  private StatsCollector statsCollector;
  private long startTimeInMills = 1253721339;
  private final AtomicInteger errorCount = new AtomicInteger(0);

  @Mock private SearchContext searchContext;
  @Mock private ShardSearchRequest shardSearchRequest;
  @Mock private ShardId shardId;
  @Mock private PerformanceAnalyzerController controller;

  @BeforeClass
  public static void setup() {
    // this test only runs in Linux system
    // as some of the static members of the ThreadList class are specific to Linux
    org.junit.Assume.assumeTrue(SystemUtils.IS_OS_LINUX);
  }

  @Before
  public void init() {
    initMocks(this);
    Mockito.when(controller.isPerformanceAnalyzerEnabled()).thenReturn(true);

    MetricsConfiguration.CONFIG_MAP.put(ThreadList.class, MetricsConfiguration.cdefault);
    searchListener = new PerformanceAnalyzerSearchListener(controller);
    assertEquals(PerformanceAnalyzerSearchListener.class.getSimpleName(), searchListener.toString());

    statsCollector = StatsCollector.instance();

    //clean metricQueue before running every test
    TestUtil.readEvents();
  }

  @Test
  public void testGetMetricsPath() {
    String expectedPath = PluginSettings.instance().getMetricsLocation()
            + PerformanceAnalyzerMetrics.getTimeInterval(startTimeInMills) + "/"
            + PerformanceAnalyzerMetrics.sThreadsPath + "/" + "SearchThread" + "/"
            + "ShardQuery" +"/"
            + "ShardSearchID" + "/"
            + PerformanceAnalyzerMetrics.FINISH_FILE_NAME;
    String actualPath = searchListener.getMetricsPath(startTimeInMills, "SearchThread", "ShardQuery", "ShardSearchID", PerformanceAnalyzerMetrics.FINISH_FILE_NAME);
    assertEquals(expectedPath, actualPath);

    try {
      searchListener.getMetricsPath(startTimeInMills, "SearchThread", "ShardQuery", "ShardSearchID");
      fail("Negative scenario test: Should have been a RuntimeException");
    } catch (RuntimeException ex) {
      //- expecting exception...3 values passed; 4 expected
    }
  }

  @Test
  public void testOnPreQueryPhase() {
    initializeValidSearchContext(true);
    searchListener.onPreQueryPhase(searchContext);
    List<String> jsonStrs = TestUtil.readMetricsInJsonString(4);
    assertTrue(jsonStrs.get(0).contains(AllMetrics.CommonMetric.START_TIME.toString()));
    assertTrue(jsonStrs.get(1).contains(AllMetrics.CommonDimension.INDEX_NAME.toString()));
    assertTrue(jsonStrs.get(2).contains(AllMetrics.CommonDimension.SHARD_ID.toString()));
  }

  @Test
  public void testOnQueryPhase() {
    initializeValidSearchContext(true);
    searchListener.onQueryPhase(searchContext, TOOK_IN_NANOS);
    List<String> jsonStrs = TestUtil.readMetricsInJsonString(5);
    assertTrue(jsonStrs.get(0).contains(AllMetrics.CommonMetric.FINISH_TIME.toString()));
    assertTrue(jsonStrs.get(1).contains(AllMetrics.CommonDimension.FAILED.toString()));
    assertTrue(jsonStrs.get(1).contains("false"));
    assertTrue(jsonStrs.get(2).contains(AllMetrics.CommonDimension.INDEX_NAME.toString()));
    assertTrue(jsonStrs.get(3).contains(AllMetrics.CommonDimension.SHARD_ID.toString()));
  }


  @Test
  public void testOnFailedQueryPhase() {
    initializeValidSearchContext(true);
    searchListener.onFailedQueryPhase(searchContext);
    List<String> jsonStrs = TestUtil.readMetricsInJsonString(5);
    assertTrue(jsonStrs.get(0).contains(AllMetrics.CommonMetric.FINISH_TIME.toString()));
    assertTrue(jsonStrs.get(1).contains(AllMetrics.CommonDimension.FAILED.toString()));
    assertTrue(jsonStrs.get(1).contains("true"));
    assertTrue(jsonStrs.get(2).contains(AllMetrics.CommonDimension.INDEX_NAME.toString()));
    assertTrue(jsonStrs.get(3).contains(AllMetrics.CommonDimension.SHARD_ID.toString()));
  }

  @Test
  public void testOnPreFetchPhase() {
    initializeValidSearchContext(true);
    searchListener.onPreFetchPhase(searchContext);
    List<String> jsonStrs = TestUtil.readMetricsInJsonString(4);
    assertTrue(jsonStrs.get(0).contains(AllMetrics.CommonMetric.START_TIME.toString()));
    assertTrue(jsonStrs.get(1).contains(AllMetrics.CommonDimension.INDEX_NAME.toString()));
    assertTrue(jsonStrs.get(2).contains(AllMetrics.CommonDimension.SHARD_ID.toString()));
  }

  @Test
  public void testOnFetchPhase() {
    initializeValidSearchContext(true);
    searchListener.onFetchPhase(searchContext, TOOK_IN_NANOS);
    List<String> jsonStrs = TestUtil.readMetricsInJsonString(5);
    assertTrue(jsonStrs.get(0).contains(AllMetrics.CommonMetric.FINISH_TIME.toString()));
    assertTrue(jsonStrs.get(1).contains(AllMetrics.CommonDimension.FAILED.toString()));
    assertTrue(jsonStrs.get(1).contains("false"));
    assertTrue(jsonStrs.get(2).contains(AllMetrics.CommonDimension.INDEX_NAME.toString()));
    assertTrue(jsonStrs.get(3).contains(AllMetrics.CommonDimension.SHARD_ID.toString()));
  }

  @Test
  public void testOnFailedFetchPhase() {
    initializeValidSearchContext(true);
    searchListener.onFailedFetchPhase(searchContext);
    List<String> jsonStrs = TestUtil.readMetricsInJsonString(5);
    assertTrue(jsonStrs.get(0).contains(AllMetrics.CommonMetric.FINISH_TIME.toString()));
    assertTrue(jsonStrs.get(1).contains(AllMetrics.CommonDimension.FAILED.toString()));
    assertTrue(jsonStrs.get(1).contains("true"));
    assertTrue(jsonStrs.get(2).contains(AllMetrics.CommonDimension.INDEX_NAME.toString()));
    assertTrue(jsonStrs.get(3).contains(AllMetrics.CommonDimension.SHARD_ID.toString()));
  }

  @Test
  public void testInvalidSearchContext() {
    initializeValidSearchContext(false);

    searchListener.onFailedFetchPhase(searchContext);
    assertEquals(errorCount.incrementAndGet(), statsCollector.getCounters().get(EXCEPTION).intValue());
    searchListener.onPreFetchPhase(searchContext);
    assertEquals(errorCount.incrementAndGet(), statsCollector.getCounters().get(EXCEPTION).intValue());
    searchListener.onFetchPhase(searchContext, TOOK_IN_NANOS);
    assertEquals(errorCount.incrementAndGet(), statsCollector.getCounters().get(EXCEPTION).intValue());
    searchListener.onPreQueryPhase(searchContext);
    assertEquals(errorCount.incrementAndGet(), statsCollector.getCounters().get(EXCEPTION).intValue());
    searchListener.onFailedQueryPhase(searchContext);
    assertEquals(errorCount.incrementAndGet(), statsCollector.getCounters().get(EXCEPTION).intValue());
    searchListener.onQueryPhase(searchContext, TOOK_IN_NANOS);
    assertEquals(errorCount.incrementAndGet(), statsCollector.getCounters().get(EXCEPTION).intValue());
  }

  private void initializeValidSearchContext(boolean isValid) {
    if (isValid) {
      Mockito.when(searchContext.request()).thenReturn(shardSearchRequest);
      Mockito.when(shardSearchRequest.shardId()).thenReturn(shardId);
      Mockito.when(shardId.getIndexName()).thenReturn("shardIndex");
      Mockito.when(shardId.getId()).thenReturn(1);
    } else {
      Mockito.when(searchContext.request()).thenReturn(null);
    }
  }
}
