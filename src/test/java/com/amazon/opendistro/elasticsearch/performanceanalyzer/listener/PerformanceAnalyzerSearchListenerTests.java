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

import static org.mockito.MockitoAnnotations.initMocks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PerformanceAnalyzerController;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.jvm.ThreadList;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import org.apache.commons.lang3.SystemUtils;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

@Ignore
public class PerformanceAnalyzerSearchListenerTests {
  private static final String TEST_INDEX = "test";

  private PerformanceAnalyzerSearchListener performanceAnalyzerSearchListener;
  @Mock private SearchContext searchContext;
  @Mock private ShardSearchRequest shardSearchRequest;
  @Mock private ShardId shardId;

  @Mock
  private PerformanceAnalyzerController controller;


  @Before
  public void setup() {
    // this test only runs in Linux system
    // as some of the static members of the ThreadList class are specific to Linux
    org.junit.Assume.assumeTrue(SystemUtils.IS_OS_LINUX);

    initMocks(this);
    Mockito.when(controller.isPerformanceAnalyzerEnabled()).thenReturn(true);
    Mockito.when(searchContext.request()).thenReturn(shardSearchRequest);
    Mockito.when(shardSearchRequest.shardId()).thenReturn(shardId);
    Mockito.when(shardId.getIndexName()).thenReturn("shardIndex");
    Mockito.when(shardId.getId()).thenReturn(1);

    MetricsConfiguration.CONFIG_MAP.put(ThreadList.class, MetricsConfiguration.cdefault);
    performanceAnalyzerSearchListener = new PerformanceAnalyzerSearchListener(controller);

//    String params[] = new String[0];
//      ThreadList.runThreadDump(OSGlobals.getPid(), params);
//      ThreadList.LOGGER.info(ThreadList.getNativeTidMap().values());
  }

  @Test
  public void testOnPreQueryPhase() {
    performanceAnalyzerSearchListener.onPreQueryPhase(searchContext);
  }

}
