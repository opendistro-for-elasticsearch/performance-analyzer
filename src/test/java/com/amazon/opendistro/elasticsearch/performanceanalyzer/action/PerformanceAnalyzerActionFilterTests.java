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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.action;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PerformanceAnalyzerController;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.tasks.Task;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

public class PerformanceAnalyzerActionFilterTests {
  private static final String[] testIndices = new String[]{"testIndex"};
  private PerformanceAnalyzerActionFilter filter;

  @Mock private PerformanceAnalyzerController controller;
  @Mock private SearchRequest searchRequest;
  @Mock private BulkRequest bulkRequest;
  @Mock private ActionRequest request;
  @Mock private ActionListener<ActionResponse> listener;
  @Mock private ActionFilterChain<ActionRequest, ActionResponse> chain;
  @Mock private Task task;

  @Before
  public void init() {
    initMocks(this);

    Mockito.when(controller.isPerformanceAnalyzerEnabled()).thenReturn(true);
    filter = new PerformanceAnalyzerActionFilter((controller));
  }

  @Test
  public void testApplyWithSearchRequest() {
    Mockito.when(searchRequest.indices()).thenReturn(testIndices);
    testApply(searchRequest);
  }

  @Test
  public void testApplyWithBulkRequest() {
    testApply(bulkRequest);
  }

  @Test
  public void testApplyWithOtherRequest() {
    testApply(request);
  }

  private void testApply(ActionRequest request) {
    filter.apply(task, "_action", request, listener, chain);
    verify(chain).proceed(eq(task), eq("_action"), eq(request), any());
  }

  @Test
  public void testOrder() {
    assertEquals(Integer.MIN_VALUE, filter.order());
  }
}

