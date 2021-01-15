/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.http_action.whoami;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ESResources;
import java.io.IOException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

@SuppressWarnings("unchecked")
public class WhoAmITests {
  private TransportWhoAmIAction transportWhoAmIAction;
  private WhoAmIAction whoAmIAction;
  private WhoAmIResponse response;

  @Mock private IndicesService indicesService;
  @Mock private ActionListener listener;
  @Mock private TransportService transportService;
  @Mock private ActionFilters actionFilters;
  @Mock private Task task;

  @Before
  public void init() {
    initMocks(this);

    transportWhoAmIAction = new TransportWhoAmIAction(transportService, actionFilters, indicesService);
    whoAmIAction = WhoAmIAction.INSTANCE;
    response = new WhoAmIResponse();
  }

  @Test
  public void testDoExecute() {
    WhoAmIRequestBuilder builder = new WhoAmIRequestBuilder(null);
    transportWhoAmIAction.doExecute(task, new WhoAmIRequest(), listener);
    verify(listener).onResponse(any());
    assertEquals(indicesService, ESResources.INSTANCE.getIndicesService());
  }

  @Test
  public void testWhoAmIActionReader() {
    Reader<WhoAmIResponse> reader = whoAmIAction.getResponseReader();
    assertEquals(WhoAmIAction.responseReader, reader);
  }

  @Test
  public void testWhoAmIActionResponse() throws IOException {
    XContentBuilder contentBuilder = XContentBuilder.builder(XContentType.JSON.xContent());
    XContentBuilder builder = response.toXContent(contentBuilder, null);
    response.writeTo(null);
    assertEquals(contentBuilder, builder);
  }
}
