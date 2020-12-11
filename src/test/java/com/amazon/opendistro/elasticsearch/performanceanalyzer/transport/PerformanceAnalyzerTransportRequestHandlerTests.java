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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.transport;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PerformanceAnalyzerController;
import org.apache.commons.lang3.SystemUtils;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.support.replication.TransportReplicationAction.ConcreteShardRequest;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

@SuppressWarnings("unchecked")
public class PerformanceAnalyzerTransportRequestHandlerTests {
  private PerformanceAnalyzerTransportRequestHandler handler;
  private ConcreteShardRequest concreteShardRequest;

  @Mock private TransportRequestHandler transportRequestHandler;
  @Mock private PerformanceAnalyzerController controller;
  @Mock private TransportChannel channel;
  @Mock private TransportRequest request;
  @Mock private BulkShardRequest bulkShardRequest;
  @Mock private Task task;
  @Mock private ShardId shardId;

  @Before
  public void init() {
    // this test only runs in Linux system
    // as some of the static members of the ThreadList class are specific to Linux
    org.junit.Assume.assumeTrue(SystemUtils.IS_OS_LINUX);

    initMocks(this);
    handler = new PerformanceAnalyzerTransportRequestHandler(transportRequestHandler, controller);
    handler.set(transportRequestHandler);
    Mockito.when(controller.isPerformanceAnalyzerEnabled()).thenReturn(true);
  }

  @Test
  public void testMessageReceived() throws Exception {
    handler.messageReceived(request, channel, task);
    verify(transportRequestHandler).messageReceived(request, channel, task);
  }

  @Test
  public void testGetChannel() {
    concreteShardRequest = new ConcreteShardRequest(bulkShardRequest, "id", 1);
    handler.getChannel(concreteShardRequest, channel, task);

    Mockito.when(bulkShardRequest.shardId()).thenReturn(shardId);
    Mockito.when(bulkShardRequest.items()).thenReturn(new BulkItemRequest[1]);
    TransportChannel actualChannel = handler.getChannel(concreteShardRequest, channel, task);
    assertTrue(actualChannel instanceof PerformanceAnalyzerTransportChannel);
  }
}
