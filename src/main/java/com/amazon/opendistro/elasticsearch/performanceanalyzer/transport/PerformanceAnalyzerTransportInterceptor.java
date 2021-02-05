/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PerformanceAnalyzerController;

public class PerformanceAnalyzerTransportInterceptor implements TransportInterceptor {

    private static final Logger LOG = LogManager.getLogger(PerformanceAnalyzerTransportInterceptor.class);
    private final PerformanceAnalyzerController controller;

    public PerformanceAnalyzerTransportInterceptor(final PerformanceAnalyzerController controller) {
        this.controller = controller;
    }

    @Override
    public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(String action,
                                                                                    String executor,
                                                                                    boolean forceExecution,
                                                                                    TransportRequestHandler<T> actualHandler) {
        return new PerformanceAnalyzerTransportRequestHandler<>(actualHandler, controller);
    }
}
