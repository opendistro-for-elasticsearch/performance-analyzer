/*
 * Copyright <2019> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.util.concurrent.atomic.AtomicLong;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.tasks.Task;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.http_action.config.PerformanceAnalyzerConfigAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;

public class PerformanceAnalyzerActionFilter implements ActionFilter {
    private static AtomicLong uniqueID = new AtomicLong(0);

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(Task task, final String action, Request request,
                                                                                       ActionListener<Response> listener,
                                                                                       ActionFilterChain<Request, Response> chain) {
        if (PerformanceAnalyzerConfigAction.getInstance() != null && PerformanceAnalyzerConfigAction.getInstance().isFeatureEnabled()) {
            if (request instanceof BulkRequest) {
                PerformanceAnalyzerActionListener<Response> newListener = new PerformanceAnalyzerActionListener<>();
                String id = String.valueOf(uniqueID.getAndIncrement());
                long startTime = System.currentTimeMillis();
                BulkRequest bulk = (BulkRequest) request;
                newListener.set(RequestType.bulk, id, listener);
                newListener.saveMetricValues(
                        newListener.generateStartMetrics(startTime, "", bulk.requests().size()),
                        startTime, RequestType.bulk.toString(), id, PerformanceAnalyzerMetrics.START_FILE_NAME);
                chain.proceed(task, action, request, newListener);
                return;
            } else if (request instanceof SearchRequest) {
                PerformanceAnalyzerActionListener<Response> newListener = new PerformanceAnalyzerActionListener<>();
                String id = String.valueOf(uniqueID.getAndIncrement());
                long startTime = System.currentTimeMillis();
                SearchRequest search = (SearchRequest) request;
                newListener.set(RequestType.search, id, listener);
                newListener.saveMetricValues(
                        newListener.generateStartMetrics(startTime, String.join(",", search.indices()), 0),
                        startTime, RequestType.search.toString(), id, PerformanceAnalyzerMetrics.START_FILE_NAME);
                chain.proceed(task, action, request, newListener);
                return;
            }
        }

        chain.proceed(task, action, request, listener);
    }

    /**
     * The position of the filter in the chain. Execution is done from lowest order to highest.
     */
    @Override
    public int order() {
        return Integer.MIN_VALUE;
    }
}
