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

package com.amazon.opendistro.performanceanalyzer.http_action.whoami;

import com.amazon.opendistro.performanceanalyzer.ESResources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.indices.IndicesService;

public class TransportWhoAmIAction extends HandledTransportAction<WhoAmIRequest, WhoAmIResponse> {

    @Inject
    public TransportWhoAmIAction(final Settings settings, final ThreadPool threadPool,
                                 final TransportService transportService, final ActionFilters actionFilters,
                                 final IndexNameExpressionResolver indexNameExpressionResolver, final IndicesService indicesService) {
        super(settings, WhoAmIAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, WhoAmIRequest::new);
        ESResources.INSTANCE.setIndicesService(indicesService);
    }

    @Override
    protected void doExecute(WhoAmIRequest request, ActionListener<WhoAmIResponse> listener) {
        listener.onResponse(new WhoAmIResponse());
    }
}
