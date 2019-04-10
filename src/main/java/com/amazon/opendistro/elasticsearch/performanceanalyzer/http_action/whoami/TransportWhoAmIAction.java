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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.http_action.whoami;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ESResources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;

public class TransportWhoAmIAction extends HandledTransportAction<WhoAmIRequest, WhoAmIResponse> {

    @Inject
    public TransportWhoAmIAction(final TransportService transportService,
                                 final ActionFilters actionFilters,
                                 final IndicesService indicesService) {
        super(WhoAmIAction.NAME, transportService, actionFilters, WhoAmIRequest::new);
        ESResources.INSTANCE.setIndicesService(indicesService);
    }

    @Override
    protected void doExecute(Task task, WhoAmIRequest request, ActionListener<WhoAmIResponse> listener) {
        listener.onResponse(new WhoAmIResponse());
    }
}
