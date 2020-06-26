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

import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.Writeable;

public class WhoAmIAction extends ActionType<WhoAmIResponse> {

    public static final String NAME = "cluster:admin/performanceanalyzer/whoami";
    public static final Writeable.Reader<WhoAmIResponse> responseReader = null;

    private WhoAmIAction() {
        super(NAME, responseReader);
    }

    @Override
    public Writeable.Reader<WhoAmIResponse> getResponseReader() {
        return responseReader;
    }
}

