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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.http_action.whoami;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;

public class WhoAmIRequest extends BaseNodesRequest<WhoAmIRequest> {

  private static final String[] EMPTY_NODE_ID_ARRAY = {};

  public WhoAmIRequest() {
    this(null);
  }

  public WhoAmIRequest(StreamInput input) {
    // Adding a ctor which matches the FunctionalInterface Writable.Reader<> so that ctor
    // reference succeeds.
    // The ctor itself calls the BaseNodesRequest<>(String... nodeIds) ctor with empty string
    // array(same functionality as 7.3.2's default ctor) since it was removed in 7.4.2.
    super(EMPTY_NODE_ID_ARRAY);
  }
}
