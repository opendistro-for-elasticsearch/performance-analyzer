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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.http_action.config;

public class PerformanceAnalyzerConfigParams {
    // Params provided by end user to Config API at Port 9200
    public static final String PA_ENABLED = "pa_enabled";
    public static final String RCA_ENABLED = "rca_enabled";
    public static final String PA_LOGGING_ENABLED = "logging_enabled";
    public static final String SHARDS_PER_COLLECTION = "shards_per_collection";
    public static final String MUTED_RCAS = "muted_rcas";

    // URL for node level config changes
    public static final String NODE_CONFIG_PATH = "/_opendistro/_performanceanalyzer/node/config";

    // URL for cluster level config changes
    public static final String CLUSTER_CONFIG_PATH = "/_opendistro/_performanceanalyzer/cluster/config";
}
