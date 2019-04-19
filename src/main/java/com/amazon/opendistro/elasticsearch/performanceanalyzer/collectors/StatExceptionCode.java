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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors;

public enum StatExceptionCode {
    ERROR_COUNT("ErrorCount"),
    START_COUNT("StartCount"),
    RESTART_COUNT("RestartCount"),
    METRICS_WRITE_ERROR("MetricsWriteError"),
    METRICS_REMOVE_ERROR("MetricsRemoveError"),
    JVM_ATTACH_ERROR("JvmAttachErrror"),
    MASTER_METRICS_ERROR("MasterMetricsError"),
    DISK_METRICS_ERROR("DiskMetricsError"),
    THREAD_IO_ERROR("ThreadIOError"),
    SCHEMA_PARSER_ERROR("SchemaParserError"),
    JSON_PARSER_ERROR("JsonParserError"),
    NETWORK_COLLECTION_ERROR("NetworkCollectionError"),
    NODESTATS_COLLECTION_ERROR("NodeStatsCollectionError"),
    OTHER_COLLECTION_ERROR("OtherCollectionError"),
    OTHER("Other");

    private final String value;

    StatExceptionCode(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}

