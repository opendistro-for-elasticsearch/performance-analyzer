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

package com.amazon.opendistro.performanceanalyzer.transport;

import com.amazon.opendistro.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.performanceanalyzer.metrics.MetricsProcessor;
import com.amazon.opendistro.performanceanalyzer.metrics.ThreadIDUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseOptions;

import java.util.concurrent.atomic.AtomicLong;

public class PerformanceAnalyzerTransportChannel implements TransportChannel, MetricsProcessor {
    private static final Logger LOG = LogManager.getLogger(PerformanceAnalyzerTransportChannel.class);
    private static final int KEYS_PATH_LENGTH = 3;
    private static final AtomicLong UNIQUE_ID = new AtomicLong(0);

    private TransportChannel original;
    private String indexName;
    private int shardId;
    private boolean primary;
    private String id;
    private String threadID;

    void set(TransportChannel original, long startTime, String indexName, int shardId, int itemCount, boolean bPrimary) {
        this.original = original;
        this.id = String.valueOf(UNIQUE_ID.getAndIncrement());
        this.indexName = indexName;
        this.shardId = shardId;
        this.primary = bPrimary;
        this.threadID = String.valueOf(ThreadIDUtil.INSTANCE.getNativeCurrentThreadId());

        StringBuilder value = new StringBuilder().append(PerformanceAnalyzerMetrics.getCurrentTimeMetric())
                .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(AllMetrics.ShardBulk_Metrics.startTime.name())
                .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(startTime)
                .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(AllMetrics.ShardBulk_Metrics.itemCount.name())
                .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(itemCount)
                .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(AllMetrics.ShardBulk_Metrics.indexName.name())
                .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(indexName)
                .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(AllMetrics.ShardBulk_Metrics.shardId.name())
                .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(shardId)
                .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(AllMetrics.ShardBulk_Metrics.primary.name())
                .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(bPrimary);

        saveMetricValues(value.toString(), startTime, threadID, id, PerformanceAnalyzerMetrics.START_FILE_NAME);
    }

    @Override
    public String getProfileName() {
        return "PerformanceAnalyzerTransportChannelProfile";
    }

    @Override
    public String getChannelType() {
        return "PerformanceAnalyzerTransportChannelType";
    }


    @Override
    public void sendResponse(TransportResponse response) throws IOException {
        emitMetricsFinish(null);
        original.sendResponse(response);
    }

    @Override
    public void sendResponse(TransportResponse response, TransportResponseOptions responseOptions) throws IOException {
        emitMetricsFinish(null);
        original.sendResponse(response, responseOptions);
    }

    @Override
    public void sendResponse(Exception exception) throws IOException {
        emitMetricsFinish(exception);
        original.sendResponse(exception);
    }

    private void emitMetricsFinish(Exception exception) {
        long currTime = System.currentTimeMillis();
        StringBuilder value = new StringBuilder().append(PerformanceAnalyzerMetrics.getCurrentTimeMetric())
                .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(AllMetrics.ShardBulk_Metrics.finishTime.name())
                .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(currTime)
                .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(AllMetrics.ShardBulk_Metrics.indexName.name())
                .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(indexName)
                .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(AllMetrics.ShardBulk_Metrics.shardId.name())
                .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(shardId)
                .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(AllMetrics.ShardBulk_Metrics.primary.name())
                .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(primary);
        if (exception != null) {
            value.append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(AllMetrics.ShardBulk_Metrics.exception.name())
                    .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(exception.getClass().getName());
            value.append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(AllMetrics.ShardBulk_Metrics.failed.name())
                    .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(true);
        } else {
            value.append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(AllMetrics.ShardBulk_Metrics.failed.name())
                    .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(false);
        }

        saveMetricValues(value.toString(), currTime, threadID, id, PerformanceAnalyzerMetrics.FINISH_FILE_NAME);
    }

    @Override
    public String getMetricsPath(long startTime, String... keysPath) {
        // throw exception if keys.length is not equal to 3 (Keys should be threadID, ShardBulkId, start/finish)
        if (keysPath.length != KEYS_PATH_LENGTH) {
            throw new RuntimeException("keys length should be " + KEYS_PATH_LENGTH);
        }
        return PerformanceAnalyzerMetrics.generatePath(startTime, PerformanceAnalyzerMetrics.sThreadsPath,
                keysPath[0], PerformanceAnalyzerMetrics.sShardBulkPath, keysPath[1], keysPath[2]);
    }
}
