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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.listener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.search.internal.SearchContext;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PerformanceAnalyzerController;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CommonDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CommonMetric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.ThreadIDUtil;

public class PerformanceAnalyzerSearchListener implements SearchOperationListener, SearchListener, MetricsProcessor {
    private static final Logger LOG = LogManager.getLogger(PerformanceAnalyzerSearchListener.class);

    private static final SearchListener NO_OP_SEARCH_LISTENER = new NoOpSearchListener();
    private static final int KEYS_PATH_LENGTH = 4;
    private final PerformanceAnalyzerController controller;
    private SearchListener searchListener;

    public PerformanceAnalyzerSearchListener(final PerformanceAnalyzerController controller) {
        this.controller = controller;
    }

    @Override
    public String toString() {
        return "PerformanceAnalyzerSearchListener";
    }


    private SearchListener getSearchListener() {
        return controller.isPerformanceAnalyzerEnabled() ? this : NO_OP_SEARCH_LISTENER;
    }

    @Override
    public void onPreQueryPhase(SearchContext searchContext) {
        getSearchListener().preQueryPhase(searchContext);
    }

    @Override
    public void onQueryPhase(SearchContext searchContext, long tookInNanos) {
        getSearchListener().queryPhase(searchContext, tookInNanos);
    }

    @Override
    public void onFailedQueryPhase(SearchContext searchContext) {
        getSearchListener().failedQueryPhase(searchContext);
    }

    @Override
    public void onPreFetchPhase(SearchContext searchContext) {
        getSearchListener().preFetchPhase(searchContext);
    }

    @Override
    public void onFetchPhase(SearchContext searchContext, long tookInNanos) {
        getSearchListener().fetchPhase(searchContext, tookInNanos);
    }

    @Override
    public void onFailedFetchPhase(SearchContext searchContext) {
        getSearchListener().failedFetchPhase(searchContext);
    }

    @Override
    public void preQueryPhase(SearchContext searchContext) {
        long currTime = System.currentTimeMillis();
        saveMetricValues(
                generateStartMetrics(currTime, searchContext.request().shardId().getIndexName(), searchContext.request().shardId().getId()),
                currTime,
                String.valueOf(ThreadIDUtil.INSTANCE.getNativeCurrentThreadId()),
                PerformanceAnalyzerMetrics.sShardQueryPath, String.valueOf(searchContext.id()), PerformanceAnalyzerMetrics.START_FILE_NAME);
    }

    @Override
    public void queryPhase(SearchContext searchContext, long tookInNanos) {
        long currTime = System.currentTimeMillis();
        saveMetricValues(
                generateFinishMetrics(currTime, false,
                        searchContext.request().shardId().getIndexName(), searchContext.request().shardId().getId()),
                currTime,
                String.valueOf(ThreadIDUtil.INSTANCE.getNativeCurrentThreadId()),
                PerformanceAnalyzerMetrics.sShardQueryPath, String.valueOf(searchContext.id()),
                PerformanceAnalyzerMetrics.FINISH_FILE_NAME);
    }

    @Override
    public void failedQueryPhase(SearchContext searchContext) {
        long currTime = System.currentTimeMillis();
        saveMetricValues(
                generateFinishMetrics(currTime, true,
                        searchContext.request().shardId().getIndexName(), searchContext.request().shardId().getId()),
                currTime,
                String.valueOf(ThreadIDUtil.INSTANCE.getNativeCurrentThreadId()),
                PerformanceAnalyzerMetrics.sShardQueryPath, String.valueOf(searchContext.id()),
                PerformanceAnalyzerMetrics.FINISH_FILE_NAME);
    }

    @Override
    public void preFetchPhase(SearchContext searchContext) {
        long currTime = System.currentTimeMillis();
        saveMetricValues(
                generateStartMetrics(currTime, searchContext.request().shardId().getIndexName(), searchContext.request().shardId().getId()),
                currTime,
                String.valueOf(ThreadIDUtil.INSTANCE.getNativeCurrentThreadId()),
                PerformanceAnalyzerMetrics.sShardFetchPath, String.valueOf(searchContext.id()), PerformanceAnalyzerMetrics.START_FILE_NAME);
    }

    @Override
    public void fetchPhase(SearchContext searchContext, long tookInNanos) {
        long currTime = System.currentTimeMillis();
        saveMetricValues(
                generateFinishMetrics(currTime, false,
                        searchContext.request().shardId().getIndexName(), searchContext.request().shardId().getId()),
                currTime,
                String.valueOf(ThreadIDUtil.INSTANCE.getNativeCurrentThreadId()),
                PerformanceAnalyzerMetrics.sShardFetchPath, String.valueOf(searchContext.id()),
                PerformanceAnalyzerMetrics.FINISH_FILE_NAME);
    }

    @Override
    public void failedFetchPhase(SearchContext searchContext) {
        long currTime = System.currentTimeMillis();
        saveMetricValues(
                generateFinishMetrics(currTime, true,
                        searchContext.request().shardId().getIndexName(), searchContext.request().shardId().getId()),
                currTime,
                String.valueOf(ThreadIDUtil.INSTANCE.getNativeCurrentThreadId()),
                PerformanceAnalyzerMetrics.sShardFetchPath, String.valueOf(searchContext.id()),
                PerformanceAnalyzerMetrics.FINISH_FILE_NAME);
    }

    @SuppressWarnings("checkstyle:magicnumber")
    @Override
    public String getMetricsPath(long startTime, String... keysPath) {
        // throw exception if keys.length is not equal to 4 (Keys should be threadID, SearchType, ShardSearchID, start/finish)
        if (keysPath.length != KEYS_PATH_LENGTH) {
            throw new RuntimeException("keys length should be " + KEYS_PATH_LENGTH);
        }

        return PerformanceAnalyzerMetrics.generatePath(startTime, PerformanceAnalyzerMetrics.sThreadsPath,
                keysPath[0], keysPath[1], keysPath[2], keysPath[3]);
    }

    public static String generateStartMetrics(long startTime, String indexName, int shardId) {
        return new StringBuilder().append(PerformanceAnalyzerMetrics.getCurrentTimeMetric())
                .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(CommonMetric.START_TIME.toString())
                .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(startTime)
                .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(CommonDimension.INDEX_NAME.toString())
                .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(indexName)
                .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(CommonDimension.SHARD_ID.toString())
                .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(shardId).toString();
    }

    public static String generateFinishMetrics(long finishTime, boolean failed, String indexName, int shardId) {
        return new StringBuilder().append(PerformanceAnalyzerMetrics.getCurrentTimeMetric())
                .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(CommonMetric.FINISH_TIME.toString())
                .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(finishTime)
                .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(CommonDimension.FAILED.toString())
                .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(failed)
                .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(CommonDimension.INDEX_NAME.toString())
                .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(indexName)
                .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(CommonDimension.SHARD_ID.toString())
                .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(shardId).toString();
    }
}
