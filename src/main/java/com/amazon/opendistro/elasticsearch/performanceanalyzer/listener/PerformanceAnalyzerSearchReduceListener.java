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
//
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//
//import PerformanceAnalyzerMetrics;
//import ThreadIDUtil;
//import MetricsProcessor;
//
//import java.util.Collection;
//import java.util.stream.Collectors;
//
//import org.elasticsearch.search.SearchReduceListener;
//import org.elasticsearch.search.SearchPhaseResult;
//import org.elasticsearch.search.query.QuerySearchResult;
//import org.elasticsearch.search.SearchShardTarget;
//import static org.elasticsearch.action.search.SearchPhaseController.ReducedQueryPhase;
//import AllMetrics.SearchReducer_Metrics;
//import PerformanceAnalyzerConfigAction;
//
//
//interface SearchRListener {
//    default void preReducePhase(Collection<? extends SearchPhaseResult> results) { }
//    default void reducePhase(Collection<? extends SearchPhaseResult> results,
//              ReducedQueryPhase reducedQueryPhase, long tookTimeInNanos) { }
//}
//
//class NoOpSearchRListener implements SearchRListener {
//    @Override
//    public String toString() {
//        return "NoOpSearchRListener";
//    }
//}
//
//
//public class PerformanceAnalyzerSearchReduceListener implements SearchRListener, SearchReduceListener, MetricsProcessor {
//
//    private static final Logger LOG = LogManager.getLogger(PerformanceAnalyzerSearchReduceListener.class);
//    private static final SearchRListener NO_OP_SEARCH_R_LISTENER = new NoOpSearchRListener();
//    private static final int KEYS_PATH_LENGTH = 4;
//
//    private SearchRListener getRSearchListener() {
//        return PerformanceAnalyzerConfigAction.getInstance() != null
//              && PerformanceAnalyzerConfigAction.getInstance().isFeatureEnabled() ? this : NO_OP_SEARCH_R_LISTENER;
//    }
//
//
//    @Override
//    public void onPreReducePhase(Collection<? extends SearchPhaseResult> results) {
//        getRSearchListener().preReducePhase(results);
//    }
//
//    @Override
//    public void onReducePhase(Collection<? extends SearchPhaseResult> results,
//                              ReducedQueryPhase reducedQueryPhase, long tookTimeInNanos) {
//        getRSearchListener().reducePhase(results, reducedQueryPhase, tookTimeInNanos);
//    }
//
//    @Override
//    public void preReducePhase(Collection<? extends SearchPhaseResult> results) {
//        if (results.size() != 0) {
//            final QuerySearchResult firstResult = results.stream().findFirst().get().queryResult();
//            final boolean hasSuggest = firstResult.suggest() != null;
//            final boolean hasProfileResults = firstResult.hasProfileResults();
//            final boolean consumeAggs = firstResult.hasAggs(); //- todo: double check
//            long requestID = firstResult.getRequestId();
//            long currentTime = System.currentTimeMillis();
//            String indexNames = results.stream().map(SearchPhaseResult::getSearchShardTarget)
//                    .map(SearchShardTarget::getIndex).collect(Collectors.toSet()).toString(); //- todo: could be expensive
//
//            StringBuilder value = new StringBuilder();
//            value.append(PerformanceAnalyzerMetrics.getCurrentTimeMetric())
//                    .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(SearchReducer_Metrics.startTime.name())
//                    .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(currentTime)
//                    .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(SearchReducer_Metrics.hasSuggest.name())
//                    .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(hasSuggest)
//                    .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(SearchReducer_Metrics.hasProfileResults.name())
//                    .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(hasProfileResults)
//                    .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(SearchReducer_Metrics.hasAggs.name())
//                    .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(consumeAggs)
//                    .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(SearchReducer_Metrics.indexNames.name())
//                    .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(indexNames);
//
//            saveMetricValues(value.toString(), currentTime,
//                    String.valueOf(ThreadIDUtil.INSTANCE.getNativeCurrentThreadId()),
//                    PerformanceAnalyzerMetrics.sSearchReducePath,
//                    String.valueOf(requestID),
//                    PerformanceAnalyzerMetrics.START_FILE_NAME);
//        }
//    }
//
//    @Override
//    public void reducePhase(Collection<? extends SearchPhaseResult> results, ReducedQueryPhase reducedQueryPhase, long tookTimeInNanos) {
//        if (results.size() != 0) {
//            long requestID = results.stream().findFirst().get().queryResult().getRequestId();
//            long currentTime = System.currentTimeMillis();
//            StringBuilder value = new StringBuilder();
//            value.append(PerformanceAnalyzerMetrics.getCurrentTimeMetric())
//                    .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(SearchReducer_Metrics.finishTime.name())
//                    .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(currentTime);
//            saveMetricValues(value.toString(), currentTime,
//                    String.valueOf(ThreadIDUtil.INSTANCE.getNativeCurrentThreadId()),
//                    PerformanceAnalyzerMetrics.sSearchReducePath,
//                    String.valueOf(requestID),
//                    PerformanceAnalyzerMetrics.FINISH_FILE_NAME);
//        }
//    }
//
//    @SuppressWarnings("checkstyle:magicnumber")
//    @Override
//    public String getMetricsPath(long startTime, String... keysPath) {
//        // throw exception if keys.length is not equal to 4 (Keys should be threadID, SearchType, SearchID, start/finish)
//        if (keysPath.length != KEYS_PATH_LENGTH) {
//            throw new RuntimeException("keys length should be " + KEYS_PATH_LENGTH);
//        }
//
//        return PerformanceAnalyzerMetrics.generatePath(startTime, PerformanceAnalyzerMetrics.sThreadsPath,
// keysPath[0], keysPath[1], keysPath[2], keysPath[3]);
//    }
//}
