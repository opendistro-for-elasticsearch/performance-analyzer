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

package com.amazon.opendistro.performanceanalyzer.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;

import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.HttpDimension;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.HttpMetric;
import com.amazon.opendistro.performanceanalyzer.metrics.MetricsProcessor;
import com.amazon.opendistro.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;

public class PerformanceAnalyzerActionListener<Response> implements ActionListener<Response>, MetricsProcessor {

    private RequestType type;
    private ActionListener<Response> original;
    private String id;
    private static final int KEYS_PATH_LENGTH = 3;

    void set(RequestType type, String id, ActionListener<Response> original) {
        this.type = type;
        this.id = id;
        this.original = original;
    }

    @Override
    public void onResponse(Response response) {
        int responseStatus = -1;

        if (response instanceof BulkResponse) {
            BulkResponse bulk = (BulkResponse) response;
            responseStatus = bulk.status().getStatus();
        } else if (response instanceof SearchResponse) {
            SearchResponse search = (SearchResponse) response;
            responseStatus = search.status().getStatus();
        }

        //- If response type is BulkResponse/SearchResponse, responseStatus will not be -1
        if (responseStatus != -1) {
            long currTime = System.currentTimeMillis();
            saveMetricValues(generateFinishMetrics(currTime, responseStatus, ""),
                    currTime, type.toString(), id, PerformanceAnalyzerMetrics.FINISH_FILE_NAME);
        }

        original.onResponse(response);
    }

    @Override
    public void onFailure(Exception exception) {
        long currTime = System.currentTimeMillis();

        if (exception instanceof ElasticsearchException) {
            saveMetricValues(generateFinishMetrics(currTime, ((ElasticsearchException) exception).status().getStatus(),
                    exception.getClass().getName()), currTime, type.toString(), id, PerformanceAnalyzerMetrics.FINISH_FILE_NAME);
        } else {
            saveMetricValues(generateFinishMetrics(currTime, -1, exception.getClass().getName()),
                    currTime, type.toString(), id, PerformanceAnalyzerMetrics.FINISH_FILE_NAME);
        }

        original.onFailure(exception);
    }

    static String generateStartMetrics(long startTime, String indices, int itemCount) {
        return new StringBuilder()
                .append(PerformanceAnalyzerMetrics.getCurrentTimeMetric())
                .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(
                        HttpMetric.START_TIME.toString())
                .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(startTime)
                .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(
                        HttpDimension.INDICES.toString())
                .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(indices)
                .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(
                        HttpMetric.HTTP_REQUEST_DOCS.toString()).append(
                                PerformanceAnalyzerMetrics.sKeyValueDelimitor)
                .append(itemCount).toString();
    }

    static String generateFinishMetrics(long finishTime, int status, String exception) {
        return new StringBuilder()
                .append(PerformanceAnalyzerMetrics.getCurrentTimeMetric())
                .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(
                        HttpMetric.FINISH_TIME.toString())
                .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(finishTime)
                .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(
                        HttpDimension.HTTP_RESP_CODE.toString())
                .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor).append(status)
                .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(
                        HttpDimension.EXCEPTION.toString()).append(PerformanceAnalyzerMetrics.sKeyValueDelimitor)
                .append(exception).toString();
    }

    @Override
    public String getMetricsPath(long startTime, String... keysPath) {
        // throw exception if keys.length is not equal to 3 (Keys should be requestType, requestID, start/finish)
        if (keysPath.length != KEYS_PATH_LENGTH) {
            throw new RuntimeException("keys length should be " + KEYS_PATH_LENGTH);
        }

        return PerformanceAnalyzerMetrics.generatePath(startTime, PerformanceAnalyzerMetrics.sThreadsPath,
                PerformanceAnalyzerMetrics.sHttpPath, keysPath[0], keysPath[1], keysPath[2]);
    }
}
