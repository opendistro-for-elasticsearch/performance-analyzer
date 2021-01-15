/*
 * Copyright <2020> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.action;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.TestUtil;
import java.util.List;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.rest.RestStatus;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

@SuppressWarnings("unchecked")
public class PerformanceAnalyzerActionListenerTests {
    private static final String listenerId= "12345";
    private long startTimeInMills = 1153721339;
    private PerformanceAnalyzerActionListener actionListener;
    private ActionListener originalActionListener;

    @Before
    public void init() {
        originalActionListener = Mockito.mock(ActionListener.class);
        actionListener = new PerformanceAnalyzerActionListener();

        //clean metricQueue before running every test
        TestUtil.readEvents();
    }

    @Test
    public void testGetMetricsPath() {
        String expectedPath = PluginSettings.instance().getMetricsLocation()
            + PerformanceAnalyzerMetrics.getTimeInterval(startTimeInMills) + "/"
            + PerformanceAnalyzerMetrics.sThreadsPath + "/"
            + PerformanceAnalyzerMetrics.sHttpPath + "/"
            + "bulk/bulkId/start";
        String actualPath = actionListener.getMetricsPath(startTimeInMills, "bulk", "bulkId", "start");
        assertEquals(expectedPath, actualPath);

        try {
            actionListener.getMetricsPath(startTimeInMills, "bulk");
            fail("Negative scenario test: Should have been a RuntimeException");
        } catch (RuntimeException ex) {
            //- expecting exception...1 values passed; 3 expected
        }
    }

    @Test
    public void testOnResponseWithBulkResponse() {
        BulkResponse bulkResponse = Mockito.mock(BulkResponse.class);
        Mockito.when(bulkResponse.status()).thenReturn(RestStatus.OK);
        actionListener.set(RequestType.bulk, listenerId , originalActionListener);
        testOnResponse(bulkResponse);
    }

    @Test
    public void testOnResponseWithSearchResponse() {
        SearchResponse searchResponse = Mockito.mock(SearchResponse.class);
        Mockito.when(searchResponse.status()).thenReturn(RestStatus.OK);
        actionListener.set(RequestType.search, listenerId , originalActionListener);
        testOnResponse(searchResponse);
    }

    @Test
    public void testOnFailureWithElasticsearchException() {
        ElasticsearchException exception = Mockito.mock(ElasticsearchException.class);
        Mockito.when(exception.status()).thenReturn(RestStatus.INTERNAL_SERVER_ERROR);
        actionListener.set(RequestType.search, listenerId , originalActionListener);
        actionListener.onFailure(exception);
        String[] metricsValues = readMetricsValue();
        assertEquals("HTTPRespCode:500", metricsValues[2]);
        assertTrue( metricsValues[3].contains("Exception:org.elasticsearch.ElasticsearchException"));
    }

    @Test
    public void testOnFailureWithException() {
        NullPointerException exception = new NullPointerException();
        actionListener.set(RequestType.search, listenerId , originalActionListener);
        actionListener.onFailure(exception);
        String[] metricsValues = readMetricsValue();
        assertEquals("HTTPRespCode:-1", metricsValues[2]);
        assertTrue( metricsValues[3].contains("Exception:java.lang.NullPointerException"));
    }

    private void testOnResponse(ActionResponse response) {
        actionListener.onResponse(response);

        String[] metricsValues = readMetricsValue();
        assertEquals("HTTPRespCode:200", metricsValues[2]);
        assertEquals("Exception:", metricsValues[3]);
    }

    private String[] readMetricsValue() {
        List<Event> metrics = TestUtil.readEvents();
        assert metrics.size() == 1;
        String[] metricsValues = metrics.get(0).value.split("\n");
        assert metricsValues.length == 4;
        return metricsValues;
    }
}
