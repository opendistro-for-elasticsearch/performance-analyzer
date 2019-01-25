package com.amazon.opendistro.performanceanalyzer.rest;

import com.amazon.opendistro.performanceanalyzer.reader.ReaderMetricsProcessor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import java.security.InvalidParameterException;

import static org.junit.Assert.assertEquals;

@SuppressWarnings("serial")
public class QueryMetricsRequestHandlerTest {

    @Test
    public void testNodeJsonBuilder() throws Exception {
        String rootLocation = "test_files/dev/shm";
        ReaderMetricsProcessor mp = new ReaderMetricsProcessor(rootLocation);
        ReaderMetricsProcessor.current = mp;
        QueryMetricsRequestHandler qHandler = new QueryMetricsRequestHandler();
        HashMap<String, String> nodeResponses = new HashMap<String, String>() {{
            this.put("node1", "{'xyz':'abc'}");
            this.put("node2", "{'xyz':'abc'}");
        }};
        assertEquals("{\"node2\": {'xyz':'abc'}, \"node1\" :{'xyz':'abc'}}",
                qHandler.nodeJsonBuilder(nodeResponses));
    }

    @Test
    public void testQueryJson() throws Exception {
        String rootLocation = "build/private/test_resources/dev/shm";
        ReaderMetricsProcessor mp = new ReaderMetricsProcessor(rootLocation);
        ReaderMetricsProcessor.current = mp;
        mp.processMetrics(rootLocation, 1535065139000L);
        mp.processMetrics(rootLocation, 1535065169000L);
        mp.processMetrics(rootLocation, 1535065199000L);
        mp.processMetrics(rootLocation, 1535065229000L);
        mp.processMetrics(rootLocation, 1535065259000L);
        mp.processMetrics(rootLocation, 1535065289000L);
        mp.processMetrics(rootLocation, 1535065319000L);
        mp.processMetrics(rootLocation, 1535065349000L);
        QueryMetricsRequestHandler qHandler = new QueryMetricsRequestHandler();
        String response = qHandler.collectStats(mp.getMetricsDB().getValue(),
                1234L, Arrays.asList("cpu"),
                Arrays.asList("sum"),
                Arrays.asList("shard", "index", "operation"), null);
        assertEquals("{\"timestamp\": 1234, \"data\": {\"fields\":[{\"name\":"+
                     "\"shard\",\"type\":\"VARCHAR\"},{\"name\":\"index\","+
                     "\"type\":\"VARCHAR\"},{\"name\":\"operation\",\"type\":"+
                     "\"VARCHAR\"},{\"name\":\"cpu\",\"type\":\"DOUBLE\""+
                     "}],\"records\":[[null,null,\"GC\",0.0],[null,null,\"management\",0.0],[null,null,\"other\""+
                     ",0.0256],[null,null,\"refresh\",0.0],[\"0\",\"sonested\",\"shardfetch\",0.00159186808056345],"+
                     "[\"0\",\"sonested\",\"shardquery\",1.55800813191944]]}}", response);

    }



    @Test
    public void testParseArrayParameter() throws Exception {
        String rootLocation = "test_files/dev/shm";
        ReaderMetricsProcessor mp = new ReaderMetricsProcessor(rootLocation);
        ReaderMetricsProcessor.current = mp;
        QueryMetricsRequestHandler qHandler = new QueryMetricsRequestHandler();

        HashMap<String, String> params = new HashMap<String, String>();
        params.put("metrics", "cpu");

        List<String> ret = qHandler.parseArrayParam(params, "metrics", false);
        assertEquals(1, ret.size());
        assertEquals("cpu", ret.get(0));

        params.put("metrics", "cpu,rss");

        ret = qHandler.parseArrayParam(params, "metrics", false);
        assertEquals(2, ret.size());
        assertEquals("cpu", ret.get(0));
        assertEquals("rss", ret.get(1));
    }



    @Test
    public void testParseArrayParameterOptional() throws Exception {
        String rootLocation = "test_files/dev/shm";
        ReaderMetricsProcessor mp = new ReaderMetricsProcessor(rootLocation);
        ReaderMetricsProcessor.current = mp;
        QueryMetricsRequestHandler qHandler = new QueryMetricsRequestHandler();

        HashMap<String, String> params = new HashMap<String, String>();
        List<String> ret = qHandler.parseArrayParam(params, "metrics", true);
        assertEquals(0, ret.size());

        params.put("metrics", "");
        ret = qHandler.parseArrayParam(params, "metrics", true);
        assertEquals(0, ret.size());
    }



    @Test(expected = InvalidParameterException.class)
    public void testParseArrayParameterNoParam() throws Exception {
        String rootLocation = "test_files/dev/shm";
        ReaderMetricsProcessor mp = new ReaderMetricsProcessor(rootLocation);
        ReaderMetricsProcessor.current = mp;
        QueryMetricsRequestHandler qHandler = new QueryMetricsRequestHandler();

        HashMap<String, String> params = new HashMap<String, String>();
        List<String> ret = qHandler.parseArrayParam(params, "metrics", false);
    }

    @Test(expected = InvalidParameterException.class)
    public void testParseArrayParameterEmptyParam() throws Exception {
        String rootLocation = "test_files/dev/shm";
        ReaderMetricsProcessor mp = new ReaderMetricsProcessor(rootLocation);
        ReaderMetricsProcessor.current = mp;
        QueryMetricsRequestHandler qHandler = new QueryMetricsRequestHandler();

        HashMap<String, String> params = new HashMap<String, String>();
        params.put("metrics", "");
        List<String> ret = qHandler.parseArrayParam(params, "metrics", false);
    }
}

