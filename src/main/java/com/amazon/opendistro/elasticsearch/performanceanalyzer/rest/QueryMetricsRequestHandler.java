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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.net.ssl.HttpsURLConnection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;

import org.jooq.Record;
import org.jooq.Result;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.model.MetricAttributes;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.model.MetricsModel;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterLevelMetricsReader;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ReaderMetricsProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.JsonConverter;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

/**
 * Request handler that supports querying MetricsDB on every EC2 instance.
 * Example query – "http://localhost:9600/_metricsdb?metrics=cpu,rss,memory%20agg=sum,avg,sum%20dims=index,operation,shard."
 * We can fetch multiple metrics using this interface and also specify the dimensions/aggregations for fetching the metrics.
 * We create a new metricsDB every 5 seconds and API only supports querying the latest snapshot.
 */
public class QueryMetricsRequestHandler extends MetricsHandler implements HttpHandler {

    private static final Logger LOG = LogManager.getLogger(QueryMetricsRequestHandler.class);
    private static final int HTTP_CLIENT_CONNECTION_TIMEOUT = 200;

    public QueryMetricsRequestHandler() {
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String requestMethod = exchange.getRequestMethod();
        Map.Entry<Long, MetricsDB> dbEntry = ReaderMetricsProcessor.current.getMetricsDB();
        if (dbEntry == null) {
            sendResponse(exchange,
                    "{\"error\":\"There are no metrics databases. The reader has run into an issue or has just started.\"}",
                    HttpURLConnection.HTTP_UNAVAILABLE);

            LOG.warn("There are no metrics databases. The reader has run into an issue or has just started.");
            return;
        }
        MetricsDB db = dbEntry.getValue();
        Long dbTimestamp = dbEntry.getKey();

        if (requestMethod.equalsIgnoreCase("GET")) {
            LOG.debug("Query handler called.");

            if (isUnitLookUp(exchange)) {
                getMetricUnits(exchange);
                return;
            }

            Map<String, String> params = getParamsMap(exchange.getRequestURI().getQuery());

            exchange.getResponseHeaders().set("Content-Type", "application/json");

            try {
                List<String> metricList = parseArrayParam(params, "metrics", false);
                List<String> aggList = parseArrayParam(params, "agg", false);
                List<String> dimList = parseArrayParam(params, "dim", true);

                if (metricList.size() != aggList.size()) {
                    sendResponse(exchange,
                            "{\"error\":\"metrics/aggregations should have the same number of entries.\"}",
                            HttpURLConnection.HTTP_BAD_REQUEST);
                    return;
                }

                if (!validParams(exchange, metricList, dimList)) {
                    return;
                }

                String nodes = params.get("nodes");
                String response = collectStats(db, dbTimestamp, metricList, aggList, dimList, nodes);
                sendResponse(exchange, response, HttpURLConnection.HTTP_OK);
            } catch (Exception e) {
                LOG.error("DB file path : {}", db.getDBFilePath());
                LOG.error(
                        (Supplier<?>) () -> new ParameterizedMessage(
                                "QueryException {}.",
                                e.toString()),
                        e);
                String response = "{\"error\":\"" + e.toString() + "\"}";
                sendResponse(exchange, response, HttpURLConnection.HTTP_INTERNAL_ERROR);
            }
        } else {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_NOT_FOUND, -1);
            exchange.close();
        }
    }

    private boolean isUnitLookUp(HttpExchange exchange) throws IOException {
        if (exchange.getRequestURI().toString().equals(PerformanceAnalyzerApp.QUERY_URL + "/units")) {
            return true;
        }
        return false;
    }

    private void getMetricUnits(HttpExchange exchange) throws IOException {
        Map<String, String> metricUnits = new HashMap<>();
        for (Map.Entry<String, MetricAttributes> entry : MetricsModel.ALL_METRICS.entrySet()) {
            String metric = entry.getKey();
            String unit = entry.getValue().unit;
            metricUnits.put(metric, unit);
        }
        sendResponse(exchange, JsonConverter.writeValueAsString(metricUnits), HttpURLConnection.HTTP_OK);
    }

    public boolean validParams(HttpExchange exchange, List<String> metricList, List<String> dimList)
            throws IOException {
        for (String metric : metricList) {
            if (MetricsModel.ALL_METRICS.get(metric) == null) {
                sendResponse(exchange,
                        String.format("{\"error\":\"%s is an invalid metric.\"}", metric),
                        HttpURLConnection.HTTP_BAD_REQUEST);
                return false;
            } else {
                for (String dim : dimList) {
                    if (!MetricsModel.ALL_METRICS.get(metric).dimensionNames.contains(dim)) {
                        sendResponse(exchange,
                                String.format("{\"error\":\"%s is an invalid dimension for %s metric.\"}",
                                        dim, metric),
                                HttpURLConnection.HTTP_BAD_REQUEST);
                        return false;
                    }
                }
            }
        }
        return true;
    }

    public List<String> parseArrayParam(Map<String, String> params, String name, boolean optional) {
        if (!optional) {
            if (!params.containsKey(name) || params.get(name).isEmpty()) {
                throw new InvalidParameterException(String.format("%s parameter needs to be set", name));
            }
        }

        if (params.containsKey(name) && !params.get(name).isEmpty()) {
            return Arrays.asList(params.get(name).split(","));
        }
        return new ArrayList<>();
    }

    public void sendResponse(HttpExchange exchange, String response, int status) throws IOException {
        try (OutputStream os = exchange.getResponseBody()) {
            exchange.sendResponseHeaders(status, response.length());
            os.write(response.getBytes());
        } catch (Exception e) {
            response = e.toString();
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_INTERNAL_ERROR, response.length());
        }
    }

    public String getParamString(List<String> metricList, List<String> aggList,
            List<String> dimList) {
        String metricString = "metrics=" + String.join(",", metricList);
        String aggString = "agg=" + String.join(",", aggList);
        String dimString = "dim=" + String.join(",", dimList);
        return String.join("&", metricString, aggString, dimString);
    }

    public String collectStats(MetricsDB db, Long dbTimestamp, List<String> metricList,
            List<String> aggList, List<String> dimList, String nodeParam) throws Exception {
        String localResponse = "";
        if (db != null) {
            Result<Record> metricResult = db.queryMetric(
                    metricList, aggList, dimList);
            if (metricResult == null) {
                localResponse = "{}";
            } else {
                localResponse = metricResult.formatJSON();
            }
        } else {
            //Empty JSON.
            localResponse = "{}";
        }
        String localResponseWithTimestamp = getQueryJsonWithTimestamp(dbTimestamp, localResponse);

        if (nodeParam == null) {
            return localResponseWithTimestamp;
        }

        if (nodeParam.equals("all")) {
            LOG.debug("Collecting metrics from all nodes");
            HashMap<String, String> nodeResponses = new HashMap<>();
            String params = getParamString(metricList, aggList, dimList);
            ClusterLevelMetricsReader.NodeDetails[] nodes = ClusterLevelMetricsReader.getNodes();
            String localNodeId = "local";
            if (nodes.length != 0) {
                localNodeId = nodes[0].getId();
            }
            nodeResponses.put(localNodeId, localResponseWithTimestamp);
            for (int i = 1; i < nodes.length; i++) {
                ClusterLevelMetricsReader.NodeDetails node = nodes[i];
                LOG.debug("Collecting remote stats");
                try {
                String remoteNodeStats = collectRemoteStats(node.getHostAddress(),
                        PerformanceAnalyzerApp.QUERY_URL,
                        params
                        );
                nodeResponses.put(node.getId(), remoteNodeStats);
                } catch (Exception e) {
                    LOG.error("Unable to collect stats for node, addr:{}, exception: {}",
                            node.getHostAddress(), e);
                }
            }
            String response = nodeJsonBuilder(nodeResponses);
            LOG.debug("Returned the final text - \n{}", response);
            return response;
        }
        return localResponseWithTimestamp;
    }

    public String getQueryJsonWithTimestamp(Long timestamp, String queryResponse) {
        return String.format("{\"timestamp\": %d, \"data\": %s}", timestamp, queryResponse);
    }

    public String nodeJsonBuilder(HashMap<String, String> nodeResponses) {
        StringBuilder outputJson = new StringBuilder();
        outputJson.append("{");
        Set<String> nodeSet = nodeResponses.keySet();
        String[] nodes = nodeSet.toArray(new String[nodeSet.size()]);
        if (nodes.length > 0) {
            outputJson.append("\"");
            outputJson.append(nodes[0]);
            outputJson.append("\": ");
            outputJson.append(nodeResponses.get(nodes[0]));
        }

        for (int i = 1; i < nodes.length; i++) {
            outputJson.append(", \"");
            outputJson.append(nodes[i]);
            outputJson.append("\" :");
            outputJson.append(nodeResponses.get(nodes[i]));
        }

        outputJson.append("}");
        return outputJson.toString();
    }

    protected String collectRemoteStats(String nodeIP, String uri, String queryString) throws Exception {
        HttpURLConnection conn = getUrlConnection(nodeIP, uri, queryString);

        conn.setConnectTimeout(HTTP_CLIENT_CONNECTION_TIMEOUT);
        int responseCode = conn.getResponseCode();
        if (responseCode != HttpURLConnection.HTTP_OK) {
            LOG.error("Did not receive 200 from remote node. NodeIP-{}", nodeIP);
            throw new Exception("Did not receive a 200 response code from the remote node.");
        }

        BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        StringBuilder response = new StringBuilder();
        String inputLine;
        try {
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
        } finally {
            in.close();
        }

        return response.toString();
    }

    private HttpURLConnection getUrlConnection(String nodeIP, String uri, String queryString) throws IOException {
        boolean httpsEnabled = PluginSettings.instance().getHttpsEnabled();
        String protocol = "http";
        if (httpsEnabled) {
            protocol = "https";
        }
        String urlString = String.format("%s://%s:9600%s?%s", protocol, nodeIP, uri, queryString);
        LOG.debug("Remote URL - {}", urlString);
        URL url = new URL(urlString);

        if (httpsEnabled) {
            return (HttpsURLConnection) url.openConnection();
        } else {
            return (HttpURLConnection) url.openConnection();
        }
    }
}

