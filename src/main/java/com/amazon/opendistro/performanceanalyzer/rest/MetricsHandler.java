package com.amazon.opendistro.performanceanalyzer.rest;

import java.util.Map;
import java.util.HashMap;

class MetricsHandler {
    protected Map<String, String> getParamsMap(String query) {
        Map<String, String> result = new HashMap<>();
        if (query != null) {
            for (String param : query.split("&")) {
                String[] entry = param.split("=");
                if (entry.length > 1) {
                    result.put(entry[0], entry[1]);
                } else {
                    result.put(entry[0], "");
                }
            }
        }
        return result;
    }
}

