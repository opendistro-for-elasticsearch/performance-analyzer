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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ESResources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class StatsCollector extends PerformanceAnalyzerMetricsCollector {
    private static final String LOG_ENTRY_INIT = "------------------------------------------------------------------------";
    private static final String LOG_ENTRY_END = "EOE";
    private static final String LOG_LINE_BREAK = "\n";
    private static final double MILLISECONDS_TO_SECONDS_DIVISOR = 1000D;

    private static final Logger STATS_LOGGER = LogManager.getLogger("stats_log");
    private static final Logger GENERAL_LOG = LogManager.getLogger(StatsCollector.class);
    private static StatsCollector statsCollector = null;
    public static String STATS_TYPE = "plugin-stats-metadata";

    private final Map<String, String> metadata;
    private Map<String, AtomicInteger> counters = new ConcurrentHashMap<>();
    private Date objectCreationTime = new Date();

    private List<StatExceptionCode> defaultExceptionCodes = new Vector<>();

    public static StatsCollector instance() {
        if(statsCollector == null) {
            synchronized(StatsCollector.class) {
                if(statsCollector == null) {
                    statsCollector = new StatsCollector(loadMetadata(PluginSettings.instance().getSettingValue(STATS_TYPE, STATS_TYPE)));
                }
            }
        }

        return statsCollector;
    }

    @VisibleForTesting
    Map<String, AtomicInteger> getCounters() {
        return counters;
    }
    public void logException() {
        logException(StatExceptionCode.OTHER);
    }

    public void logException(StatExceptionCode statExceptionCode) {
        incCounter(statExceptionCode.toString());
        incErrorCounter();
    }

    public void logMetric(final String metricName) {
        incCounter(metricName);
    }

    public void logStatsRecord(Map<String, AtomicInteger> counters, Map<String, String> statsdata, 
                               Map<String, Double> latencies, long startTimeMillis, long endTimeMillis) {
        writeStats(metadata, counters, statsdata, latencies, startTimeMillis, endTimeMillis);
    }

    private static Map<String, String> loadMetadata(String fileLocation) {
        Map<String, String> retVal = new ConcurrentHashMap<>();

        if(fileLocation != null) {
            Properties props = new Properties();
    
            try (InputStream input = new FileInputStream(
                     ESResources.INSTANCE.getPluginFileLocation() + PluginSettings.CONFIG_FILES_PATH + fileLocation); ) {
                // load properties file
                props.load(input);
            } catch(Exception ex) {
                GENERAL_LOG.error("Error in loading metadata for fileLocation: {}", fileLocation);
            }

            props.forEach((key, value) -> retVal.put((String)key, (String)value));
        }

        return retVal;
    }

    private StatsCollector(Map<String, String> metadata) {
        super(MetricsConfiguration.CONFIG_MAP.get(StatsCollector.class).samplingInterval,
            "StatsCollector");
        this.metadata = metadata;
        defaultExceptionCodes.add(StatExceptionCode.TOTAL_ERROR);
    }

    public void addDefaultExceptionCode(StatExceptionCode statExceptionCode) {
        defaultExceptionCodes.add(statExceptionCode);
    }

    @Override
    public void collectMetrics(long startTime) {
        Map<String, AtomicInteger> currentCounters = counters;
        counters = new ConcurrentHashMap<>();

        //currentCounters.putIfAbsent(StatExceptionCode.TOTAL_ERROR.toString(), new AtomicInteger(0));

        for(StatExceptionCode statExceptionCode : defaultExceptionCodes) {
            currentCounters.putIfAbsent(statExceptionCode.toString(), new AtomicInteger(0));
        }

        writeStats(metadata, currentCounters, null, null, objectCreationTime.getTime(), new Date().getTime());
        objectCreationTime = new Date();
    }
    
    private void incCounter(String counterName) {
        AtomicInteger val = counters.putIfAbsent(counterName, new AtomicInteger(1));
        if (val != null) {
            val.getAndIncrement();
        }
    }

    private void incErrorCounter() {
        AtomicInteger all_val = counters.putIfAbsent(StatExceptionCode.TOTAL_ERROR.toString(), new AtomicInteger(1));
        if (all_val != null) {
            all_val.getAndIncrement();
        }
    }

    private static void writeStats(Map<String, String> metadata, Map<String, AtomicInteger> counters,
                                   Map<String, String> statsdata, Map<String, Double> latencies,
                                   long startTimeMillis, long endTimeMillis) {
        StringBuilder builder = new StringBuilder();
        builder.append(LOG_ENTRY_INIT + LOG_LINE_BREAK);
        logValues(metadata, builder);
        logValues(statsdata, builder);
        logTimeMetrics(startTimeMillis, endTimeMillis, builder);

        Map<String, Double> tmpLatencies;

        if(latencies == null) {
            tmpLatencies = new ConcurrentHashMap<>();
        } else {
            tmpLatencies = new ConcurrentHashMap<>(latencies);
        }

        tmpLatencies.put("total-time", (double)endTimeMillis-startTimeMillis);
        addEntry("Timing", getLatencyMetrics(tmpLatencies), builder);


        addEntry("Counters", getCountersString(counters), builder);
        builder.append(LOG_ENTRY_END);// + LOG_LINE_BREAK);
        STATS_LOGGER.info(builder.toString());
    }

    private static String getCountersString(Map<String, AtomicInteger> counters) {
        StringBuilder builder = new StringBuilder();
        if (counters == null || counters.isEmpty()) {
            return "";
        }
        for (Map.Entry<String, AtomicInteger> counter : counters.entrySet()) {
            builder.append(counter.getKey()).append("=").append(counter.getValue().get()).append(",");
        }
        builder.delete(builder.length() - 1, builder.length());
        return builder.toString();
    }

    private static void logTimeMetrics(long startTimeMillis, long endTimeMillis, StringBuilder builder) {
        // Date Example: Wed, 20 Mar 2013 15:07:51 GMT
        SimpleDateFormat dateFormat = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z", Locale.ROOT);
        addEntry("StartTime", String.format(Locale.ROOT, "%.3f", startTimeMillis / MILLISECONDS_TO_SECONDS_DIVISOR), builder);
        addEntry("EndTime", dateFormat.format(new Date(endTimeMillis)), builder);
        addEntry("Time", (endTimeMillis - startTimeMillis) + " msecs", builder);
    }

    private static void logValues(Map<String, String> values, StringBuilder sb) {
        if(values == null) {
            return;
        }
        for (Map.Entry<String, String> entry : values.entrySet()) {
            addEntry(entry.getKey(), entry.getValue(), sb);
        }
    }

    private static void addEntry(String key, Object value, StringBuilder sb) {
        sb.append(key).append('=').append(value).append(LOG_LINE_BREAK);
    } 

    private static String getLatencyMetrics(Map<String, Double> values) {
        StringBuilder builder = new StringBuilder();
        if (values == null || values.isEmpty()) {
            return "";
        }
        for (Map.Entry<String, Double> value : values.entrySet()) {
            getTimingInfo(value.getKey(), value.getValue(), builder);
        }
        builder.delete(builder.length() - 1, builder.length());
        return builder.toString();
    }

    private static void getTimingInfo(String timerName, double latency, StringBuilder builder) {
        getTimingInfo(timerName, latency, builder, 1);
    }

    private static void getTimingInfo(String timerName, double latency, StringBuilder builder, int attempts) {
        builder.append(timerName).append(":").append(latency).append("/").append(attempts).append(",");
    }
}

