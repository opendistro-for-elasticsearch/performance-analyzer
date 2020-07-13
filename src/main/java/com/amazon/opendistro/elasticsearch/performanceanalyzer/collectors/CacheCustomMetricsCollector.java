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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CacheCustomDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CacheCustomValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedAction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.indices.IndicesRequestCache;
import org.elasticsearch.indices.IndicesService;

/*
 * Unlike Cache Hit, Miss, Eviction Count and Size, which is tracked on a per shard basis,
 * the Cache Max size is a node-level static setting and thus, we need a custom collector
 * (other than NodeStatsMetricsCollector which collects the per shard metrics) for this
 * metric.
 *
 * CacheCustomMetricsCollector collects the max size for the Field Data and Shard Request
 * Cache currently and can be extended for remaining cache types and any other node level
 * cache metric.
 *
 */
public class CacheCustomMetricsCollector extends PerformanceAnalyzerMetricsCollector implements MetricsProcessor {
    private static final Logger LOG = LogManager.getLogger(CacheCustomMetricsCollector.class);
    public static final int SAMPLING_TIME_INTERVAL = MetricsConfiguration.CONFIG_MAP.get(
            CacheCustomMetricsCollector.class).samplingInterval;
    private static final int KEYS_PATH_LENGTH = 0;
    private StringBuilder value;

    public CacheCustomMetricsCollector() {
        super(SAMPLING_TIME_INTERVAL, "CacheCustomMetrics");
        value = new StringBuilder();
    }

    @Override
    public void collectMetrics(long startTime) {
        IndicesService indicesService = ESResources.INSTANCE.getIndicesService();
        if (indicesService == null) {
            return;
        }

        value.setLength(0);
        value.append(PerformanceAnalyzerMetrics.getJsonCurrentMilliSeconds());
        // This is for backward compatibility. Core ES may or may not emit custom shard metrics
        // (depending on whether the patch has been applied or not). Thus, we need to use reflection
        // to check whether getMaxWeight() method exist in Cache.java
        //
        // Currently, we are collecting maxWeight metrics only for FieldData and Shard Request Cache.
        LOG.info("\nMOCHI, insider the for loop for collectMetrics()!!");
        CacheMaxSizeStatus fieldDataCacheMaxSizeStatus = AccessController.doPrivileged(
                (PrivilegedAction<CacheMaxSizeStatus>) () -> {
                    try {
                        LOG.info("MOCHI, querying for max memory for Field Data Cache!!");
                        Cache fieldDataCache = indicesService.getIndicesFieldDataCache().getCache();
                        Field field = fieldDataCache.getClass().getDeclaredField("maximumWeight");
                        field.setAccessible(true);
                        long fieldDataMaxSize =  (Long) field.get(fieldDataCache);
                        LOG.info("MOCHI, fieldDataMaxSize: " + fieldDataMaxSize);
                        return new CacheMaxSizeStatus("Field Data Cache", fieldDataMaxSize);
                    } catch (Exception e) {
                        return new CacheMaxSizeStatus(null, null);
                    }
                });
        value.append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(fieldDataCacheMaxSizeStatus.serialize());

        CacheMaxSizeStatus shardRequestCacheMaxSizeStatus = AccessController.doPrivileged(
                (PrivilegedAction<CacheMaxSizeStatus>) () -> {
                    try {
                        LOG.info("MOCHI, querying for max memory for Shard Request Cache!!");
                        Field requestCacheField = indicesService.getClass().getDeclaredField("indicesRequestCache");
                        requestCacheField.setAccessible(true);
                        IndicesRequestCache requestCache = (IndicesRequestCache) requestCacheField.get(indicesService);

                        Field cacheField = requestCache.getClass().getDeclaredField("cache");
                        cacheField.setAccessible(true);
                        Cache cache = (Cache) requestCacheField.get(requestCache);

                        Field maximumWeightField = cache.getClass().getDeclaredField("maximumWeight");
                        maximumWeightField.setAccessible(true);
                        Long requestCacheMaxSize = (Long) maximumWeightField.get(cache);
                        LOG.info("MOCHI, requestCacheMaxSize: " + requestCacheMaxSize);
                        return new CacheMaxSizeStatus("Shard Request Cache", requestCacheMaxSize);
                    } catch (Exception e) {
                        return new CacheMaxSizeStatus(null, null);
                    }
                });
        value.append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor).append(shardRequestCacheMaxSizeStatus.serialize());

        saveMetricValues(value.toString(), startTime);
    }

    @Override
    public String getMetricsPath(long startTime, String... keysPath) {
        // throw exception if keys.length is not equal to 0
        if (keysPath.length != KEYS_PATH_LENGTH) {
            throw new RuntimeException("keys length should be " + KEYS_PATH_LENGTH);
        }

        return PerformanceAnalyzerMetrics.generatePath(startTime, PerformanceAnalyzerMetrics.sCacheCustomPath);
    }

    static class CacheMaxSizeStatus extends MetricStatus {

        private final String cacheType;

        @JsonInclude(JsonInclude.Include.NON_NULL)
        private final long cacheMaxSize;

        CacheMaxSizeStatus(String cacheType, Long cacheMaxSize) {
            this.cacheType = cacheType;
            this.cacheMaxSize = cacheMaxSize;
        }

        @JsonProperty(CacheCustomDimension.Constants.TYPE_VALUE)
        public String getCacheType() {
            return cacheType;
        }

        @JsonProperty(CacheCustomValue.Constants.MAX_SIZE)
        public long getCacheMaxSize() {
            return cacheMaxSize;
        }
    }
}
