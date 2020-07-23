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

import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.CacheConfigMetricsCollector.CacheMaxSizeStatus;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.CircuitBreakerCollector.CircuitBreakerStatus;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.HeapMetricsCollector.HeapStatus;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.MasterServiceMetrics.MasterPendingStatus;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.ThreadPoolMetricsCollector.ThreadPoolStatus;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CircuitBreakerDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CircuitBreakerValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CacheConfigDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CacheConfigValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.DiskDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.DiskValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.HeapDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.HeapValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.IPDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.IPValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.MasterPendingValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.TCPDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.TCPValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ThreadPoolDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ThreadPoolValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricValue;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.junit.Test;

/**
 * Writer serialize a java bean to a /dev/shm/performanceanalyzer file using a collector's
 * instance field names or @jsonproperty annotation on getter method as json key
 * names. Reader extracts values using enum names in AllMetrics. The tests make
 * sure the field names and enum names match. If you see test errors here, it
 * means somebody changes either the field name or enum names and forget to sync
 * them.
 *
 */
public class JsonKeyTests {
    Function<Method, String> getMethodJsonProperty = f -> {
        if (!f.getName().startsWith("get")) {
            return null;
        } else if (f.isAnnotationPresent(JsonProperty.class)) {
            return f.getAnnotation(JsonProperty.class).value();
        } else {
            return null;
        }
    };

    // For some fields we use abbreviation as the json key but use longer
    // words as the field names to save memory and disk space.
    Function<Field, String> fieldToString = f -> {
        if (f.isAnnotationPresent(JsonIgnore.class) || f.isSynthetic()) {
            return null;
        } else {
            return f.getName();
        }
    };

    @Test
    public void testJsonKeyNames() throws NoSuchFieldException,
    SecurityException {
        verifyMethodWithJsonKeyNames(CacheMaxSizeStatus.class,
                CacheConfigDimension.values(),
                CacheConfigValue.values(),
                getMethodJsonProperty);
        verifyMethodWithJsonKeyNames(CircuitBreakerStatus.class,
                CircuitBreakerDimension.values(),
                CircuitBreakerValue.values(),
                getMethodJsonProperty);
        verifyMethodWithJsonKeyNames(HeapStatus.class,
                HeapDimension.values(),
                HeapValue.values(),
                getMethodJsonProperty);
        verifyMethodWithJsonKeyNames(DiskMetrics.class, DiskDimension.values(),
                DiskValue.values(), getMethodJsonProperty);
        verifyMethodWithJsonKeyNames(TCPStatus.class, TCPDimension.values(),
                TCPValue.values(), getMethodJsonProperty);
        verifyMethodWithJsonKeyNames(NetInterfaceSummary.class,
                IPDimension.values(), IPValue.values(), getMethodJsonProperty);
        verifyMethodWithJsonKeyNames(ThreadPoolStatus.class,
                ThreadPoolDimension.values(), ThreadPoolValue.values(),
                getMethodJsonProperty);
        verifyMethodWithJsonKeyNames(MasterPendingStatus.class,
                new MetricDimension[] {},
                MasterPendingValue.values(),
                getMethodJsonProperty);
        verifyNodeDetailJsonKeyNames();
    }

    private void verifyMethodWithJsonKeyNames(
            Class<? extends MetricStatus> statusBean,
            MetricDimension[] dimensions, MetricValue[] metrics,
            Function<Method, String> toJsonKey) {
        Set<String> jsonKeySet = new HashSet<>();
        Method[] methods = statusBean.getDeclaredMethods();

        for (Method method : methods) {
            String annotationValue = toJsonKey.apply(method);
            if (annotationValue != null) {
                jsonKeySet.add(annotationValue);
            }
        }

        assertTrue(dimensions.length + metrics.length == jsonKeySet.size());

        for (MetricDimension d : dimensions) {
            assertTrue(String.format("We need %s", d.toString()),
                    jsonKeySet.contains(d.toString()));
        }

        for (MetricValue v : metrics) {
            assertTrue(String.format("We need %s", v.toString()),
                    jsonKeySet.contains(v.toString()));
        }
    }

    private void verifyFieldWithJsonKeyNames(
            Class<? extends MetricStatus> statusBean,
            MetricDimension[] dimensions, MetricValue[] metrics,
            Function<Field, String> toJsonKey) {
        Set<String> jsonKeySet = new HashSet<>();
        Field[] fields = statusBean.getDeclaredFields();

        for (Field field : fields) {
            String annotationValue = toJsonKey.apply(field);
            if (annotationValue != null) {
                jsonKeySet.add(annotationValue);
            }
        }

        assertTrue(dimensions.length + metrics.length == jsonKeySet.size());

        for (MetricDimension d : dimensions) {
            assertTrue(String.format("We need %s", d.toString()),
                    jsonKeySet.contains(d.toString()));
        }

        for (MetricValue v : metrics) {
            assertTrue(String.format("We need %s", v.toString()),
                    jsonKeySet.contains(v.toString()));
        }
    }

    private void verifyNodeDetailJsonKeyNames() {
        Set<String> jsonKeySet = new HashSet<>();
        Set<String> nodeDetailColumnSet = new HashSet<>();
        Method[] methods = NodeDetailsCollector.NodeDetailsStatus.class.getDeclaredMethods();

        for (Method method : methods) {
            String annotationValue = getMethodJsonProperty.apply(method);
            if (annotationValue != null) {
                jsonKeySet.add(annotationValue);
            }
        }

        AllMetrics.NodeDetailColumns[] columns = AllMetrics.NodeDetailColumns.values();
        for (AllMetrics.NodeDetailColumns d : columns) {
            nodeDetailColumnSet.add(d.toString());
        }

        // The _cat/master fix might not be backport to all PA versions in brazil
        // So not all domains has the IS_MASTER_NODE field in NodeDetailsStatus
        // change this assert statement to support backward compatibility
        assertTrue(nodeDetailColumnSet.size() == jsonKeySet.size()
            || nodeDetailColumnSet.size() - 1 == jsonKeySet.size());

        for (String key : jsonKeySet) {
            assertTrue(String.format("We need %s", key),
                nodeDetailColumnSet.contains(key));
        }
    }
}
