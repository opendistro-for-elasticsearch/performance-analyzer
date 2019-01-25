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


package com.amazon.opendistro.performanceanalyzer.collectors;

import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import org.junit.Test;

import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.performanceanalyzer.metrics.MetricDimension;
import com.amazon.opendistro.performanceanalyzer.metrics.MetricValue;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Writer serialize a java bean to a /dev/shm/performanceanalyzer file using a collector's
 * instance field names or @jsonproperty annotation on getter method as json key
 * names. Reader extracts values using enum names in AllMetrics. The tests make
 * sure the field names and enum names match. If you see test errors here, it
 * means somebody changes either the field name or enum names and forget to sync
 * them.
 *
 */
public class JsonKeyTest {
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

        verifyFieldWithJsonKeyNames(CircuitBreakerCollector.CircuitBreakerStatus.class,
                AllMetrics.CircuitBreakerDimension.values(),
                AllMetrics.CircuitBreakerValue.values(),
                fieldToString);
        verifyFieldWithJsonKeyNames(HeapMetricsCollector.HeapStatus.class,
                AllMetrics.HeapDimension.values(),
                AllMetrics.HeapValue.values(),
                fieldToString);
        verifyMethodWithJsonKeyNames(DiskMetrics.class, AllMetrics.DiskDimension.values(),
                AllMetrics.DiskValue.values(), getMethodJsonProperty);
        verifyMethodWithJsonKeyNames(TCPStatus.class, AllMetrics.TCPDimension.values(),
                AllMetrics.TCPValue.values(), getMethodJsonProperty);
        verifyFieldWithJsonKeyNames(NetInterfaceSummary.class,
                AllMetrics.IPDimension.values(), AllMetrics.IPValue.values(), fieldToString);
        verifyFieldWithJsonKeyNames(ThreadPoolMetricsCollector.ThreadPoolStatus.class,
                AllMetrics.ThreadPoolDimension.values(), AllMetrics.ThreadPoolValue.values(),
                fieldToString);
        verifyFieldWithJsonKeyNames(NodeStatsMetricsCollector.NodeStatsMetricsStatus.class,
                new MetricDimension[] {}, AllMetrics.ShardStatsValue.values(),
                fieldToString);
        verifyFieldWithJsonKeyNames(MasterServiceMetrics.MasterPendingStatus.class,
                new MetricDimension[] {},
                AllMetrics.MasterPendingValue.values(),
                fieldToString);
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
        Method[] methods = NodeDetailsCollector.NodeDetailsStatus.class.getDeclaredMethods();

        for (Method method : methods) {
            String annotationValue = getMethodJsonProperty.apply(method);
            if (annotationValue != null) {
                jsonKeySet.add(annotationValue);
            }
        }

        AllMetrics.NodeDetailColumns[] columns = AllMetrics.NodeDetailColumns.values();
        assertTrue(columns.length == jsonKeySet.size());

        for (AllMetrics.NodeDetailColumns d : columns) {
            assertTrue(String.format("We need %s", d.toString()),
                    jsonKeySet.contains(d.toString()));
        }
    }
}
