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


package com.amazon.opendistro.elasticsearch.performanceanalyzer.reader;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AbstractTests;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.DiskMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.HeapMetricsCollector.HeapStatus;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.MasterServiceMetrics.MasterPendingStatus;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.NodeDetailsCollector.NodeDetailsStatus;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.NodeStatsMetricsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.GCType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.NodeRole;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.MetricPropertiesTests.FailureCondition;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.jooq.Condition;
import org.jooq.impl.DSL;
import org.junit.Ignore;

@Ignore
public class AbstractReaderTests extends AbstractTests {
    protected final String DB_URL;

    protected final Connection conn;

    public AbstractReaderTests() throws SQLException, ClassNotFoundException {
        // make sure the sqlite classes and driver are loaded
        Class.forName("org.sqlite.JDBC");
        DB_URL = "jdbc:sqlite:";
        System.setProperty("java.io.tmpdir", "/tmp");
        conn = DriverManager.getConnection(DB_URL);
    }

    protected Condition getDimensionEqCondition(MetricDimension dimentionHeader,
            String dimensionName) {
        return DSL.field(dimentionHeader.toString(), String.class)
                .eq(dimensionName);
    }

    protected String createRelativePath(String... paths) {
        StringBuilder sb = new StringBuilder();
        for (String path : paths) {
            sb.append(path);
            sb.append(File.separator);
        }
        return sb.toString();
    }

    protected void write(File f, boolean append, String... input)
            throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(f, append)))) {
            for (String line : input) {
                writer.append(line);
                writer.newLine();
            }
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }

    protected String getCurrentMilliSeconds(long currentTimeMillis) {
        return new StringBuilder().append("{\"")
                .append(PerformanceAnalyzerMetrics.METRIC_CURRENT_TIME).append("\"")
                .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor)
                .append(currentTimeMillis).append("}").toString();
    }

    protected String createDiskMetrics(String name, double utilization,
            double await, double serviceRate) {
        StringBuffer value = new StringBuffer();

        value.append(new DiskMetrics(name, utilization, await, serviceRate)
                .serialize());

        return value.toString();
    }

    protected String createPendingTaskMetrics(int pendingTaskCount) {
        StringBuffer value = new StringBuffer();

        value.append(new MasterPendingStatus(pendingTaskCount)
                .serialize());

        return value.toString();
    }

    protected String createShardStatMetrics(long indexingThrottleTime,
            long queryCacheHitCount, long queryCacheMissCount,
            long queryCacheInBytes, long queryCacheMaxSizeInBytes,
            long fieldDataEvictions, long fieldDataInBytes,
            long fieldDataMaxSizeInBytes, long requestCacheHitCount,
            long requestCacheMissCount, long requestCacheEvictions,
            long requestCacheInBytes, long requestCacheMaxSizeInBytes,
            long refreshCount, long refreshTime,
            long flushCount, long flushTime, long mergeCount,
            long mergeTime, long mergeCurrent, long indexBufferBytes,
            long segmentCount, long segmentsMemory, long termsMemory,
            long storedFieldsMemory, long termVectorsMemory,
            long normsMemory, long pointsMemory, long docValuesMemory,
            long indexWriterMemory, long versionMapMemory,
            long bitsetMemory, long shardSizeInBytes, FailureCondition condition) {
        // dummyCollector is only used to create the json string
        NodeStatsMetricsCollector dummyCollector = new NodeStatsMetricsCollector(null);
        String str = (dummyCollector.new NodeStatsMetricsStatus(
                indexingThrottleTime,
                 queryCacheHitCount,
                 queryCacheMissCount,
                 queryCacheInBytes,
                 queryCacheMaxSizeInBytes,
                 fieldDataEvictions,
                 fieldDataInBytes,
                 fieldDataMaxSizeInBytes,
                 requestCacheHitCount,
                 requestCacheMissCount,
                 requestCacheEvictions,
                 requestCacheInBytes,
                 requestCacheMaxSizeInBytes,
                 refreshCount,
                 refreshTime,
                 flushCount,
                 flushTime,
                 mergeCount,
                 mergeTime,
                 mergeCurrent,
                 indexBufferBytes,
                 segmentCount,
                 segmentsMemory,
                 termsMemory,
                 storedFieldsMemory,
                 termVectorsMemory,
                 normsMemory,
                 pointsMemory,
                 docValuesMemory,
                 indexWriterMemory,
                 versionMapMemory,
                 bitsetMemory, shardSizeInBytes)).serialize();

        if (condition == FailureCondition.INVALID_JSON_METRIC) {
            str = str.substring(1);
        }
        return str;
    }

    protected String createHeapMetrics(GCType name, long committed, long init,
            long max, long used) {
        return new HeapStatus(name.toString(), committed, init, max, used)
                .serialize();
    }

    protected String createHeapMetrics(GCType name, long collectionCount,
            long collectionTime) {
        return new HeapStatus(name.toString(),
                collectionCount,
                collectionTime).serialize();
    }

    protected String createNodeDetailsMetrics(String id, String ipAddress) {
        StringBuffer value = new StringBuffer();

        value.append(new NodeDetailsStatus(id, ipAddress, NodeRole.UNKNOWN.toString(), false)
                .serialize());

        return value.toString();
    }

    static void setFinalStatic(Field field, Object newValue) throws Exception {
        field.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
        field.set(null, newValue);
    }
}
