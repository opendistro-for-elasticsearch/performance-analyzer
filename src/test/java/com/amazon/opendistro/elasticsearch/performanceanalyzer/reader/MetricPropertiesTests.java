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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.GCType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.HeapDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.HeapValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.MetricName;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ShardStatsDerivedDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ShardStatsValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;

public class MetricPropertiesTests extends AbstractReaderTests {

    public MetricPropertiesTests() throws SQLException, ClassNotFoundException {
        super();
    }

    @Test
    public void testHeap() throws Exception {
        long currTimestamp = System.currentTimeMillis() + 4000;

        long currTimeBucket = PerformanceAnalyzerMetrics.getTimeInterval(currTimestamp);
        String currentTimeBucketStr = String.valueOf(currTimeBucket);
        temporaryFolder.newFolder(currentTimeBucketStr);
        File output = temporaryFolder.newFile(createRelativePath(
                currentTimeBucketStr, PerformanceAnalyzerMetrics.sHeapPath));

        write(output, false,
                PerformanceAnalyzerMetrics.getJsonCurrentMilliSeconds(),
                createHeapMetrics(GCType.TOT_YOUNG_GC, 0, 0),
                createHeapMetrics(GCType.TOT_FULL_GC, 0, 0),
                createHeapMetrics(GCType.SURVIVOR, 69730304, 69730304, 69730304,
                        6343808),
                createHeapMetrics(GCType.PERM_GEN, 144998400, 0, -1, 137707592),
                createHeapMetrics(GCType.OLD_GEN, 32051232768L, 32051232768L,
                        32051232768L, 650876744),
                createHeapMetrics(GCType.EDEN, 558432256, 558432256, 558432256,
                        367013104),
                createHeapMetrics(GCType.NON_HEAP, 259436544, 2555904, -1,
                        249276784),
                createHeapMetrics(GCType.HEAP, 32679395328L, 32749125632L,
                        32679395328L, 1024254960L));

        long lastSnapTimestamp = System.currentTimeMillis() - 1000;

        MetricProperties heapProperty = MetricPropertiesConfig.getInstance()
                .getProperty(MetricName.HEAP_METRICS);

        heapProperty.getHandler().setRootLocation(
                temporaryFolder.getRoot().getCanonicalPath() + File.separator);

        MemoryDBSnapshot heapSnap = new MemoryDBSnapshot(this.conn,
                MetricName.HEAP_METRICS, currTimestamp);

        boolean res = heapProperty.dispatch(heapSnap,
                currTimestamp, lastSnapTimestamp);

        assertTrue(res);

        assertTrue (GCType.values().length == heapSnap.fetchAll().size());

        @SuppressWarnings("unchecked")
        Field<Double>[] fields = new Field[6];

        fields[0] = DSL.field(HeapValue.GC_COLLECTION_EVENT.toString(),
                Double.class);
        fields[1] =
                DSL.field(HeapValue.GC_COLLECTION_TIME.toString(), Double.class);
        fields[2] = DSL.field(HeapValue.HEAP_COMMITTED.toString(),
                Double.class);
        fields[3] = DSL.field(HeapValue.HEAP_INIT.toString(),
                Double.class);
        fields[4] = DSL.field(HeapValue.HEAP_MAX.toString(),
                Double.class);
        fields[5] = DSL.field(HeapValue.HEAP_USED.toString(),
                Double.class);

        Result<Record> resRecord = heapSnap.fetchMetric(
                getDimensionEqCondition(
                        HeapDimension.MEM_TYPE,
                        GCType.TOT_YOUNG_GC.toString()),
                fields);

        Record record0 = resRecord.get(0);
        Double collectionCount = Double.parseDouble(
                record0.get(fields[0]).toString());
        assertEquals(0, collectionCount, 0.001);

        Double collectionTime = Double.parseDouble(
                record0.get(fields[1]).toString());
        assertEquals(0, collectionTime, 0.001);

        for (int i=2; i<6; i++) {
            assertEquals(-2, record0.get(fields[i]), 0.001);
        }
    }

    private String createShardStatMetrics(long indexingThrottleTime,
            long queryCacheHitCount, long queryCacheMissCount,
            long queryCacheInBytes, long fieldDataEvictions,
            long fieldDataInBytes, long requestCacheHitCount,
            long requestCacheMissCount, long requestCacheEvictions,
            long requestCacheInBytes, long refreshCount, long refreshTime,
            long flushCount, long flushTime, long mergeCount,
            long mergeTime, long mergeCurrent, long indexBufferBytes,
            long segmentCount, long segmentsMemory, long termsMemory,
            long storedFieldsMemory, long termVectorsMemory,
            long normsMemory, long pointsMemory, long docValuesMemory,
            long indexWriterMemory, long versionMapMemory,
            long bitsetMemory) {
        return createShardStatMetrics(
                indexingThrottleTime,
                queryCacheHitCount, queryCacheMissCount,
                queryCacheInBytes, fieldDataEvictions,
                fieldDataInBytes, requestCacheHitCount,
                requestCacheMissCount, requestCacheEvictions,
                requestCacheInBytes, refreshCount, refreshTime,
                flushCount, flushTime, mergeCount,
                mergeTime, mergeCurrent, indexBufferBytes,
                segmentCount, segmentsMemory, termsMemory,
                storedFieldsMemory, termVectorsMemory,
                normsMemory, pointsMemory, docValuesMemory,
                indexWriterMemory, versionMapMemory,
                bitsetMemory,
                FailureCondition.NONE);
    }

    @Test
    public void testShardStat() throws Exception {
        long currTimestamp = System.currentTimeMillis() + 4000;

        long currTimeBucket = PerformanceAnalyzerMetrics.getTimeInterval(currTimestamp);
        String currentTimeBucketStr = String.valueOf(currTimeBucket);

        String moviesIndex = "movies-2013";
        String taxisIndex = "nyc_taxis";
        temporaryFolder.newFolder(currentTimeBucketStr,
                PerformanceAnalyzerMetrics.sIndicesPath, moviesIndex);
        temporaryFolder.newFolder(currentTimeBucketStr,
                PerformanceAnalyzerMetrics.sIndicesPath, taxisIndex);
        String indicesRelativePath = createRelativePath(currentTimeBucketStr,
                PerformanceAnalyzerMetrics.sIndicesPath);
        String moviesIndexeRelativePath = createRelativePath(
                indicesRelativePath, moviesIndex);
        String taxisIndexeRelativePath = createRelativePath(indicesRelativePath,
                taxisIndex);

        File moviesShard0 = temporaryFolder
                .newFile(createRelativePath(moviesIndexeRelativePath, "0"));
        File moviesShard1 = temporaryFolder
                .newFile(createRelativePath(moviesIndexeRelativePath, "1"));

        write(moviesShard0, false, PerformanceAnalyzerMetrics.getJsonCurrentMilliSeconds(),
                createShardStatMetrics(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                        0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 2L, 8145L, 6565L,
                        672L, 0L, 384L, 28L, 496L, 0L, 0L, 0L));

        write(moviesShard1, false, PerformanceAnalyzerMetrics.getJsonCurrentMilliSeconds(),
                createShardStatMetrics(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                        0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 2L, 8019L, 6445L,
                        664L, 0L, 384L, 6L, 520L, 0L, 0L, 0L));

        File taxisShard0 = temporaryFolder
                .newFile(createRelativePath(taxisIndexeRelativePath, "0"));

        write(taxisShard0, false, PerformanceAnalyzerMetrics.getJsonCurrentMilliSeconds(),
                createShardStatMetrics(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                        0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                        0L, 0L, 0L, 0L, 0L, 0L, 0L));

        long lastSnapTimestamp = System.currentTimeMillis() - 1000;

        MetricProperties shardStatProperty = MetricPropertiesConfig.getInstance()
                .getProperty(MetricName.SHARD_STATS);
        MetricPropertiesConfig.ShardStatFileHandler handler = new MetricPropertiesConfig.ShardStatFileHandler();
        MetricPropertiesConfig.ShardStatFileHandler spyHandler = Mockito.spy(handler);
        shardStatProperty.setHandler(spyHandler);

        Mockito.doReturn(temporaryFolder.getRoot().getCanonicalPath())
                .when(spyHandler).getRootLocation();

        MemoryDBSnapshot shardStatSnap = new MemoryDBSnapshot(this.conn,
                MetricName.SHARD_STATS, currTimestamp);

        boolean res = shardStatProperty.dispatch(shardStatSnap,
                currTimestamp, lastSnapTimestamp);

        assertTrue(res);

        assertTrue (3 == shardStatSnap.fetchAll().size());

        @SuppressWarnings("unchecked")
        Field<Double>[] fields = new Field[6];

        fields[0] = DSL.field(ShardStatsValue.SEGMENTS_TOTAL.toString(),
                Double.class);
        fields[1] = DSL.field(ShardStatsValue.SEGMENTS_MEMORY.toString(),
                Double.class);
        fields[2] = DSL.field(ShardStatsValue.TERM_VECTOR_MEMORY.toString(),
                Double.class);
        fields[3] = DSL.field(ShardStatsValue.CACHE_FIELDDATA_EVICTION.toString(),
                Double.class);
        fields[4] = DSL.field(ShardStatsValue.DOC_VALUES_MEMORY.toString(),
                Double.class);
        fields[5] = DSL.field(ShardStatsValue.MERGE_EVENT.toString(),
                Double.class);

        Result<Record> resRecord = shardStatSnap.fetchMetric(
                getDimensionEqCondition(
                        ShardStatsDerivedDimension.INDEX_NAME,
                        moviesIndex),
                fields);

        assertTrue(2 == resRecord.size());

        Result<Record> resRecord2 = shardStatSnap.fetchMetric(
                getDimensionEqCondition(ShardStatsDerivedDimension.INDEX_NAME,
                        moviesIndex)
                                .and(getDimensionEqCondition(
                                        ShardStatsDerivedDimension.SHARD_ID,
                                        "0")),
                fields);

        Record record0 = resRecord2.get(0);
        Double segmentCount = Double.parseDouble(
                record0.get(fields[0]).toString());
        assertEquals(2, segmentCount, 0.001);

        Double segmentMemory = Double.parseDouble(
                record0.get(fields[1]).toString());
        assertEquals(8145, segmentMemory, 0.001);

        Double termVectorMemory = Double.parseDouble(
                record0.get(fields[2]).toString());
        assertEquals(0, termVectorMemory, 0.001);

        Double fieldDataEviction = Double.parseDouble(
                record0.get(fields[3]).toString());
        assertEquals(0, fieldDataEviction, 0.001);

        Double docValuesMemory = Double.parseDouble(
                record0.get(fields[4]).toString());
        assertEquals(496, docValuesMemory, 0.001);

        Double mergeCount = Double.parseDouble(
                record0.get(fields[5]).toString());
        assertEquals(0, mergeCount, 0.001);
    }

    enum FailureCondition {
        BAD_FILE_NAME, EMPTY_FILE, MODIFIED_AFTER_START, ALREADY_PROCESSED,
        INVALID_JSON_TIME, INVALID_JSON_METRIC, NONE
    }

    public String getCurrentNonJsonMilliSeconds() {
        return new StringBuilder()
                .append(PerformanceAnalyzerMetrics.METRIC_CURRENT_TIME).append("\"")
                .append(PerformanceAnalyzerMetrics.sKeyValueDelimitor)
                .append(System.currentTimeMillis()).append("}").toString();
    }

    private boolean createFailureScenario(FailureCondition condition)
            throws Exception {
        long currTimestamp = 0;
        // we don't process files modified after currTimestamp
        if (condition == FailureCondition.MODIFIED_AFTER_START) {
            currTimestamp = System.currentTimeMillis() - 1000;
        } else {
            currTimestamp = System.currentTimeMillis() + 4000;
        }

        long currTimeBucket = PerformanceAnalyzerMetrics.getTimeInterval(currTimestamp);
        String currentTimeBucketStr = String.valueOf(currTimeBucket);

        String moviesIndex = "movies-2013";
        temporaryFolder.newFolder(currentTimeBucketStr,
                PerformanceAnalyzerMetrics.sIndicesPath, moviesIndex);

        String indicesRelativePath = createRelativePath(currentTimeBucketStr,
                PerformanceAnalyzerMetrics.sIndicesPath);
        String moviesIndexeRelativePath = createRelativePath(
                indicesRelativePath, moviesIndex);

        File moviesShard0 = null;
        if (condition == FailureCondition.BAD_FILE_NAME) {
            // this metric file is not numeric
            moviesShard0 = temporaryFolder
                    .newFile(createRelativePath(moviesIndexeRelativePath, "X"));
        } else {
            moviesShard0 = temporaryFolder
                    .newFile(createRelativePath(moviesIndexeRelativePath, "0"));
        }

        if (condition != FailureCondition.EMPTY_FILE) {
            if (condition == FailureCondition.INVALID_JSON_TIME) {
                write(moviesShard0, false, getCurrentNonJsonMilliSeconds(),
                        createShardStatMetrics(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                                0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 2L,
                                8145L, 6565L, 672L, 0L, 384L, 28L, 496L, 0L, 0L,
                                0L));
            } else if (condition == FailureCondition.INVALID_JSON_METRIC) {
                write(moviesShard0, false, PerformanceAnalyzerMetrics.getJsonCurrentMilliSeconds(),
                        createShardStatMetrics(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                                0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 2L,
                                8145L, 6565L, 672L, 0L, 384L, 28L, 496L, 0L, 0L,
                                0L, condition));
            }
            else {
                write(moviesShard0, false, PerformanceAnalyzerMetrics.getJsonCurrentMilliSeconds(),
                        createShardStatMetrics(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                                0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 2L,
                                8145L, 6565L, 672L, 0L, 384L, 28L, 496L, 0L, 0L,
                                0L));
            }
        }

        long lastSnapTimestamp = 0;
        if (condition == FailureCondition.ALREADY_PROCESSED) {
            // we don't process files if this file has already been processed
            // previously
            lastSnapTimestamp = System.currentTimeMillis();
        } else {
            lastSnapTimestamp = System.currentTimeMillis() - 1000;
        }


        MetricProperties shardStatProperty = MetricPropertiesConfig
                .getInstance().getProperty(MetricName.SHARD_STATS);
        MetricPropertiesConfig.ShardStatFileHandler handler = new MetricPropertiesConfig.ShardStatFileHandler();
        MetricPropertiesConfig.ShardStatFileHandler spyHandler = Mockito.spy(handler);
        shardStatProperty.setHandler(spyHandler);

        Mockito.doReturn(temporaryFolder.getRoot().getCanonicalPath())
                .when(spyHandler).getRootLocation();

        MemoryDBSnapshot shardStatSnap = new MemoryDBSnapshot(this.conn,
                MetricName.SHARD_STATS, currTimestamp);

        // no metrics parsed
        return shardStatProperty.dispatch(shardStatSnap,
                currTimestamp, lastSnapTimestamp);
    }

    @Test(expected = IOException.class)
    public void testBadPathPattern() throws Exception {
        createFailureScenario(FailureCondition.BAD_FILE_NAME);
    }

    @Test
    public void testDefaultRootLocation() {
        assertEquals(PerformanceAnalyzerMetrics.sDevShmLocation,
                MetricPropertiesConfig
                        .createFileHandler(PerformanceAnalyzerMetrics.sCircuitBreakerPath)
                        .getRootLocation());
    }

    @Test
    public void testEmptyFile() throws Exception {
        assertTrue(!createFailureScenario(FailureCondition.EMPTY_FILE));
    }

    @Test
    public void testModifiedAfterStart() throws Exception {
        assertTrue(
                !createFailureScenario(FailureCondition.MODIFIED_AFTER_START));
    }

    @Test
    public void testAlreadyProcessed() throws Exception {
        assertTrue(!createFailureScenario(FailureCondition.ALREADY_PROCESSED));
    }

    @Test
    public void testInvalidJsonTime() throws Exception {
        assertTrue(!createFailureScenario(FailureCondition.INVALID_JSON_TIME));
    }

    @Test
    public void testInvalidJsonMetric() throws Exception {
        assertTrue(!createFailureScenario(FailureCondition.INVALID_JSON_METRIC));
    }
}
