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

package com.amazon.opendistro.performanceanalyzer.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.BatchBindStep;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SelectField;
import org.jooq.SelectHavingStep;
import org.jooq.impl.DSL;

import com.amazon.opendistro.performanceanalyzer.DBUtils;
import com.amazon.opendistro.performanceanalyzer.config.TroubleshootingConfig;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.CommonMetric;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.OSMetrics;
import com.amazon.opendistro.performanceanalyzer.metricsdb.Dimensions;
import com.amazon.opendistro.performanceanalyzer.metricsdb.Metric;
import com.amazon.opendistro.performanceanalyzer.metricsdb.MetricsDB;

@SuppressWarnings("serial")
public class MetricsEmitter {

    private static final Logger LOG = LogManager.getLogger(MetricsEmitter.class);

    private static final Pattern GC_PATTERN = Pattern.compile(".*(GC|CMS|Parallel).*");
    private static final Pattern REFRESH_PATTERN = Pattern.compile(".*elasticsearch.*\\[refresh\\].*");
    private static final Pattern MANAGEMENT_PATTERN = Pattern.compile(".*elasticsearch.*\\[management\\].*");
    private static final Pattern MERGE_PATTERN = Pattern.compile(".*elasticsearch\\[.*\\]\\[\\[(.*)\\]\\[(.*)\\].*Lucene Merge.*");
    private static final Pattern SEARCH_PATTERN = Pattern.compile(".*elasticsearch.*\\[search\\].*");
    private static final Pattern BULK_PATTERN = Pattern.compile(".*elasticsearch.*\\[bulk\\].*");
    //ES 6.4 onwards uses write threadpool.
    private static final Pattern WRITE_PATTERN = Pattern.compile(".*elasticsearch.*\\[write\\].*");
    //Pattern otherPattern = Pattern.compile(".*(elasticsearch).*");
    private static final Pattern HTTP_SERVER_PATTERN = Pattern.compile(".*elasticsearch.*\\[http_server_worker\\].*");
    private static final Pattern TRANS_SERVER_PATTERN = Pattern.compile(".*elasticsearch.*\\[transport_server_worker.*");
    private static final Pattern TRANS_CLIENT_PATTERN = Pattern.compile(".*elasticsearch.*\\[transport_client_boss\\].*");


    public static void emitAggregatedOSMetrics(final DSLContext create,
        final MetricsDB db, final OSMetricsSnapshot osMetricsSnap,
        final ShardRequestMetricsSnapshot rqMetricsSnap) throws Exception {

        SelectHavingStep<Record> rqTable = rqMetricsSnap.fetchThreadUtilizationRatioTable();
        SelectHavingStep<Record> osTable = osMetricsSnap.selectAll();

        List<SelectField<?>> fields = new ArrayList<SelectField<?>>() { {
            this.add(DSL.field(DSL.name(ShardRequestMetricsSnapshot.Fields.SHARD_ID.toString()), String.class));
            this.add(DSL.field(DSL.name(ShardRequestMetricsSnapshot.Fields.INDEX_NAME.toString()), String.class));
            this.add(DSL.field(DSL.name(ShardRequestMetricsSnapshot.Fields.OPERATION.toString()), String.class));
            this.add(DSL.field(DSL.name(ShardRequestMetricsSnapshot.Fields.SHARD_ROLE.toString()), String.class));
        } };

        for (OSMetrics metric: OSMetrics.values()) {
            fields.add(DSL.field(ShardRequestMetricsSnapshot.Fields.TUTIL.toString(), Double.class).mul(
                            DSL.field(DSL.name(metric.toString()), Double.class))
                      .as(metric.toString()));
        }

        ArrayList<Field<?>> groupByFields = new ArrayList<Field<?>>() { {
            this.add(DSL.field(DSL.name(ShardRequestMetricsSnapshot.Fields.SHARD_ID.toString()), String.class));
            this.add(DSL.field(DSL.name(ShardRequestMetricsSnapshot.Fields.INDEX_NAME.toString()), String.class));
            this.add(DSL.field(DSL.name(ShardRequestMetricsSnapshot.Fields.OPERATION.toString()), String.class));
            this.add(DSL.field(DSL.name(ShardRequestMetricsSnapshot.Fields.SHARD_ROLE.toString()), String.class));
        } };

        List<SelectField<?>> aggFields = new ArrayList<SelectField<?>>() { {
            this.add(DSL.field(DSL.name(ShardRequestMetricsSnapshot.Fields.SHARD_ID.toString()), String.class));
            this.add(DSL.field(DSL.name(ShardRequestMetricsSnapshot.Fields.INDEX_NAME.toString()), String.class));
            this.add(DSL.field(DSL.name(ShardRequestMetricsSnapshot.Fields.OPERATION.toString()), String.class));
            this.add(DSL.field(DSL.name(ShardRequestMetricsSnapshot.Fields.SHARD_ROLE.toString()), String.class));
        } };

        for (OSMetrics metric: OSMetrics.values()) {
            aggFields.add(DSL.sum(DSL.field(DSL.name(metric.toString()), Double.class))
                      .as(MetricsDB.SUM + "_" + metric.toString()));
            aggFields.add(DSL.avg(DSL.field(DSL.name(metric.toString()), Double.class))
                      .as(MetricsDB.AVG + "_" + metric.toString()));
            aggFields.add(DSL.min(DSL.field(DSL.name(metric.toString()), Double.class))
                      .as(MetricsDB.MIN + "_" + metric.toString()));
            aggFields.add(DSL.max(DSL.field(DSL.name(metric.toString()), Double.class))
                      .as(MetricsDB.MAX + "_" + metric.toString()));
        }

        long mCurrT = System.currentTimeMillis();
        Result<Record> res = create.select(aggFields)
            .from(
            create.select(fields)
            .from(rqTable)
            .join(osTable)
            .on(osTable.field(OSMetricsSnapshot.Fields.tid.toString(), String.class).eq(
                        rqTable.field(OSMetricsSnapshot.Fields.tid.toString(), String.class)))
            )
            .groupBy(groupByFields)
            .fetch();
        long mFinalT = System.currentTimeMillis();
        LOG.info("Total time taken for tid corelation: {}", mFinalT - mCurrT);
        checkInvalidData(rqTable, osTable, create);

        Set<String> metricColumns = osMetricsSnap.getMetricColumns();

        mCurrT = System.currentTimeMillis();
        for (String metricColumn: metricColumns) {
            List<String> dims = new ArrayList<String>() { {
            this.add(AllMetrics.CommonDimension.SHARD_ID.toString());
            this.add(AllMetrics.CommonDimension.INDEX_NAME.toString());
            this.add(AllMetrics.CommonDimension.OPERATION.toString());
            this.add(AllMetrics.CommonDimension.SHARD_ROLE.toString());
            } };
            db.createMetric(new Metric<Double>(metricColumn, 0d),
                            dims);
            BatchBindStep handle = db.startBatchPut(new Metric<Double>(metricColumn, 0d),
                                                    dims);
            for (Record r: res) {
                if (r.get(MetricsDB.SUM + "_" + metricColumn) == null) {
                    continue;
                }

                Double sumMetric = Double.parseDouble(r.get(MetricsDB.SUM + "_" + metricColumn).toString());
                Double avgMetric = Double.parseDouble(r.get(MetricsDB.AVG + "_" + metricColumn).toString());
                Double minMetric = Double.parseDouble(r.get(MetricsDB.MIN + "_" + metricColumn).toString());
                Double maxMetric = Double.parseDouble(r.get(MetricsDB.MAX + "_" + metricColumn).toString());
                handle.bind(r.get(ShardRequestMetricsSnapshot.Fields.SHARD_ID.toString()).toString(),
                        r.get(ShardRequestMetricsSnapshot.Fields.INDEX_NAME.toString()).toString(),
                        r.get(ShardRequestMetricsSnapshot.Fields.OPERATION.toString()).toString(),
                        r.get(ShardRequestMetricsSnapshot.Fields.SHARD_ROLE.toString()).toString(),
                        sumMetric, avgMetric, minMetric, maxMetric);
            }

            if (handle.size() > 0) {
                handle.execute();
            }
        }
        mFinalT = System.currentTimeMillis();
        LOG.info("Total time taken for writing resource metrics metricsdb: {}", mFinalT - mCurrT);
    }

    /**
     * Check if there is any invalid data.
     * Invalid data is if we have tid in request table but not in OS tables.
     * @param rqTable request table select
     * @param osTable OS table select
     * @param create db connection
     */
    @SuppressWarnings("unchecked")
    private static void checkInvalidData(SelectHavingStep<Record> rqTable, SelectHavingStep<Record> osTable,
                                         final DSLContext create) {
        if (!TroubleshootingConfig.getEnableDevAssert()) {
            return;
        }

        Condition condition = DSL.trueCondition();
        Field tidField = DSL.field(DSL.name(OSMetricsSnapshot.Fields.tid.toString()), String.class);
        Field tNameField = DSL.field(DSL.name(OSMetricsSnapshot.Fields.tName.toString()), String.class);

        Set<String> rqSet = DBUtils.getRecordSetByField(rqTable, tidField, condition, create);
        condition = tNameField.contains("[bulk]").or(tNameField.contains("[search]"));
        Set<String> osSet = DBUtils.getRecordSetByField(osTable, tidField, condition, create);

        if (!osSet.containsAll(rqSet)) {
            String msg = String.format("[Invalid Data] Unmatched tid between %s and %s", rqSet.toString(), osSet.toString());
            LOG.error(msg);
            LOG.error(create.select().from(rqTable).fetch().toString());
            LOG.error(create.select().from(osTable).where(condition).fetch().toString());
            throw new RuntimeException(msg);
        }
    }

    public static void emitWorkloadMetrics(final DSLContext create, final MetricsDB db,
            final ShardRequestMetricsSnapshot rqMetricsSnap) throws Exception {
        long mCurrT = System.currentTimeMillis();
        Result<Record> res = rqMetricsSnap.fetchLatencyByOp();
        List<String> dims = new ArrayList<String>() { {
            this.add(ShardRequestMetricsSnapshot.Fields.OPERATION.toString());
            this.add(HttpRequestMetricsSnapshot.Fields.EXCEPTION.toString());
            this.add(HttpRequestMetricsSnapshot.Fields.INDICES.toString());
            this.add(HttpRequestMetricsSnapshot.Fields.HTTP_RESP_CODE.toString());
            this.add(ShardRequestMetricsSnapshot.Fields.SHARD_ID.toString());
            this.add(ShardRequestMetricsSnapshot.Fields.INDEX_NAME.toString());
            this.add(ShardRequestMetricsSnapshot.Fields.SHARD_ROLE.toString());
            } };

        db.createMetric(new Metric<Double>(CommonMetric.LATENCY.toString(), 0d),
                        dims);
        BatchBindStep handle = db.startBatchPut(new Metric<Double>(
                CommonMetric.LATENCY.toString(), 0d), dims);


        for (Record r: res) {
            Double sumLatency = Double.parseDouble(r.get(DBUtils.
                        getAggFieldName(ShardRequestMetricsSnapshot.Fields.LAT.toString(), MetricsDB.SUM))
                        .toString());
            Double avgLatency = Double.parseDouble(r.get(DBUtils.
                        getAggFieldName(ShardRequestMetricsSnapshot.Fields.LAT.toString(), MetricsDB.AVG))
                        .toString());
            Double minLatency = Double.parseDouble(r.get(DBUtils.
                        getAggFieldName(ShardRequestMetricsSnapshot.Fields.LAT.toString(), MetricsDB.MIN))
                        .toString());
            Double maxLatency = Double.parseDouble(r.get(DBUtils.
                        getAggFieldName(ShardRequestMetricsSnapshot.Fields.LAT.toString(), MetricsDB.MAX))
                        .toString());

            handle.bind(r.get(ShardRequestMetricsSnapshot.Fields.OPERATION.toString()).toString(),
                        null,
                        null,
                        null,
                        r.get(ShardRequestMetricsSnapshot.Fields.SHARD_ID.toString()).toString(),
                        r.get(ShardRequestMetricsSnapshot.Fields.INDEX_NAME.toString()).toString(),
                        r.get(ShardRequestMetricsSnapshot.Fields.SHARD_ROLE.toString()).toString(),
                        sumLatency,
                        avgLatency,
                        minLatency,
                        maxLatency
                    );
        }
        long mFinalT = System.currentTimeMillis();
        LOG.info("Total time taken for writing workload metrics metricsdb: {}", mFinalT - mCurrT);
    }

    public static void emitThreadNameMetrics(final DSLContext create, final MetricsDB db,
            final OSMetricsSnapshot osMetricsSnap) throws Exception {
        long mCurrT = System.currentTimeMillis();
        Result<Record> res = osMetricsSnap.getOSMetrics();

        Set<String> metricColumns = osMetricsSnap.getMetricColumns();
        for (Record r: res) {
            Dimensions dimensions = new Dimensions();
            Object threadName = r.get(OSMetricsSnapshot.Fields.tName.toString());

            if (threadName == null) {
                LOG.debug("Could not find tName: {}", r);
                continue;
            }
            String operation = categorizeThreadName(threadName.toString(), dimensions);
            if (operation == null) {
                continue;
            }

            dimensions.put(ShardRequestMetricsSnapshot.Fields.OPERATION.toString(), operation);
            for (String metricColumn: metricColumns) {
                if (r.get(metricColumn) == null) {
                    continue;
                }
                Double metric = Double.parseDouble(r.get(metricColumn).toString());
                if (operation.equals("merge") && metricColumn.equals("cpu")) {
                    LOG.debug("Putting merge metric {}", metric);
                }
                db.putMetric(new Metric<Double>(metricColumn, metric),
                        dimensions, 0);
            }
        }
        long mFinalT = System.currentTimeMillis();
        LOG.info("Total time taken for writing threadName metrics metricsdb: {}", mFinalT - mCurrT);
    }

    public static String categorizeThreadName(String threadName, Dimensions dimensions) {
        //shardSearch and shardBulk os metrics are emitted by emitAggregatedOSMetrics and emitWorkloadMetrics functions.
        //Hence these are ignored in this emitter.
        if (SEARCH_PATTERN.matcher(threadName).matches()) {
            return null;
        }
        if (BULK_PATTERN.matcher(threadName).matches() || WRITE_PATTERN.matcher(threadName).matches()) {
            return null;
        }

        if (GC_PATTERN.matcher(threadName).matches()) {
            return "GC";
        }
        if (REFRESH_PATTERN.matcher(threadName).matches()) {
            return "refresh";
        }
        if (MANAGEMENT_PATTERN.matcher(threadName).matches()) {
            return "management";
        }
        if (HTTP_SERVER_PATTERN.matcher(threadName).matches()) {
            return "httpServer";
        }
        if (TRANS_CLIENT_PATTERN.matcher(threadName).matches()) {
            return "transportClient";
        }
        if (TRANS_SERVER_PATTERN.matcher(threadName).matches()) {
            return "transportServer";
        }
        Matcher mergeMatcher = MERGE_PATTERN.matcher(threadName);
        if (mergeMatcher.matches()) {
            dimensions.put(
                    ShardRequestMetricsSnapshot.Fields.INDEX_NAME.toString(),
                    mergeMatcher.group(1));
            dimensions.put(
                    ShardRequestMetricsSnapshot.Fields.SHARD_ID.toString(),
                    mergeMatcher.group(2));
            return "merge";
        }
        return "other";
    }

    public static void emitHttpMetrics(final DSLContext create, final MetricsDB db,
            final HttpRequestMetricsSnapshot rqMetricsSnap) throws Exception {
        long mCurrT = System.currentTimeMillis();
        Dimensions dimensions = new Dimensions();
        Result<Record> res = rqMetricsSnap.fetchLatencyByOp();
        List<String> dims = new ArrayList<String>() { {
            this.add(HttpRequestMetricsSnapshot.Fields.OPERATION.toString());
            this.add(HttpRequestMetricsSnapshot.Fields.EXCEPTION.toString());
            this.add(HttpRequestMetricsSnapshot.Fields.INDICES.toString());
            this.add(HttpRequestMetricsSnapshot.Fields.HTTP_RESP_CODE.toString());
            } };
        db.createMetric(new Metric<Double>(AllMetrics.CommonMetric.LATENCY.toString(), 0d),
                       dims);
        db.createMetric(new Metric<Double>(AllMetrics.HttpMetric.HTTP_TOTAL_REQUESTS.toString(), 0d),
                        dims);
        db.createMetric(new Metric<Double>(AllMetrics.HttpMetric.HTTP_REQUEST_DOCS.toString(), 0d),
                        dims);

        for (Record r: res) {
            dimensions.put(HttpRequestMetricsSnapshot.Fields.OPERATION.toString(),
                    r.get(HttpRequestMetricsSnapshot.Fields.OPERATION.toString()).toString());
            dimensions.put(HttpRequestMetricsSnapshot.Fields.HTTP_RESP_CODE.toString(),
                    r.get(HttpRequestMetricsSnapshot.Fields.HTTP_RESP_CODE.toString()).toString());
            dimensions.put(HttpRequestMetricsSnapshot.Fields.INDICES.toString(),
                    r.get(HttpRequestMetricsSnapshot.Fields.INDICES.toString()).toString());
            dimensions.put(HttpRequestMetricsSnapshot.Fields.EXCEPTION.toString(),
                    r.get(HttpRequestMetricsSnapshot.Fields.EXCEPTION.toString()).toString());

            Double sumLatency = Double.parseDouble(r.get(DBUtils.
                        getAggFieldName(HttpRequestMetricsSnapshot.Fields.LAT.toString(), MetricsDB.SUM))
                        .toString());
            Double avgLatency = Double.parseDouble(r.get(DBUtils.
                        getAggFieldName(HttpRequestMetricsSnapshot.Fields.LAT.toString(), MetricsDB.AVG))
                        .toString());
            Double minLatency = Double.parseDouble(r.get(DBUtils.
                        getAggFieldName(HttpRequestMetricsSnapshot.Fields.LAT.toString(), MetricsDB.MIN))
                        .toString());
            Double maxLatency = Double.parseDouble(r.get(DBUtils.
                        getAggFieldName(HttpRequestMetricsSnapshot.Fields.LAT.toString(), MetricsDB.MAX))
                        .toString());

            Double count = Double.parseDouble(r.get(HttpRequestMetricsSnapshot.Fields.HTTP_TOTAL_REQUESTS.toString()).toString());

            Double sumItemCount = Double.parseDouble(r.get(DBUtils.
                        getAggFieldName(HttpRequestMetricsSnapshot.Fields.HTTP_REQUEST_DOCS.toString(), MetricsDB.SUM))
                        .toString());
            Double avgItemCount = Double.parseDouble(r.get(DBUtils.
                        getAggFieldName(HttpRequestMetricsSnapshot.Fields.HTTP_REQUEST_DOCS.toString(), MetricsDB.AVG))
                        .toString());
            Double minItemCount = Double.parseDouble(r.get(DBUtils.
                        getAggFieldName(HttpRequestMetricsSnapshot.Fields.HTTP_REQUEST_DOCS.toString(), MetricsDB.MIN))
                        .toString());
            Double maxItemCount = Double.parseDouble(r.get(DBUtils.
                        getAggFieldName(HttpRequestMetricsSnapshot.Fields.HTTP_REQUEST_DOCS.toString(), MetricsDB.MAX))
                        .toString());

            db.putMetric(new Metric<Double>(AllMetrics.CommonMetric.LATENCY.toString(), sumLatency,
                        avgLatency, minLatency, maxLatency),
                        dimensions, 0);
            db.putMetric(new Metric<Double>(AllMetrics.HttpMetric.HTTP_TOTAL_REQUESTS.toString(), count),
                        dimensions, 0);
            db.putMetric(new Metric<Double>(AllMetrics.HttpMetric.HTTP_REQUEST_DOCS.toString(), sumItemCount,
                        avgItemCount, minItemCount, maxItemCount),
                        dimensions, 0);
        }

        long mFinalT = System.currentTimeMillis();
        LOG.info("Total time taken for writing http metrics metricsdb: {}", mFinalT - mCurrT);
    }

    /**
     * TODO: Some of these metrics have default value like tcp.SSThresh:-1.
     *  Should we count them in aggregation?
     * @param create A contextual DSL providing "attached" implementations to
     *  the org.jooq interfaces.
     * @param db On-disk database that holds a snapshot of all metrics, which
     *   includes the metrics that customers can query.
      *@param snap In memory database that holds a snapshot of all metrics.
      *  This is the intermediate representation of metrics.
     * @throws Exception thrown when we cannot emit metrics from the in-memory
     * database to the on-disk database.
     */
    public static void emitNodeMetrics(final DSLContext create,
            final MetricsDB db, final MemoryDBSnapshot snap) throws Exception {

        Map<String, SelectHavingStep<Record>> metadataTable = snap
                .selectMetadataSource();

        Map<String, List<Field<?>>> selectField = snap
                .getTableSelectFieldsMap();

        List<String> dimensionNames = snap.getDimensionNames();

        for (Map.Entry<String, SelectHavingStep<Record>> entry : metadataTable
                .entrySet()) {
            long mCurrT = System.currentTimeMillis();

            String tableName = entry.getKey();
            Result<Record> fetchedData = entry.getValue().fetch();

            long mFinalT = System.currentTimeMillis();
            LOG.info("Total time taken for aggregating {} : {}", tableName,
                    mFinalT - mCurrT);

            if (fetchedData == null  || fetchedData.size() == 0) {
                LOG.info("No data to emit: {}", tableName);
                continue;
            }

            mCurrT = System.currentTimeMillis();

            List<Field<?>> selectFields = selectField.get(tableName);

            db.createMetric(new Metric<Double>(tableName, 0d),
                        dimensionNames);

            BatchBindStep handle = db.startBatchPut(tableName,
                    selectFields.size());
            for (Record r : fetchedData) {
                int columnNum = selectFields.size();
                Object[] bindValues = new Object[columnNum];
                for (int i = 0; i < columnNum; i++) {
                    bindValues[i] = r.get(selectFields.get(i).getName());
                }
                handle.bind(bindValues);
            }
            handle.execute();

            mFinalT = System.currentTimeMillis();
            LOG.info("Total time taken for writing {} metrics metricsdb: {}",
                    tableName, mFinalT - mCurrT);
        }
    }
}


