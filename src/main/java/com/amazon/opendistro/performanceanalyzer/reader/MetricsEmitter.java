package com.amazon.opendistro.performanceanalyzer.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazon.opendistro.performanceanalyzer.DBUtils;
import com.amazon.opendistro.performanceanalyzer.config.TroubleshootingConfig;
import com.amazon.opendistro.performanceanalyzer.metricsdb.Metric;
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

import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.OS_Metrics;
import com.amazon.opendistro.performanceanalyzer.metricsdb.Dimensions;
import com.amazon.opendistro.performanceanalyzer.metricsdb.MetricsDB;

@SuppressWarnings("serial")
public class MetricsEmitter {

    private static final Logger LOG = LogManager.getLogger(MetricsEmitter.class);

    public static void emitAggregatedOSMetrics(final DSLContext create,
        final MetricsDB db, final OSMetricsSnapshot osMetricsSnap,
        final ShardRequestMetricsSnapshot rqMetricsSnap) throws Exception {

        SelectHavingStep<Record> rqTable = rqMetricsSnap.fetchThreadUtilizationRatioTable();
        SelectHavingStep<Record> osTable = osMetricsSnap.selectAll();

        List<SelectField<?>> fields = new ArrayList<SelectField<?>>() { {
            this.add(DSL.field(DSL.name(ShardRequestMetricsSnapshot.Fields.shard.name()), String.class));
            this.add(DSL.field(DSL.name(ShardRequestMetricsSnapshot.Fields.indexName.name()), String.class));
            this.add(DSL.field(DSL.name(ShardRequestMetricsSnapshot.Fields.operation.name()), String.class));
            this.add(DSL.field(DSL.name(ShardRequestMetricsSnapshot.Fields.role.name()), String.class));
        } };

        for (OS_Metrics metric: OS_Metrics.values()) {
            fields.add(DSL.field(ShardRequestMetricsSnapshot.Fields.tUtil.name(), Double.class).mul(
                            DSL.field(DSL.name(metric.name()), Double.class))
                      .as(metric.name()));
        }

        ArrayList<Field<?>> groupByFields = new ArrayList<Field<?>>() { {
            this.add(DSL.field(DSL.name(ShardRequestMetricsSnapshot.Fields.shard.name()), String.class));
            this.add(DSL.field(DSL.name(ShardRequestMetricsSnapshot.Fields.indexName.name()), String.class));
            this.add(DSL.field(DSL.name(ShardRequestMetricsSnapshot.Fields.operation.name()), String.class));
            this.add(DSL.field(DSL.name(ShardRequestMetricsSnapshot.Fields.role.name()), String.class));
        } };

        List<SelectField<?>> aggFields = new ArrayList<SelectField<?>>() { {
            this.add(DSL.field(DSL.name(ShardRequestMetricsSnapshot.Fields.shard.name()), String.class));
            this.add(DSL.field(DSL.name(ShardRequestMetricsSnapshot.Fields.indexName.name()), String.class));
            this.add(DSL.field(DSL.name(ShardRequestMetricsSnapshot.Fields.operation.name()), String.class));
            this.add(DSL.field(DSL.name(ShardRequestMetricsSnapshot.Fields.role.name()), String.class));
        } };

        for (OS_Metrics metric: OS_Metrics.values()) {
            aggFields.add(DSL.sum(DSL.field(DSL.name(metric.name()), Double.class))
                      .as(MetricsDB.SUM + "_" + metric.name()));
            aggFields.add(DSL.avg(DSL.field(DSL.name(metric.name()), Double.class))
                      .as(MetricsDB.AVG + "_" + metric.name()));
            aggFields.add(DSL.min(DSL.field(DSL.name(metric.name()), Double.class))
                      .as(MetricsDB.MIN + "_" + metric.name()));
            aggFields.add(DSL.max(DSL.field(DSL.name(metric.name()), Double.class))
                      .as(MetricsDB.MAX + "_" + metric.name()));
        }

        long mCurrT = System.currentTimeMillis();
        Result<Record> res = create.select(aggFields)
            .from(
            create.select(fields)
            .from(rqTable)
            .join(osTable)
            .on(osTable.field(OSMetricsSnapshot.Fields.tid.name(), String.class).eq(
                        rqTable.field(OSMetricsSnapshot.Fields.tid.name(), String.class)))
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
            this.add(AllMetrics.Dimensions.shard.name());
            this.add(AllMetrics.Dimensions.index.name());
            this.add(AllMetrics.Dimensions.operation.name());
            this.add(AllMetrics.Dimensions.role.name());
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
                handle.bind(r.get(ShardRequestMetricsSnapshot.Fields.shard.name()).toString(),
                        r.get(ShardRequestMetricsSnapshot.Fields.indexName.name()).toString(),
                        r.get(ShardRequestMetricsSnapshot.Fields.operation.name()).toString(),
                        r.get(ShardRequestMetricsSnapshot.Fields.role.name()).toString(),
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
        if (!TroubleshootingConfig.EnableDevAssert) {
            return;
        }

        Condition condition = DSL.trueCondition();
        Field tidField = DSL.field(DSL.name(OSMetricsSnapshot.Fields.tid.name()), String.class);
        Field tNameField = DSL.field(DSL.name(OSMetricsSnapshot.Fields.tName.name()), String.class);

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
            this.add(ShardRequestMetricsSnapshot.Fields.operation.name());
            this.add(HttpRequestMetricsSnapshot.Fields.exception.name());
            this.add(HttpRequestMetricsSnapshot.Fields.indices.name());
            this.add(HttpRequestMetricsSnapshot.Fields.status.name());
            this.add(ShardRequestMetricsSnapshot.Fields.shard.name());
            this.add(ShardRequestMetricsSnapshot.Fields.indexName.name());
            this.add(ShardRequestMetricsSnapshot.Fields.role.name());
            } };

        db.createMetric(new Metric<Double>("latency", 0d),
                        dims);
        BatchBindStep handle = db.startBatchPut(new Metric<Double>("latency", 0d),
                                                dims);


        for (Record r: res) {
            Double sumLatency = Double.parseDouble(r.get(DBUtils.
                        getAggFieldName(ShardRequestMetricsSnapshot.Fields.lat.name(), MetricsDB.SUM))
                        .toString());
            Double avgLatency = Double.parseDouble(r.get(DBUtils.
                        getAggFieldName(ShardRequestMetricsSnapshot.Fields.lat.name(), MetricsDB.AVG))
                        .toString());
            Double minLatency = Double.parseDouble(r.get(DBUtils.
                        getAggFieldName(ShardRequestMetricsSnapshot.Fields.lat.name(), MetricsDB.MIN))
                        .toString());
            Double maxLatency = Double.parseDouble(r.get(DBUtils.
                        getAggFieldName(ShardRequestMetricsSnapshot.Fields.lat.name(), MetricsDB.MAX))
                        .toString());

            handle.bind(r.get(ShardRequestMetricsSnapshot.Fields.operation.name()).toString(),
                        null,
                        null,
                        null,
                        r.get(ShardRequestMetricsSnapshot.Fields.shard.name()).toString(),
                        r.get(ShardRequestMetricsSnapshot.Fields.indexName.name()).toString(),
                        r.get(ShardRequestMetricsSnapshot.Fields.role.name()).toString(),
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
            Object threadName = r.get(OSMetricsSnapshot.Fields.tName.name());

            if (threadName == null) {
                LOG.debug("Could not find tName: {}", r);
                continue;
            }
            String operation = categorizeThreadName(threadName.toString(), dimensions);
            if (operation == null) {
                continue;
            }

            dimensions.put(ShardRequestMetricsSnapshot.Fields.operation.name(), operation);
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
        Pattern gcPattern = Pattern.compile(".*(GC|CMS|Parallel).*");
        Pattern refreshPattern = Pattern.compile(".*elasticsearch.*\\[refresh\\].*");
        Pattern managementPattern = Pattern.compile(".*elasticsearch.*\\[management\\].*");
        Pattern mergePattern = Pattern.compile(".*elasticsearch\\[.*\\]\\[\\[(.*)\\]\\[(.*)\\].*Lucene Merge.*");
        Pattern searchPattern = Pattern.compile(".*elasticsearch.*\\[search\\].*");
        Pattern bulkPattern = Pattern.compile(".*elasticsearch.*\\[bulk\\].*");
        //Pattern otherPattern = Pattern.compile(".*(elasticsearch).*");
        if (gcPattern.matcher(threadName).matches()) {
            return "GC";
        }
        if (refreshPattern.matcher(threadName).matches()) {
            return "refresh";
        }
        if (managementPattern.matcher(threadName).matches()) {
            return "management";
        }
        //shardSearch and shardBulk os metrics are emitted by emitAggregatedOSMetrics and emitWorkloadMetrics functions.
        //Hence these are ignored in this emitter.
        if (searchPattern.matcher(threadName).matches()) {
            return null;
        }
        if (bulkPattern.matcher(threadName).matches()) {
            return null;
        }
        Matcher mergeMatcher = mergePattern.matcher(threadName);
        if (mergeMatcher.matches()) {
            dimensions.put("index", mergeMatcher.group(1));
            dimensions.put(ShardRequestMetricsSnapshot.Fields.shard.name(), mergeMatcher.group(2));
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
            this.add(HttpRequestMetricsSnapshot.Fields.operation.name());
            this.add(HttpRequestMetricsSnapshot.Fields.exception.name());
            this.add(HttpRequestMetricsSnapshot.Fields.indices.name());
            this.add(HttpRequestMetricsSnapshot.Fields.status.name());
            } };
        db.createMetric(new Metric<Double>(AllMetrics.Http_Metrics.latency.name(), 0d),
                       dims);
        db.createMetric(new Metric<Double>(AllMetrics.Http_Metrics.count.name(), 0d),
                        dims);
        db.createMetric(new Metric<Double>(AllMetrics.Http_Metrics.itemCount.name(), 0d),
                        dims);

        for (Record r: res) {
            dimensions.put(HttpRequestMetricsSnapshot.Fields.operation.name(),
                    r.get(HttpRequestMetricsSnapshot.Fields.operation.name()).toString());
            dimensions.put(HttpRequestMetricsSnapshot.Fields.status.name(),
                    r.get(HttpRequestMetricsSnapshot.Fields.status.name()).toString());
            dimensions.put(HttpRequestMetricsSnapshot.Fields.indices.name(),
                    r.get(HttpRequestMetricsSnapshot.Fields.indices.name()).toString());
            dimensions.put(HttpRequestMetricsSnapshot.Fields.exception.name(),
                    r.get(HttpRequestMetricsSnapshot.Fields.exception.name()).toString());

            Double sumLatency = Double.parseDouble(r.get(DBUtils.
                        getAggFieldName(HttpRequestMetricsSnapshot.Fields.lat.name(), MetricsDB.SUM))
                        .toString());
            Double avgLatency = Double.parseDouble(r.get(DBUtils.
                        getAggFieldName(HttpRequestMetricsSnapshot.Fields.lat.name(), MetricsDB.AVG))
                        .toString());
            Double minLatency = Double.parseDouble(r.get(DBUtils.
                        getAggFieldName(HttpRequestMetricsSnapshot.Fields.lat.name(), MetricsDB.MIN))
                        .toString());
            Double maxLatency = Double.parseDouble(r.get(DBUtils.
                        getAggFieldName(HttpRequestMetricsSnapshot.Fields.lat.name(), MetricsDB.MAX))
                        .toString());

            Double count = Double.parseDouble(r.get(HttpRequestMetricsSnapshot.Fields.count.name()).toString());

            Double sumItemCount = Double.parseDouble(r.get(DBUtils.
                        getAggFieldName(HttpRequestMetricsSnapshot.Fields.itemCount.name(), MetricsDB.SUM))
                        .toString());
            Double avgItemCount = Double.parseDouble(r.get(DBUtils.
                        getAggFieldName(HttpRequestMetricsSnapshot.Fields.itemCount.name(), MetricsDB.AVG))
                        .toString());
            Double minItemCount = Double.parseDouble(r.get(DBUtils.
                        getAggFieldName(HttpRequestMetricsSnapshot.Fields.itemCount.name(), MetricsDB.MIN))
                        .toString());
            Double maxItemCount = Double.parseDouble(r.get(DBUtils.
                        getAggFieldName(HttpRequestMetricsSnapshot.Fields.itemCount.name(), MetricsDB.MAX))
                        .toString());

            db.putMetric(new Metric<Double>(AllMetrics.Http_Metrics.latency.name(), sumLatency,
                        avgLatency, minLatency, maxLatency),
                        dimensions, 0);
            db.putMetric(new Metric<Double>(AllMetrics.Http_Metrics.count.name(), count),
                        dimensions, 0);
            db.putMetric(new Metric<Double>(AllMetrics.Http_Metrics.itemCount.name(), sumItemCount,
                        avgItemCount, minItemCount, maxItemCount),
                        dimensions, 0);
        }

        long mFinalT = System.currentTimeMillis();
        LOG.info("Total time taken for writing http metrics metricsdb: {}", mFinalT - mCurrT);
    }

    /**
     * TODO (kaituo): some of these metrics have default value like
     *  tcp.SSThresh:-1. Should we count them in aggregation?
     * @param create
     * @param db
     * @param snap
     * @throws Exception
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
                for (int i=0; i<columnNum; i++) {
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


