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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.BatchBindStep;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.Select;
import org.jooq.TableLike;
import org.jooq.impl.DSL;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.DBUtils;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.Removable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatExceptionCode;

/**
 * On-disk database that holds a 5 second snapshot of all metrics.
 * We create one table per metric. Every row contains four aggregations and any other relevant dimensions.
 *
 * Eg:
 * CPU table
 * |sum|avg|max|min|   index|shard|role|
 * +---+---+---+---+--------+-----+----+
 * |  5|2.5|  3|  2|sonested|    1| N/A|
 *
 * RSS table
 * |sum|avg|max|min|    index|shard|role|
 * +---+---+---+---+---------+-----+----+
 * | 30| 15| 20| 10|nyc_taxis|    1| N/A|
 */
@SuppressWarnings("serial")
public class MetricsDB implements Removable {

    private static final Logger LOG = LogManager.getLogger(MetricsDB.class);

    private static final String DB_FILE_PREFIX_PATH_DEFAULT = "/tmp/metricsdb_";
    private static final String DB_FILE_PREFIX_PATH_CONF_NAME = "metrics-db-file-prefix-path";
    private static final String DB_URL = "jdbc:sqlite:";
    private final Connection conn;
    private final DSLContext create;
    public static final String SUM = "sum";
    public static final String AVG = "avg";
    public static final String MIN = "min";
    public static final String MAX = "max";
    private long windowStartTime;

    public String getDBFilePath() {
        return PluginSettings.instance().getSettingValue(DB_FILE_PREFIX_PATH_CONF_NAME, DB_FILE_PREFIX_PATH_DEFAULT)
                + Long.toString(windowStartTime);
    }

    public MetricsDB(long windowStartTime) throws Exception {
        this.windowStartTime = windowStartTime;
        String url = DB_URL + getDBFilePath();
        conn = DriverManager.getConnection(url);
        conn.setAutoCommit(false);
        create = DSL.using(conn, SQLDialect.SQLITE);
    }

    public void close() throws Exception {
        conn.close();
    }

    public void createMetric(Metric<?> metric, List<String> dimensions) {
        if (DBUtils.checkIfTableExists(create, metric.getName())) {
            return;
        }

        List<Field<?>> fields = DBUtils.getFieldsFromList(dimensions);
        fields.add(DSL.field(SUM, metric.getValueType()));
        fields.add(DSL.field(AVG, metric.getValueType()));
        fields.add(DSL.field(MIN, metric.getValueType()));
        fields.add(DSL.field(MAX, metric.getValueType()));
        create.createTable(metric.getName())
            .columns(fields)
            .execute();
    }

    public BatchBindStep startBatchPut(Metric<?> metric, List<String> dimensions) {
        List<?> dummyValues = new ArrayList<>();
        for (String dim: dimensions) {
            dummyValues.add(null);
        }
        //Finally add sum, avg, min, max
        dummyValues.add(null);
        dummyValues.add(null);
        dummyValues.add(null);
        dummyValues.add(null);
        return create.batch(create.insertInto(DSL.table(metric.getName())).values(dummyValues));
    }

    public BatchBindStep startBatchPut(String tableName, int dimNum) {
        if (dimNum < 1 || !DBUtils.checkIfTableExists(create, tableName)) {
            throw new IllegalArgumentException(String
                    .format("Incorrect arguments %s, %d", tableName, dimNum));
        }
        List<?> dummyValues = new ArrayList<>(dimNum);
        for (int i = 0; i < dimNum; i++) {
            dummyValues.add(null);
        }

        return create.batch(
                create.insertInto(DSL.table(tableName)).values(dummyValues));
    }

    public void putMetric(Metric<Double> metric,
            Dimensions dimensions,
            long windowStartTime) {
        create.insertInto(DSL.table(metric.getName()))
            .set(DSL.field(SUM, Double.class), metric.getSum())
            .set(DSL.field(AVG, Double.class), metric.getAvg())
            .set(DSL.field(MIN, Double.class), metric.getMin())
            .set(DSL.field(MAX, Double.class), metric.getMax())
            .set(dimensions.getFieldMap())
            .execute();
    }

    //We have a table per metric. We do a group by/aggregate on
    //every dimension and return all the metric tables.
    public List<TableLike<Record>> getAggregatedMetricTables(List<String> metrics,
            List<String> aggregations, List<String> dimensions) throws Exception {
        List<TableLike<Record>> tList = new ArrayList<>();
        List<Field<?>> groupByFields = DBUtils.getFieldsFromList(dimensions);

        for (int i = 0; i < metrics.size(); i++) {
            String metric = metrics.get(i);
            List<Field<?>> selectFields = DBUtils.getFieldsFromList(dimensions);
            String aggType = aggregations.get(i);
            if (aggType.equals(SUM)) {
                Field<Double> field = DSL.field(SUM, Double.class);
                selectFields.add(DSL.sum(field).as(metric));
            } else if (aggType.equals(AVG)) {
                Field<Double> field = DSL.field(AVG, Double.class);
                selectFields.add(DSL.avg(field).as(metric));
            } else if (aggType.equals(MIN)) {
                Field<Double> field = DSL.field(MIN, Double.class);
                selectFields.add(DSL.min(field).as(metric));
            } else if (aggType.equals(MAX)) {
                Field<Double> field = DSL.field(MAX, Double.class);
                selectFields.add(DSL.max(field).as(metric));
            } else {
                throw new Exception("Unknown agg type");
            }
            if (!DBUtils.checkIfTableExists(create, metrics.get(i))) {
                tList.add(null);
            } else {
                tList.add(create.select(selectFields)
                        .from(DSL.table(metric))
                        .groupBy(groupByFields)
                        .asTable());
            }
        }
        return tList;
    }


    /**
     * query metrics from different tables and merge to one table.
     *
     * getAggregatedMetricTables returns tables like:
     * +-----+---------+-----+
     * |shard|indexName|  cpu|
     * +-----+---------+-----+
     * |0    |sonested |   10|
     * |1    |sonested |   20|
     *
     * +-----+---------+-----+
     * |shard|indexName|  rss|
     * +-----+---------+-----+
     * |0    |sonested |   54|
     * |2    |sonested |   47|
     *
     * We select metrics from each table and union them:
     * +-----+---------+-----+-----+
     * |shard|indexName|  cpu|  rss|
     * +-----+---------+-----+-----+
     * |0    |sonested |   10| null|
     * |1    |sonested |   20| null|
     * |0    |sonested | null|   54|
     * |2    |sonested | null|   47|
     *
     * Then, we group by dimensions and return following table:
     * +-----+---------+-----+-----+
     * |shard|indexName|  cpu|  rss|
     * +-----+---------+-----+-----+
     * |0    |sonested |   10|   54|
     * |1    |sonested |   20| null|
     * |2    |sonested | null|   47|
     *
     * @param metrics a list of metrics we want to query
     * @param aggregations aggregation we want to use for each metric
     * @param dimensions dimension we want to use for each metric
     *
     * @return result of query
     *
     * @throws Exception if one of the aggregations contains sth other than
     *   "sum", "avg", "min", and "max".
     * */
    public Result<Record> queryMetric(List<String> metrics,
            List<String> aggregations, List<String> dimensions) throws Exception {
        List<TableLike<Record>> tList = getAggregatedMetricTables(metrics,
                aggregations, dimensions);

        //Join all the individual metric tables to generate the final table.
        Select<Record> finalTable = null;
        for (int i = 0; i < tList.size(); i++) {
            TableLike<Record> metricTable = tList.get(i);
            if (metricTable == null) {
                LOG.info(String.format("%s metric table does not exist. " +
                        "Returning null for the metric/dimension.", metrics.get(i)));
                continue;
            }
            List<Field<?>> selectFields = DBUtils.getSelectFieldsForMetricName(metrics.get(i), metrics, dimensions);
            Select<Record> curTable = create.select(selectFields).from(metricTable);

            if (finalTable == null) {
                finalTable = curTable;
            } else {
                finalTable = finalTable.union(curTable);
            }
        }

        List<Field<?>> allFields = DBUtils.getFieldsFromList(dimensions);
        for (String metric : metrics) {
            allFields.add(DSL.max(DSL.field(metric, Double.class)).as(metric));
        }
        List<Field<?>> groupByFields = DBUtils.getFieldsFromList(dimensions);
        if (finalTable == null) {
            return null;
        }
        return create.select(allFields).from(finalTable).groupBy(groupByFields).fetch();
    }

    public void commit() throws Exception {
        conn.commit();
    }

    /**
     * We delete the physical file on disk, only if the user has not set the DB_FILE_MAX_COUNT_CONF_NAME
     * to -1. Otherwise we just close the connection and return.
     **/
    @Override
    public void remove() throws Exception {
       conn.close();
       if (PluginSettings.instance().getMaxMetricsDBFilesCount() != PluginSettings.DB_FILE_VALUE_FOR_NO_DELETION) {
          File dbFile = new File(getDBFilePath());
          if (!dbFile.delete()) {
              LOG.error("Failed to delete File - {} with ExceptionCode: {}", getDBFilePath(), StatExceptionCode.OTHER.toString());
              StatsCollector.instance().logException();
          }
       }
    }

    public Result<Record> queryMetric(String metric) {
        return create.select().from(DSL.table(metric)).fetch();
    }

    public boolean metricExists(String metric) {
        return DBUtils.checkIfTableExists(create, metric);
    }
}


