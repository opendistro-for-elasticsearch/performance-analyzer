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

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.BatchBindStep;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.SelectField;
import org.jooq.SelectHavingStep;
import org.jooq.impl.DSL;

import com.amazon.opendistro.performanceanalyzer.DBUtils;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.OSMetrics;

@SuppressWarnings("serial")
public class OSMetricsSnapshot implements Removable {
    private static final Logger LOG = LogManager.getLogger(OSMetricsSnapshot.class);

    private final DSLContext create;
    private final String tableName;
    private long lastUpdatedTime;
    private Set<String> dimensionColumns;

    private static final LinkedHashSet<String> METRIC_COLUMNS;

    public enum Fields {
        tid, tName, weight
    }

    static {
        METRIC_COLUMNS = new LinkedHashSet<>();
        for (OSMetrics metric: OSMetrics.values()) {
            METRIC_COLUMNS.add(metric.toString());
        }
    }

    public DSLContext getDSLContext() {
        return create;
    }

    public OSMetricsSnapshot(Connection conn, String tableNamePrefix, Long windowEndTime) {
        this.tableName = tableNamePrefix + windowEndTime;
        this.create = DSL.using(conn, SQLDialect.SQLITE);

        this.dimensionColumns = new LinkedHashSet<String>() { {
            this.add(Fields.tid.toString());
            this.add(Fields.tName.toString());
        } };

        LOG.debug("Creating a new os snapshot table - {}", tableName);
        create
            .createTable(this.tableName)
            .columns(DBUtils.getStringFieldsFromList(dimensionColumns))
            .columns(DBUtils.getDoubleFieldsFromList(METRIC_COLUMNS))
            .execute();
    }

    public OSMetricsSnapshot(Connection conn, Long windowEndTime) {
        this(conn, "os_", windowEndTime);
    }

    public long getLastUpdatedTime() {
        return this.lastUpdatedTime;
    }

    public void setLastUpdatedTime(long lastUpdatedTime) {
        this.lastUpdatedTime = lastUpdatedTime;
    }

    public void putMetric(Map<String, Double> metrics, Map<String, String> dimensions) {
        Map<Field<?>, String> dimensionMap = new HashMap<Field<?>, String>();
        Map<Field<?>, Double> metricMap = new HashMap<Field<?>, Double>();

        for (Map.Entry<String, String> dimension: dimensions.entrySet()) {
            dimensionMap.put(DSL.field(
                        DSL.name(dimension.getKey()), String.class), dimension.getValue());
        }

        for (Map.Entry<String, Double> metricName: metrics.entrySet()) {
                metricMap.put(DSL.field(
                            DSL.name(metricName.getKey()), Double.class), metricName.getValue());
        }

        create.insertInto(DSL.table(this.tableName))
            .set(metricMap)
            .set(dimensionMap)
            .execute();
    }

    public BatchBindStep startBatchPut() {
        List<Object> dummyValues = new ArrayList<>();
        for (int i = 0; i < dimensionColumns.size(); i++) {
            dummyValues.add(null);
        }
        for (int i = 0; i < METRIC_COLUMNS.size(); i++) {
            dummyValues.add(null);
        }
        return create.batch(create.insertInto(DSL.table(this.tableName)).values(dummyValues));
    }

    public List<Field<?>> getMetricColumnFields() {
        return OSMetricsSnapshot.METRIC_COLUMNS.stream().map(s -> DSL.field(s, Double.class))
            .collect(Collectors.toList());
    }

    public void putMetric(Map<String, Double> metrics, String tid, String tName) {
        Map<Field<?>, Double> metricMap = new HashMap<Field<?>, Double>();

        for (Map.Entry<String, Double> metricName: metrics.entrySet()) {
                metricMap.put(DSL.field(
                            DSL.name(metricName.getKey()), Double.class),
                            metricName.getValue());
        }

        create.insertInto(DSL.table(this.tableName))
            .set(DSL.field(Fields.tid.toString()), tid)
            .set(DSL.field(Fields.tName.toString()), tName)
            .set(metricMap)
            .execute();
    }

    public String getTableName() {
        return this.tableName;
    }

    public Result<Record> fetchAll() {
        return create.select().from(DSL.table(this.tableName)).fetch();
    }

    public Result<Record> fetchNegative() {
        return create.select().from(DSL.table(this.tableName))
            .where(DSL.field(OSMetrics.CPU_UTILIZATION.toString()).lt(0L)).fetch();
    }

    public SelectHavingStep<Record> selectAll() {
        return create.select(getFields()).from(this.tableName);
    }

    @Override
    public void remove() {
        LOG.info("Dropping {}", this.tableName);
        create.dropTable(DSL.table(this.tableName)).execute();
    }

    public void logSnap() {
        LOG.debug(() -> getDebugSnap());
    }

    public Result<?> getDebugSnap() {
        return create
                .select(DSL.field(Fields.tid.toString()).as(Fields.tid.toString()),
                        DSL.field(Fields.tName.toString()).as(Fields.tName.toString()),
                        DSL.field(OSMetrics.CPU_UTILIZATION.toString()),
                        DSL.field(OSMetrics.PAGING_MIN_FLT_RATE.toString()))
                .from(this.tableName)
                .where(DSL.field(OSMetrics.CPU_UTILIZATION.toString(),
                        Double.class).ne(0d))
                .fetch();
    }

    public Result<Record> getOSMetrics() {
        List<SelectField<?>> fields = new ArrayList<SelectField<?>>();
        fields.add(DSL.field(Fields.tid.toString()).as(Fields.tid.toString()));
        fields.add(DSL.field(Fields.tName.toString()).as(Fields.tName.toString()));
        for (String metricColumn: METRIC_COLUMNS) {
            fields.add(DSL.field(metricColumn, Double.class).as(metricColumn));
        }
        return create.select(fields)
            .from(this.tableName)
            .fetch();
    }

    /**
     * Given metrics in two windows calculates a new window which overlaps with the given windows.
     * |------leftWindow-------|-------rightWindow--------|
     *                         t
     *            a                              b
     *            |-----------alignedWindow------|
     *
     * This method assumes that both left/right windows are greater than or equal to 5 seconds.
     *
     *  @param leftWindow a snapshot of the left window metrics
     *  @param rightWindow a snapshot of the right window metrics
     *  @param alignedWindow aligned window combinging left and right window
     *  @param t leftWindow end time, as well as right window start time
     *  @param a aligned window start time.
     *  @param b aligned window end time.
     *
     */
    public static void alignWindow(OSMetricsSnapshot leftWindow,
            OSMetricsSnapshot rightWindow, String alignedWindow,
            long t, long a, long b) {
        DSLContext create = leftWindow.getDSLContext();
        ArrayList<SelectField<?>> alignedFields = new ArrayList<SelectField<?>>();
        alignedFields.add(DSL.field(Fields.tid.toString()).as(Fields.tid.toString()));
        alignedFields.add(DSL.field(Fields.tName.toString()).as(Fields.tName.toString()));
        for (String metricName: METRIC_COLUMNS) {
            alignedFields.add(DSL.sum(DSL.field(metricName, Double.class))
                    .div(DSL.sum(DSL.field(Fields.weight.toString(), Double.class))).as(metricName));
        }

        List<SelectField<?>> leftWinFields = new ArrayList<SelectField<?>>();
        leftWinFields.add(DSL.field(Fields.tid.toString(), String.class).as(Fields.tid.toString()));
        leftWinFields.add(DSL.field(Fields.tName.toString(), String.class).as(Fields.tName.toString()));
        leftWinFields.add(DSL.val(t - a).as(Fields.weight.toString()));
        for (String c: METRIC_COLUMNS) {
            leftWinFields.add(DSL.field(c, Double.class).mul(t - a).as(c));
        }
        List<SelectField<?>> rightWinFields = new ArrayList<SelectField<?>>();
        rightWinFields.add(DSL.field(Fields.tid.toString(), String.class).as(Fields.tid.toString()));
        rightWinFields.add(DSL.field(Fields.tName.toString(), String.class).as(Fields.tName.toString()));
        rightWinFields.add(DSL.val(b - t).as(Fields.weight.toString()));
        for (String c: METRIC_COLUMNS) {
            rightWinFields.add(DSL.field(c, Double.class).mul(b - t).as(c));
        }

        create.insertInto(DSL.table(alignedWindow)).select(
                create.select(alignedFields).from(
                    create.select(leftWinFields).from(leftWindow.tableName)
                        .unionAll(create
                            .select(rightWinFields).from(rightWindow.getTableName())
                        )
                )
                .groupBy(DSL.field(Fields.tid.toString(), String.class)))
            .execute();
    }

    public List<Field<?>> getFields() {
        List<Field<?>> fields = new ArrayList<Field<?>>();
        for (String dimension: dimensionColumns) {
            fields.add(DSL.field(dimension, String.class));
        }
        for (String metric: METRIC_COLUMNS) {
            fields.add(DSL.field(metric, Double.class));
        }
        return fields;
    }

    public Set<String> getMetricColumns() {
        return OSMetricsSnapshot.METRIC_COLUMNS;
    }
}

