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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.DBUtils;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
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


import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;


public class MasterEventMetricsSnapshot implements Removable {
    private static final Logger LOG = LogManager.getLogger(MasterEventMetricsSnapshot.class);

    private final DSLContext create;
    private final Long windowStartTime;
    private final String tableName;
    private static final Long EXPIRE_AFTER = 1200000L;
    private List<Field<?>> columns;


    public enum Fields {

        TID("tid"),
        IS_CURRENT("isCurrent"),
        OLD_START("oldStart"),
        ST("st"),
        ET("et"),
        LAT ("lat");

        private final String fieldValue;

        Fields(String fieldValue) {
            this.fieldValue = fieldValue;
        }

        @Override
        public String toString() {
            return fieldValue;
        }
    };

    public MasterEventMetricsSnapshot(Connection conn, Long windowStartTime) {
        this.create = DSL.using(conn, SQLDialect.SQLITE);
        this.windowStartTime = windowStartTime;
        this.tableName = "master_event_" + windowStartTime;

        this.columns = new ArrayList<Field<?>>() { {
            this.add(DSL.field(DSL.name(Fields.TID.toString()), String.class));
            this.add(DSL.field(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_INSERT_ORDER.toString()), String.class));
            this.add(DSL.field(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_PRIORITY.toString()), String.class));
            this.add(DSL.field(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_TYPE.toString()), String.class));
            this.add(DSL.field(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_METADATA.toString()), String.class));
            this.add(DSL.field(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_QUEUE_TIME.toString()), String.class));
            this.add(DSL.field(DSL.name(Fields.ST.toString()), Long.class));
            this.add(DSL.field(DSL.name(Fields.ET.toString()), Long.class));
        } };

        System.out.println(this.windowStartTime);

        create.createTable(this.tableName)
                .columns(columns)
                .execute();
    }


    @Override
    public void remove() throws Exception {

        create.dropTable(DSL.table(this.tableName)).execute();
    }

    public void rolloverInflightRequests(MasterEventMetricsSnapshot prevSnap) {
        //Fetch all entries that have not ended and write to current table.
        create.insertInto(DSL.table(this.tableName)).select(prevSnap.fetchInflightRequests()).execute();

        LOG.error("Inflight shard requests");
        LOG.error(() -> fetchAll());
    }

    private SelectHavingStep<Record> fetchInflightRequests() {

        ArrayList<SelectField<?>> fields = new ArrayList<SelectField<?>>() { {
            this.add(DSL.field(DSL.name(Fields.TID.toString()), String.class));
            this.add(DSL.field(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_INSERT_ORDER.toString()), String.class));
            this.add(DSL.field(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_PRIORITY.toString()), String.class));
            this.add(DSL.field(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_TYPE.toString()), String.class));
            this.add(DSL.field(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_METADATA.toString()), String.class));
            this.add(DSL.field(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_QUEUE_TIME.toString()), String.class));
            this.add(DSL.field(DSL.name(Fields.ST.toString()), Long.class));
            this.add(DSL.field(DSL.name(Fields.ET.toString()), Long.class));
        } };

        System.out.println(this.windowStartTime);
        System.out.println(this.windowStartTime - EXPIRE_AFTER);

        return create.select(fields).from(groupByInsertOrder())
                .where(DSL.field(Fields.ST.toString()).isNotNull()
                        .and(DSL.field(Fields.ET.toString()).isNull())
                        .and(DSL.field(Fields.ST.toString()).gt(this.windowStartTime - EXPIRE_AFTER)));
    }

    public Result<Record> fetchAll() {
        return create.select().from(DSL.table(this.tableName)).fetch();
    }

    public BatchBindStep startBatchPut() {

        List<Object> dummyValues = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            dummyValues.add(null);
        }
        return create.batch(create.insertInto(DSL.table(this.tableName)).values(dummyValues));
    }


    public Result<Record> fetchQueueAndRunTime() {

        List<SelectField<?>> fields = new ArrayList<SelectField<?>>() { {
            this.add(DSL.field(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_INSERT_ORDER.toString()), String.class));
            this.add(DSL.field(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_PRIORITY.toString()), String.class));
            this.add(DSL.field(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_TYPE.toString()), String.class));
            this.add(DSL.field(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_METADATA.toString()), String.class));
            this.add(DSL.field(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_QUEUE_TIME.toString()), String.class));

            this.add(DSL.sum(DSL.field(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_QUEUE_TIME.toString()), Double.class))
                    .as(DBUtils.getAggFieldName(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_QUEUE_TIME.toString(), MetricsDB.SUM)));
            this.add(DSL.avg(DSL.field(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_QUEUE_TIME.toString()), Double.class))
                    .as(DBUtils.getAggFieldName(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_QUEUE_TIME.toString(), MetricsDB.AVG)));
            this.add(DSL.min(DSL.field(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_QUEUE_TIME.toString()), Double.class))
                    .as(DBUtils.getAggFieldName(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_QUEUE_TIME.toString(), MetricsDB.MIN)));
            this.add(DSL.max(DSL.field(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_QUEUE_TIME.toString()), Double.class))
                    .as(DBUtils.getAggFieldName(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_QUEUE_TIME.toString(), MetricsDB.MAX)));

            this.add(DSL.sum(DSL.field(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_RUN_TIME.toString()), Double.class))
                    .as(DBUtils.getAggFieldName(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_RUN_TIME.toString(), MetricsDB.SUM)));
            this.add(DSL.avg(DSL.field(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_RUN_TIME.toString()), Double.class))
                    .as(DBUtils.getAggFieldName(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_RUN_TIME.toString(), MetricsDB.AVG)));
            this.add(DSL.min(DSL.field(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_RUN_TIME.toString()), Double.class))
                    .as(DBUtils.getAggFieldName(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_RUN_TIME.toString(), MetricsDB.MIN)));
            this.add(DSL.max(DSL.field(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_RUN_TIME.toString()), Double.class))
                    .as(DBUtils.getAggFieldName(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_RUN_TIME.toString(), MetricsDB.MAX)));
        } };

        ArrayList<Field<?>> groupByFields = new ArrayList<Field<?>>() { {
            this.add(DSL.field(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_INSERT_ORDER.toString()), String.class));
        } };

        return create.select(fields).from(fetchRunTimeHelper())
                .groupBy(groupByFields)
                .fetch();
    }

    public SelectHavingStep<Record> fetchRunTimeHelper() {

        List<SelectField<?>> fields = new ArrayList<SelectField<?>>() { {
            this.add(DSL.field(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_INSERT_ORDER.toString()), String.class));
            this.add(DSL.field(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_PRIORITY.toString()), String.class));
            this.add(DSL.field(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_TYPE.toString()), String.class));
            this.add(DSL.field(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_METADATA.toString()), String.class));
            this.add(DSL.field(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_QUEUE_TIME.toString()), String.class));
            this.add(DSL.field(Fields.ET.toString()).minus(DSL.field(Fields.ST.toString())).
                    as(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_RUN_TIME.toString())));
        } };

        return create.select(fields).from(groupByInsertOrderWithoutNull())
                .where(DSL.field(Fields.ET.toString()).isNotNull().and(
                        DSL.field(Fields.ST.toString()).isNotNull()));
    }

    public SelectHavingStep<Record> groupByInsertOrderWithoutNull() {

        Long endTime = windowStartTime + MetricsConfiguration.SAMPLING_INTERVAL;
        List<SelectField<?>> fields = getGroupByInsertOrderSelectFields();
        fields.add(DSL.least(DSL.coalesce(DSL.max(DSL.field(Fields.ET.toString(), Long.class)), endTime), endTime)
                .as(DSL.name(Fields.ET.toString())));

        ArrayList<Field<?>> groupByInsertOrder = new ArrayList<Field<?>>() { {
            this.add(DSL.field(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_INSERT_ORDER.toString()), String.class));
        } };

        return create.select(fields)
                .from(DSL.table(this.tableName))
                .groupBy(groupByInsertOrder);
    }

    public SelectHavingStep<Record> groupByInsertOrder() {

        ArrayList<SelectField<?>> fields = getGroupByInsertOrderSelectFields();

        fields.add(DSL.max(DSL.field(Fields.ET.toString(), Long.class)).as(DSL.name(Fields.ET.toString())));
        fields.add(DSL.field(DSL.name(Fields.TID.toString()), String.class));

        ArrayList<Field<?>> groupByInsertOrder = new ArrayList<Field<?>>() { {
            this.add(DSL.field(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_INSERT_ORDER.toString()), String.class));
        } };

        return create.select(fields)
                .from(DSL.table(this.tableName))
                .groupBy(groupByInsertOrder);
    }

    private ArrayList<SelectField<?>> getGroupByInsertOrderSelectFields() {

        ArrayList<SelectField<?>> fields = new ArrayList<SelectField<?>>() { {
            this.add(DSL.field(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_INSERT_ORDER.toString()), String.class));

            this.add(DSL.max(DSL.field(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_TYPE.toString()))
                    .as(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_TYPE.toString())));

            this.add(DSL.max(DSL.field(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_METADATA.toString()))
                    .as(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_METADATA.toString())));

            this.add(DSL.max(DSL.field(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_QUEUE_TIME.toString()))
                    .as(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_QUEUE_TIME.toString())));

            this.add(DSL.max(DSL.field(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_PRIORITY.toString()))
                    .as(DSL.name(AllMetrics.Master_Metric_Dimensions.MASTER_TASK_PRIORITY.toString())));

            this.add(DSL.max(DSL.field(Fields.ST.toString(), Long.class)).as(DSL.name(Fields.ST.toString())));

        } };

        return fields;
    }
}
