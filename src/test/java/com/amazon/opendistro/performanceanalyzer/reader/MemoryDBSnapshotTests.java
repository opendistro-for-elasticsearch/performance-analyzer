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

import static org.hamcrest.Matchers.closeTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.impl.DSL;
import org.junit.Test;

import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.CircuitBreakerDimension;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.CircuitBreakerValue;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.MetricName;

public class MemoryDBSnapshotTests extends AbstractReaderTests {

    private Field<Double>[] fields;

    //- May change across versions, based on new/removed circuit breakers

    enum CircuitBreakerType {
        request, fielddata, in_flight_requests, accounting, parent;
    }

    @SuppressWarnings("unchecked")
    public MemoryDBSnapshotTests() throws SQLException, ClassNotFoundException {
        super();

        fields = new Field[3];

        fields[0] = DSL.field(CircuitBreakerValue.CB_ESTIMATED_SIZE.toString(),
                Double.class);
        fields[1] =
                DSL.field(CircuitBreakerValue.CB_CONFIGURED_SIZE.toString(), Double.class);
        fields[2] = DSL.field(CircuitBreakerValue.CB_TRIPPED_EVENTS.toString(),
                Double.class);
    }

    @Test
    public void testCreateMemoryDBSnapshot() throws Exception {
        Connection conn = DriverManager.getConnection(DB_URL);

        long windowEndTime = 1L;

        MemoryDBSnapshot circuitBreakerValuesSnap = new MemoryDBSnapshot(conn,
                MetricName.CIRCUIT_BREAKER,
                windowEndTime);

        assertEquals(
                MetricName.CIRCUIT_BREAKER.toString()
                        + windowEndTime,
                circuitBreakerValuesSnap.getTableName());

        long lastUpdatedTime = 2L;
        circuitBreakerValuesSnap.setLastUpdatedTime(lastUpdatedTime);
        assertTrue(lastUpdatedTime == circuitBreakerValuesSnap
                .getLastUpdatedTime());

        // should cause no exception
        circuitBreakerValuesSnap.insertMultiRows(null);

        assertTrue (0 == circuitBreakerValuesSnap.fetchAll().size());


        Object[][] values = {
                { CircuitBreakerType.request.toString(), 0, 0d,
                        19607637196d },
                { CircuitBreakerType.fielddata.toString(), 0, 0,
                        19607637196d },
                { CircuitBreakerType.in_flight_requests.toString(), 0,
                        0, 32679395328d },
                { CircuitBreakerType.accounting.toString(), 0, 0,
                        32679395328d },
                { CircuitBreakerType.parent.toString(), 0, 0,
                        22875576729d } };

        circuitBreakerValuesSnap.insertMultiRows(values);

        assertTrue (5 == circuitBreakerValuesSnap.fetchAll().size());

        // no need to commit as database is in auto-commit mode


        Result<Record> resultRecord = circuitBreakerValuesSnap.fetchMetric(
                getDimensionEqCondition(
                        CircuitBreakerDimension.CB_TYPE,
                        CircuitBreakerType.request.toString()),
                fields);

        assertTrue(1 == resultRecord.size());

        Record r = resultRecord.get(0);
        Double estimated = r.get(fields[0]);
        assertThat(estimated, closeTo(0, 0.1));

        Double tripped = r.get(fields[1]);
        assertThat(tripped, closeTo(19607637196d, 0.1));

        Double limit = r.get(fields[2]);
        assertThat(limit, closeTo(0, 0.1));

        // The 2nd remove should have no effect since the db table has already
        // been deleted
        for (int i = 0; i < 2; i++) {
            circuitBreakerValuesSnap.remove();
            assertTrue(!circuitBreakerValuesSnap.dbTableExists());
        }
    }

    @Test
    public void testAlignWindow() throws Exception {
        // time line
        // writer writes at 2000l
        // reader reads at 6000l
        // writer writes at 7000l
        // reader reads at 11000l
        // writer writes at 12000l

        MemoryDBSnapshot circuitBreakerValuesSnap1 = new MemoryDBSnapshot(conn,
                MetricName.CIRCUIT_BREAKER, 6000L);
        circuitBreakerValuesSnap1.setLastUpdatedTime(2000L);
        Object[][] values1 = {
                {CircuitBreakerType.fielddata.toString(),
                        0, 1, 19607637196d } };
        circuitBreakerValuesSnap1.insertMultiRows(values1);

        MemoryDBSnapshot circuitBreakerValuesSnap2 = new MemoryDBSnapshot(conn,
                MetricName.CIRCUIT_BREAKER, 11000L);
        circuitBreakerValuesSnap2.setLastUpdatedTime(7000L);
        Object[][] values2 = {
                { CircuitBreakerType.fielddata.toString(),
                        0, 2, 19607637196d } };
        circuitBreakerValuesSnap1.insertMultiRows(values2);

        // The 3rd parameter is windowEndTime.
        // So current time is 11000. But we use PerformanceAnalyzerMetrics.getTimeInterval to
        // compute the aligned reader window time: 10000.
        // So our aligned window time is [5000,10000].
        MemoryDBSnapshot circuitFinal = new MemoryDBSnapshot(conn,
                MetricName.CIRCUIT_BREAKER, 10000L, true);
        circuitFinal.alignWindow(circuitBreakerValuesSnap1,
                circuitBreakerValuesSnap2, 7000L, 5000L, 10000L);

        Result<Record> res = circuitFinal.fetchMetric(
                getDimensionEqCondition(CircuitBreakerDimension.CB_TYPE,
                        CircuitBreakerType.fielddata.toString()),
                fields);
        Double estimated = Double
                .parseDouble(res.get(0).get(fields[0]).toString());

        assertEquals(estimated, 0, 0.001);

        Double tripped = Double.parseDouble(
                res.get(0).get(fields[2]).toString());
        assertEquals(tripped, 1.5, 0.001);

        Double limit = Double.parseDouble(
                res.get(0).get(fields[1]).toString());
        assertEquals(limit, 19607637196d, 0.001);
    }
}
