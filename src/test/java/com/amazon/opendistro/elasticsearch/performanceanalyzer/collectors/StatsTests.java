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

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class StatsTests {
    static class AddStatsThread extends Thread {
        LinkedList<StatExceptionCode> exceptionCodeList;
        int startIndex;
        int totalToIterate;
        StatsCollector sc;
        AddStatsThread(LinkedList<StatExceptionCode> exceptionCodeList, int startIndex, int totalToIterate, StatsCollector sc) {
            this.exceptionCodeList = exceptionCodeList;
            this.startIndex = startIndex;
            this.totalToIterate = totalToIterate;
            this.sc = sc;
        }

        @Override
        public void run() {
            StatsTests.iterate(exceptionCodeList, startIndex, totalToIterate, sc);
        }
    }

    static Random RANDOM = new Random();
    private static final int MAX_COUNT = 500;
    private static final int MASTER_METRICS_ERRORS = Math.abs(RANDOM.nextInt()%MAX_COUNT);
    private static final int REQUEST_REMOTE_ERRORS = Math.abs(RANDOM.nextInt()%MAX_COUNT);
    private static final int READER_PARSER_ERRORS = Math.abs(RANDOM.nextInt()%MAX_COUNT);
    private static final int READER_RESTART_PROCESSINGS = Math.abs(RANDOM.nextInt()%MAX_COUNT);
    private static final int OTHERS = Math.abs(RANDOM.nextInt()%MAX_COUNT);
    private static final int TOTAL_ERRORS = MASTER_METRICS_ERRORS + REQUEST_REMOTE_ERRORS + READER_PARSER_ERRORS
                                            + READER_RESTART_PROCESSINGS + OTHERS;
    private static final AtomicInteger DEFAULT_VAL = new AtomicInteger(0);
    private static final int EXEC_COUNT = 20;

    @Test
    public void testStats() throws Exception {
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");

        LinkedList<StatExceptionCode> exceptionCodeList = new LinkedList<>();

        for(int i = 0 ; i < MASTER_METRICS_ERRORS; i++) {
            exceptionCodeList.add(StatExceptionCode.MASTER_METRICS_ERROR);
        }

        for(int i = 0 ; i < REQUEST_REMOTE_ERRORS; i++) {
            exceptionCodeList.add(StatExceptionCode.REQUEST_REMOTE_ERROR);
        }

        for(int i = 0 ; i < READER_PARSER_ERRORS; i++) {
            exceptionCodeList.add(StatExceptionCode.READER_PARSER_ERROR);
        }

        for(int i = 0 ; i < READER_RESTART_PROCESSINGS; i++) {
            exceptionCodeList.add(StatExceptionCode.READER_RESTART_PROCESSING);
        }

        for(int i = 0 ; i < OTHERS; i++) {
            exceptionCodeList.add(null);
        }

        Collections.shuffle(exceptionCodeList);
        StatsCollector sc = StatsCollector.instance();
        int iterateSize = exceptionCodeList.size()/EXEC_COUNT;
        runInSerial(iterateSize,  exceptionCodeList, sc);
        runInParallel(iterateSize,  exceptionCodeList, sc);
    }

    private static void runInSerial(int iterateSize,  LinkedList<StatExceptionCode> exceptionCodeList, StatsCollector sc) {
        int i = 0;
        for(; i < EXEC_COUNT-1; i++) {
            iterate(exceptionCodeList, i * iterateSize, iterateSize, sc);
        }
        iterate(exceptionCodeList, i * iterateSize, exceptionCodeList.size() - i * iterateSize, sc);
        assertExpected(sc);
    }

    private static void assertExpected(StatsCollector sc) {
        assertEquals(sc.getCounters().getOrDefault(StatExceptionCode.MASTER_METRICS_ERROR.toString(), DEFAULT_VAL).get(),
                     MASTER_METRICS_ERRORS);
        assertEquals(sc.getCounters().getOrDefault(StatExceptionCode.REQUEST_REMOTE_ERROR.toString(), DEFAULT_VAL).get(),
                     REQUEST_REMOTE_ERRORS);
        assertEquals(sc.getCounters().getOrDefault(StatExceptionCode.READER_PARSER_ERROR.toString(), DEFAULT_VAL).get(),
                     READER_PARSER_ERRORS);
        assertEquals(sc.getCounters().getOrDefault(StatExceptionCode.READER_RESTART_PROCESSING.toString(), DEFAULT_VAL).get(),
                     READER_RESTART_PROCESSINGS);
        assertEquals(sc.getCounters().getOrDefault(StatExceptionCode.OTHER.toString(), DEFAULT_VAL).get(),
                     OTHERS);
        assertEquals(sc.getCounters().getOrDefault(StatExceptionCode.TOTAL_ERROR.toString(), DEFAULT_VAL).get(), TOTAL_ERRORS);
    }

    private static void runInParallel(int iterateSize,  LinkedList<StatExceptionCode> exceptionCodeList, StatsCollector sc)
      throws Exception {
        sc.getCounters().clear();
        int i = 0;
        Thread[] threads = new Thread[EXEC_COUNT];
        for(; i < EXEC_COUNT-1; i++) {
            threads[i] = new AddStatsThread(exceptionCodeList, i * iterateSize, iterateSize, sc);
        }
        threads[i] = new AddStatsThread(exceptionCodeList, i * iterateSize, exceptionCodeList.size() - i * iterateSize, sc);

        for(i = 0; i < EXEC_COUNT; i++) {
            threads[i].start();
        }

        for(i = 0; i < EXEC_COUNT; i++) {
            threads[i].join();
        }

        assertExpected(sc);
    }

    private static void iterate(LinkedList<StatExceptionCode> exceptionCodeList, int startIndex, int totalToIterate, StatsCollector sc) {
        int count = 0;
        ListIterator<StatExceptionCode> iterator = exceptionCodeList.listIterator(startIndex);

        while(count < totalToIterate && iterator.hasNext()) {
            StatExceptionCode exceptionCode = iterator.next();

            if (exceptionCode != null) { 
                sc.logException(exceptionCode);
            } else {
                sc.logException();
            }
            count++;
        }
    }
}
