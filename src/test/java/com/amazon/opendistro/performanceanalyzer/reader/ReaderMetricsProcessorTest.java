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

import com.amazon.opendistro.performanceanalyzer.util.CopyTestResource;
import org.jooq.Record;
import org.jooq.Result;
import org.junit.Test;

import java.io.File;
import java.io.FilenameFilter;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;

// This test will be skipped in gradle
public class ReaderMetricsProcessorTest extends AbstractReaderTests {

    public ReaderMetricsProcessorTest() throws SQLException, ClassNotFoundException {
        super();
    }

    @Test
    public void testReaderMetricsProcessorFrequently() throws Exception {

        try (CopyTestResource testResource = new CopyTestResource("build/private/test_resources/dev/shm",
                "build/private/test_resources/dev/shm_processor_testFrequently")) {
            String rootLocation = testResource.getPath();
            deleteAll();
            ReaderMetricsProcessor mp = new ReaderMetricsProcessor(rootLocation);

            for (long i = 1535065139000L; i <= 1535065326500L; i += 2500) {
                Map<String, Long> orgModified = new HashMap<>();
                final long time = i;
                if (i % 1000 == 0) {
                    walkFiles(rootLocation, time, (File file) -> {
                        orgModified.put(file.getAbsolutePath(), file.lastModified());
                        return file.setLastModified(time - 2000);
                    });
                }

                mp.processMetrics(rootLocation, i);

                if (i % 1000 == 0) {
                    walkFiles(rootLocation, time, (File file) ->
                            file.setLastModified(orgModified.get(file.getAbsolutePath())));
                }
            }

            Result<Record> res = mp.getMetricsDB().getValue().queryMetric(Arrays.asList("CPU_Utilization"),
                    Arrays.asList("sum"),
                    Arrays.asList("ShardID", "IndexName", "Operation"));
            Double shardFetchCpu = 0d;
            for (Record record : res) {
                if (record.get("Operation").equals("shardfetch")) {
                    shardFetchCpu = Double.parseDouble(record.get("CPU_Utilization").toString());
                    break;
                }
            }

            assertEquals(0D, shardFetchCpu.doubleValue(), 0.001);

            mp.trimOldSnapshots();
            mp.deleteDBs();
        }
    }

    @Test
    public void testReaderMetricsProcessorFrequentlyWithDelay() throws Exception {
        try (CopyTestResource testResource = new CopyTestResource("build/private/test_resources/dev/shm",
                "build/private/test_resources/dev/shm_processor_testFrequentlyWithDelay")) {

            String rootLocation = testResource.getPath();
            deleteAll();
            int delay = 0;
            ReaderMetricsProcessor mp = new ReaderMetricsProcessor(rootLocation);

            for (long i = 1535065139000L; i <= 1535065326500L; i += 2500) {
                Map<String, Long> orgModified = new HashMap<>();
                final long time = i;
                if (i % 1000 == 0) {
                    walkFiles(rootLocation, time, (File file) -> {
                        orgModified.put(file.getAbsolutePath(), file.lastModified());
                        return file.setLastModified(time - 2000);
                    });
                }

                mp.processMetrics(rootLocation, i + delay);
                delay = (delay + 1000) % 4000;

                if (i % 1000 == 0) {
                    walkFiles(rootLocation, time, (File file) ->
                            file.setLastModified(orgModified.get(file.getAbsolutePath())));
                }
            }

            Result<Record> res = mp.getMetricsDB().getValue().queryMetric(Arrays.asList("CPU_Utilization"),
                    Arrays.asList("sum"),
                    Arrays.asList("ShardID", "IndexName", "Operation"));
            Double shardFetchCpu = 0d;
            for (Record record : res) {
                if (record.get("Operation").equals("shardfetch")) {
                    shardFetchCpu = Double.parseDouble(record.get("CPU_Utilization").toString());
                    break;
                }
            }

            assertEquals(0D, shardFetchCpu.doubleValue(), 0.001);

            mp.trimOldSnapshots();
            mp.deleteDBs();
        }
    }


    public void deleteAll() {
        final File folder = new File("/tmp");
        final File[] files = folder.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(final File dir, final String name) {
                return name.matches("metricsdb_.*");
            }
        });
        for (final File file : files) {
            if (!file.delete()) {
                System.err.println("Can't remove " + file.getAbsolutePath());
            }
        }
    }

    private void walkFiles(String rootLocation, long time, Function<File, Boolean> fun) {
        long startTimeThirtySecondBucket = time / 30000 * 30000;

        File threadsFile = new File(rootLocation + File.separator
                + startTimeThirtySecondBucket + File.separator
                + "threads");

        if (threadsFile == null || threadsFile.listFiles() == null) {
            return;
        }

        for (File threadIDFile : threadsFile.listFiles()) {
            if (!threadIDFile.getName().equals("http")) {

                for (File opFile : threadIDFile.listFiles()) {
                    if (opFile.getName().equals("os_metrics")) {
                        fun.apply(opFile);
                    }
                }
            }
        }
    }

}

