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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.os;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics_generator.linux.LinuxCPUPagingActivityGenerator;

public final class ThreadCPU  {
    private static final Logger LOGGER = LogManager.getLogger(ThreadCPU.class);
    public static final ThreadCPU INSTANCE = new ThreadCPU();
    private long scClkTck = 0;
    private String pid = null;
    private List<String> tids = null;
    private Map<String, Map<String, Object>> tidKVMap = new HashMap<>();
    private Map<String, Map<String, Object>> oldtidKVMap = new HashMap<>();
    private long kvTimestamp = 0;
    private long oldkvTimestamp = 0;
    private LinuxCPUPagingActivityGenerator cpuPagingActivityMap = new LinuxCPUPagingActivityGenerator();

    // these two arrays map 1-1
    private static String[] statKeys = {
            "pid",
            "comm",
            "state",
            "ppid",
            "pgrp",
            "session",
            "ttynr",
            "tpgid",
            "flags",
            "minflt",
            "cminflt",
            "majflt",
            "cmajflt",
            "utime",
            "stime",
            "cutime",
            "cstime",
            "prio",
            "nice",
            "nthreads",
            "itrealvalue",
            "starttime",
            "vsize",
            "rss",
            "rsslim",
            "startcode",
            "endcode",
            "startstack",
            "kstkesp",
            "kstkeip",
            "signal",
            "blocked",
            "sigignore",
            "sigcatch",
            "wchan",
            "nswap",
            "cnswap",
            "exitsig",
            "cpu",
            "rtprio",
            "schedpolicy",
            "bio_ticks",
            "vmtime",
            "cvmtime"
            // more that we ignore
    };

    private static SchemaFileParser.FieldTypes[] statTypes = {
            SchemaFileParser.FieldTypes.INT,
            SchemaFileParser.FieldTypes.STRING,
            SchemaFileParser.FieldTypes.CHAR,
            SchemaFileParser.FieldTypes.INT,
            SchemaFileParser.FieldTypes.INT,
            SchemaFileParser.FieldTypes.INT,
            SchemaFileParser.FieldTypes.INT,
            SchemaFileParser.FieldTypes.INT,
            SchemaFileParser.FieldTypes.INT,
            SchemaFileParser.FieldTypes.ULONG, //10
            SchemaFileParser.FieldTypes.ULONG,
            SchemaFileParser.FieldTypes.ULONG,
            SchemaFileParser.FieldTypes.ULONG,
            SchemaFileParser.FieldTypes.ULONG,
            SchemaFileParser.FieldTypes.ULONG,
            SchemaFileParser.FieldTypes.ULONG,
            SchemaFileParser.FieldTypes.ULONG,
            SchemaFileParser.FieldTypes.ULONG,
            SchemaFileParser.FieldTypes.ULONG,
            SchemaFileParser.FieldTypes.ULONG, //20
            SchemaFileParser.FieldTypes.ULONG,
            SchemaFileParser.FieldTypes.ULONG,
            SchemaFileParser.FieldTypes.ULONG,
            SchemaFileParser.FieldTypes.ULONG,
            SchemaFileParser.FieldTypes.ULONG,
            SchemaFileParser.FieldTypes.ULONG,
            SchemaFileParser.FieldTypes.ULONG,
            SchemaFileParser.FieldTypes.ULONG,
            SchemaFileParser.FieldTypes.ULONG,
            SchemaFileParser.FieldTypes.ULONG, //30
            SchemaFileParser.FieldTypes.ULONG,
            SchemaFileParser.FieldTypes.ULONG,
            SchemaFileParser.FieldTypes.ULONG,
            SchemaFileParser.FieldTypes.ULONG,
            SchemaFileParser.FieldTypes.ULONG,
            SchemaFileParser.FieldTypes.ULONG,
            SchemaFileParser.FieldTypes.ULONG,
            SchemaFileParser.FieldTypes.INT,
            SchemaFileParser.FieldTypes.INT,
            SchemaFileParser.FieldTypes.INT, //40
            SchemaFileParser.FieldTypes.INT,
            SchemaFileParser.FieldTypes.INT,
            SchemaFileParser.FieldTypes.INT,
            SchemaFileParser.FieldTypes.INT
    };

    private ThreadCPU() {
        try {
            pid = OSGlobals.getPid();
            scClkTck = OSGlobals.getScClkTck();
            tids = OSGlobals.getTids();
        } catch (Exception e) {
            LOGGER.error(
                    (Supplier<?>) () -> new ParameterizedMessage(
                            "Error In Initializing ThreadCPU: {}",
                            e.toString()),
                    e);
        }
    }

    public synchronized void addSample() {
        tids = OSGlobals.getTids();

        oldtidKVMap.clear();
        oldtidKVMap.putAll(tidKVMap);

        tidKVMap.clear();
        oldkvTimestamp = kvTimestamp;
        kvTimestamp = System.currentTimeMillis();
        for (String tid : tids) {
            Map<String, Object> sample =
                    //(new SchemaFileParser("/proc/"+tid+"/stat",
                    (new SchemaFileParser("/proc/" + pid + "/task/" + tid + "/stat", statKeys, statTypes, true)).parse();
            tidKVMap.put(tid, sample);
        }

        calculateCPUDetails();
        calculatePagingActivity();
    }

    private void calculateCPUDetails() {
        if (oldkvTimestamp == kvTimestamp) {
            return;
        }

        for (String tid : tidKVMap.keySet()) {
            Map<String, Object> v = tidKVMap.get(tid);
            Map<String, Object> oldv = oldtidKVMap.get(tid);
            if (v != null && oldv != null) {
                if (!v.containsKey("utime") || !oldv.containsKey("utime")) {
                    continue;
                }
                long diff = ((long) (v.getOrDefault("utime", 0L)) - (long) (oldv.getOrDefault("utime", 0L)))
                        + ((long) (v.getOrDefault("stime", 0L)) - (long) (oldv.getOrDefault("stime", 0L)));
                double util = (1.0e3 * diff / scClkTck) / (kvTimestamp - oldkvTimestamp);
                cpuPagingActivityMap.setCPUUtilization(tid, util);
            }
        }
    }

    /**
     * Note: major faults include mmap()'ed accesses
     *
     */
    private void calculatePagingActivity() {
        if (oldkvTimestamp == kvTimestamp) {
            return;
        }


        for (String tid : tidKVMap.keySet()) {
            Map<String, Object> v = tidKVMap.get(tid);
            Map<String, Object> oldv = oldtidKVMap.get(tid);
            if (v != null && oldv != null) {
                if (!v.containsKey("majflt") || !oldv.containsKey("majflt")) {
                    continue;
                }
                double majdiff = ((long) (v.getOrDefault("majflt", 0L)) - (long) (oldv.getOrDefault("majflt", 0L)));
                majdiff /= 1.0e-3 * (kvTimestamp - oldkvTimestamp);
                double mindiff = ((long) (v.getOrDefault("minflt", 0L)) - (long) (oldv.getOrDefault("minflt", 0L)));
                mindiff /= 1.0e-3 * (kvTimestamp - oldkvTimestamp);

                Double[] fltarr = {majdiff, mindiff, (double) ((long) v.getOrDefault("rss", 0L))};
                cpuPagingActivityMap.setPagingActivities(tid, fltarr);
            }
        }
    }

    public LinuxCPUPagingActivityGenerator getCPUPagingActivity() {

        return cpuPagingActivityMap;
    }
}

