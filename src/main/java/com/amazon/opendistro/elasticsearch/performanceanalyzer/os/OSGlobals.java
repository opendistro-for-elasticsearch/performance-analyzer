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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ConfigStatus;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;

public class OSGlobals {
    private static long scClkTck;
    private static String pid;
    private static final String CLK_TCK_SYS_PROPERTY_NAME = "clk.tck";

    private static final Logger LOGGER = LogManager.getLogger(OSGlobals.class);
    private static final long REFRESH_INTERVAL_MS = MetricsConfiguration.CONFIG_MAP.get(OSGlobals.class).samplingInterval;
    private static List<String> tids = new ArrayList<>();
    private static long lastUpdated = -1;

    static {
        try {
            pid = new File("/proc/self").getCanonicalFile().getName();
            getScClkTckFromConfig();
            enumTids();
            lastUpdated = System.currentTimeMillis();
        } catch (Exception e) {
            LOGGER.error(
                    (Supplier<?>) () -> new ParameterizedMessage(
                            "Error in static initialization of OSGlobals with exception: {}",
                            e.toString()),
                    e);
        }
    }

    public static String getPid() {
        return pid;
    }

    public static long getScClkTck() {
        return scClkTck;
    }

    private static void getScClkTckFromConfig() throws Exception {
        try {
            scClkTck = Long.parseUnsignedLong(System.getProperty(CLK_TCK_SYS_PROPERTY_NAME));
        } catch (Exception e) {
            LOGGER.error(
                    (Supplier<?>) () -> new ParameterizedMessage(
                            "Error in reading/parsing clk.tck value: {}",
                            e.toString()),
                    e);
            ConfigStatus.INSTANCE.setConfigurationInvalid();
        }
    }

    private static void enumTids() {
        tids.clear();
        tids.add(pid);

        File self = new File("/proc/self/task");
        File[] filesList = self.listFiles();
        for (File f : filesList) {
            if (f.isDirectory()) {
                String tid = f.getName();
                tids.add(tid);
            }
        }
    }

    static synchronized List<String> getTids() {
        long curtime = System.currentTimeMillis();
        if (curtime - lastUpdated > REFRESH_INTERVAL_MS) {
            enumTids();
            lastUpdated = curtime;
        }
        return new ArrayList<>(tids);
    }
}

