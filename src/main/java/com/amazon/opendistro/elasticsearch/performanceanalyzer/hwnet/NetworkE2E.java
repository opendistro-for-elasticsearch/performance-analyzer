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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.hwnet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics_generator.linux.LinuxTCPMetricsGenerator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.os.OSGlobals;
import com.google.common.annotations.VisibleForTesting;

public class NetworkE2E {
    /* Data sources:
      /proc/net/tcp, /proc/net/tcp6 and /proc/pid/fd/*
      intersection of these gives a list of flows
      owned by the process. net/tcp gives metrics
      (by src-dest pair) around queues, retx's
      and TCP sndwnd.
     */

    private static final Logger LOG = LogManager.getLogger(
            NetworkE2E.class);
    private static String pid = OSGlobals.getPid();

    static class TCPFlowMetrics {
        String destIP;

        long txQueue;
        long rxQueue;
        long currentLost;
        long sendCWND;
        long SSThresh;
    }

    static class destTCPFlowMetrics {
        long txQueueTot;
        long rxQueueTot;
        long currentLostTot;
        long sendCWNDTot;
        long SSThreshTot;
        int numFlows;
        destTCPFlowMetrics(TCPFlowMetrics m) {
            txQueueTot = m.txQueue;
            rxQueueTot = m.rxQueue;
            currentLostTot = m.currentLost;
            sendCWNDTot = m.sendCWND;
            SSThreshTot = m.SSThresh;
            numFlows = 1;
        }
    }

    private static Set<String> inodeSocketList
        = new HashSet<>();
    private static Map<String, TCPFlowMetrics> inodeFlowMetricsMap
        = new HashMap<>();
    private static Map<String, destTCPFlowMetrics> destnodeFlowMetricsMap
        = new HashMap<>();
    private static LinuxTCPMetricsGenerator linuxTCPMetricsHandler = new LinuxTCPMetricsGenerator();

    private static StringBuilder value = new StringBuilder();

    static void listSockets() {
        File self = new File("/proc/" + pid + "/fd");
        File[] filesList = self.listFiles();
        for (File f : filesList) {
            // no check for file, as this dir is all files/symlinks
            String target = null;
            try {
                Path targetp = Files.readSymbolicLink(Paths.get(f.getCanonicalPath()));
                target = targetp.toString();
            } catch (Exception e) {
                continue;
            }
            if (target.contains("socket:")) {
                target = target.split("socket:\\[")[1];
                target = target.split("\\]")[0];
                inodeSocketList.add(target);
            }
        }
    }

    private static void generateMap(String line, String ver) {
        String[] toks = line.trim().split("\\s+");
        if (!inodeSocketList.contains(toks[9])) { // inode
            return;
        }
        TCPFlowMetrics m = new TCPFlowMetrics();
        m.destIP = toks[2].split(":")[0];
        m.txQueue = Long.decode("0x" + toks[4].split(":")[0]);
        m.rxQueue = Long.decode("0x" + toks[4].split(":")[1]);
        m.currentLost = Long.decode("0x" + toks[6]);
        if (toks.length > 16) {
            m.sendCWND = Long.parseLong(toks[15]);
            m.SSThresh = Long.parseLong(toks[16]);
        } else {
            m.sendCWND = -1;
            m.SSThresh = -1;
        }
        inodeFlowMetricsMap.put(toks[9], m);
    }

    private static void mapTCPMetrics(String ver) {
        int ln = 0;
        try (FileReader fileReader = new FileReader(new File(ver));
             BufferedReader bufferedReader = new BufferedReader(fileReader)) {
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                if (ln != 0) { // first line is keys
                    generateMap(line, ver);
                }
                ln++;
            }
        } catch (Exception e) {
            LOG.debug("Error in mapTCPMetrics: {}", () -> e);
        }
    }

    private static void mapTCPMetrics() {
        mapTCPMetrics("/proc/net/tcp");
        mapTCPMetrics("/proc/net/tcp6");
    }

    private static void clearAll() {
        inodeSocketList.clear();
        inodeFlowMetricsMap.clear();
        destnodeFlowMetricsMap.clear();
    }

    private static void computeSummary() {
        for (String inode : inodeFlowMetricsMap.keySet()) {
            TCPFlowMetrics m = inodeFlowMetricsMap.get(inode);
            destTCPFlowMetrics exist = destnodeFlowMetricsMap.get(m.destIP);
            if (exist == null) {
                destnodeFlowMetricsMap.put(m.destIP, new destTCPFlowMetrics(m));
            } else {
                // check for "-1"s and add to total only if it is not -1
                exist.numFlows++;
                exist.txQueueTot += (m.txQueue != -1 ? m.txQueue : 0);
                exist.rxQueueTot += (m.rxQueue != -1 ? m.rxQueue : 0);
                exist.currentLostTot += (m.currentLost != -1 ? m.currentLost : 0);
                exist.sendCWNDTot += (m.sendCWND != -1 ? m.sendCWND : 0);
                exist.SSThreshTot += (m.SSThresh != -1 ? m.SSThresh : 0);
            }
        }

        calculateTCPMetrics();
    }

    protected static void calculateTCPMetrics() {

        Map<String, double[]> localMap = new HashMap<>();
        for (String dest : destnodeFlowMetricsMap.keySet()) {
            destTCPFlowMetrics m = destnodeFlowMetricsMap.get(dest);

            double[] metrics = new double[6];
            metrics[0] = m.numFlows;
            metrics[1] = m.txQueueTot * 1.0 / m.numFlows;
            metrics[2] = m.rxQueueTot * 1.0 / m.numFlows;
            metrics[3] = m.currentLostTot * 1.0 / m.numFlows;
            metrics[4] = m.sendCWNDTot * 1.0 / m.numFlows;
            metrics[5] = m.SSThreshTot * 1.0 / m.numFlows;

            localMap.put(dest, metrics);
        }

        linuxTCPMetricsHandler.setTCPMetrics(localMap);
    }

    public static LinuxTCPMetricsGenerator getTCPMetricsHandler() {

        return linuxTCPMetricsHandler;
    }

    public static void addSample() {
        clearAll();
        listSockets();
        mapTCPMetrics();
        computeSummary();
    }

    public static void runOnce() {
        clearAll();
        listSockets();
        mapTCPMetrics();
        computeSummary();
    }

    @VisibleForTesting
    protected static void setDestnodeFlowMetricsMap(
            Map<String, destTCPFlowMetrics> destnodeFlowMetricsMap) {
        NetworkE2E.destnodeFlowMetricsMap = destnodeFlowMetricsMap;
    }
}

