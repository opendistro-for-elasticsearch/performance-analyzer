/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.NetInterfaceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatExceptionCode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics_generator.linux.LinuxIPMetricsGenerator;
import com.google.common.annotations.VisibleForTesting;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NetworkInterface {
  private static final Logger LOG = LogManager.getLogger(NetworkInterface.class);

  /* Data sources:
   /proc/net/snmp, /prov/net/snmp6, /proc/net/dev
   measures tcp and ip-layer pathologies.
   SNMP fields of interest (see RFCs 2011 and 1213):
   - [ip6]inReceives: total including errors
   - [ip6]inDelivers: sent to next layer (including ICMP)
   - [ip6]outRequests: sent from previous layer
   - [ip6]outDiscards + [ip6]outNoRoutes: sender-side drops
  */

  static class NetInterfaceMetrics {
    Map<String, Long> PHYmetrics = new HashMap<>();
    Map<String, Long> IPmetrics = new HashMap<>();
    // these three are currently unused;
    // leaving them commented for now.
    /*Map<String, Long> TCPmetrics =
        new HashMap<>();
    Map<String, Long> UDPmetrics =
        new HashMap<>();
    Map<String, Long> ICMPmetrics =
        new HashMap<>();*/

    public void clearAll() {
      PHYmetrics.clear();
      IPmetrics.clear();
      /*TCPmetrics.clear();
      UDPmetrics.clear();
      ICMPmetrics.clear();*/
    }

    public void putAll(NetInterfaceMetrics m) {
      PHYmetrics.putAll(m.PHYmetrics);
      IPmetrics.putAll(m.IPmetrics);
      /*TCPmetrics.putAll(m.TCPmetrics);
      UDPmetrics.putAll(m.UDPmetrics);
      ICMPmetrics.putAll(m.ICMPmetrics);*/
    }
  }

  private static NetInterfaceMetrics currentMetrics = new NetInterfaceMetrics();
  private static NetInterfaceMetrics oldMetrics = new NetInterfaceMetrics();
  private static Map<String, Long> currentMetrics6 = new HashMap<>();
  private static Map<String, Long> oldMetrics6 = new HashMap<>();
  private static long kvTimestamp = 0;
  private static long oldkvTimestamp = 0;

  private static StringBuilder ret = new StringBuilder();

  private static String[] IPkeys = null;
  //    static private String[] TCPkeys = null;
  //    static private String[] UDPkeys = null;
  //    static private String[] ICMPkeys = null;

  private static LinuxIPMetricsGenerator linuxIPMetricsGenerator = new LinuxIPMetricsGenerator();

  static {
    addSampleHelper();
  }

  public static LinuxIPMetricsGenerator getLinuxIPMetricsGenerator() {
    return linuxIPMetricsGenerator;
  }

  protected static void calculateNetworkMetrics() {

    if (kvTimestamp <= oldkvTimestamp) {
      linuxIPMetricsGenerator.setInNetworkInterfaceSummary(null);
      linuxIPMetricsGenerator.setOutNetworkInterfaceSummary(null);
      return;
    }

    Map<String, Long> curphy = currentMetrics.PHYmetrics;
    Map<String, Long> curipv4 = currentMetrics.IPmetrics;
    Map<String, Long> oldphy = oldMetrics.PHYmetrics;
    Map<String, Long> oldipv4 = oldMetrics.IPmetrics;

    long nin = curipv4.get("InReceives") - oldipv4.get("InReceives");
    long nout = curipv4.get("OutRequests") - oldipv4.get("OutRequests");
    long delivin = curipv4.get("InDelivers") - oldipv4.get("InDelivers");
    long dropout =
        curipv4.get("OutDiscards")
            + curipv4.get("OutNoRoutes")
            - oldipv4.get("OutDiscards")
            - oldipv4.get("OutNoRoutes");
    long nin6 = currentMetrics6.get("Ip6InReceives") - oldMetrics6.get("Ip6InReceives");
    long nout6 = currentMetrics6.get("Ip6OutRequests") - oldMetrics6.get("Ip6OutRequests");
    long delivin6 = currentMetrics6.get("Ip6InDelivers") - oldMetrics6.get("Ip6InDelivers");
    long dropout6 =
        currentMetrics6.get("Ip6OutDiscards")
            + currentMetrics6.get("Ip6OutNoRoutes")
            - oldMetrics6.get("Ip6OutDiscards")
            - oldMetrics6.get("Ip6OutNoRoutes");

    long timeDelta = kvTimestamp - oldkvTimestamp;
    double inbps = 8 * 1.0e3 * (curphy.get("inbytes") - oldphy.get("inbytes")) / timeDelta;
    double outbps = 8 * 1.0e3 * (curphy.get("outbytes") - oldphy.get("outbytes")) / timeDelta;
    double inPacketRate4 = 1.0e3 * (nin) / timeDelta;
    double outPacketRate4 = 1.0e3 * (nout) / timeDelta;
    double inDropRate4 = 1.0e3 * (nin - delivin) / timeDelta;
    double outDropRate4 = 1.0e3 * (dropout) / timeDelta;
    double inPacketRate6 = 1.0e3 * (nin6) / timeDelta;
    double outPacketRate6 = 1.0e3 * (nout6) / timeDelta;
    double inDropRate6 = 1.0e3 * (nin6 - delivin6) / timeDelta;
    double outDropRate6 = 1.0e3 * (dropout6) / timeDelta;

    NetInterfaceSummary inNetwork =
        new NetInterfaceSummary(
            NetInterfaceSummary.Direction.in,
            inPacketRate4,
            inDropRate4,
            inPacketRate6,
            inDropRate6,
            inbps);

    NetInterfaceSummary outNetwork =
        new NetInterfaceSummary(
            NetInterfaceSummary.Direction.out,
            outPacketRate4,
            outDropRate4,
            outPacketRate6,
            outDropRate6,
            outbps);

    linuxIPMetricsGenerator.setInNetworkInterfaceSummary(inNetwork);
    linuxIPMetricsGenerator.setOutNetworkInterfaceSummary(outNetwork);
  }

  private static void getKeys(String line) {
    if (IPkeys != null) {
      // { && TCPkeys != null &&
      // UDPkeys != null && ICMPkeys != null) {
      return;
    }
    if (line.startsWith("Ip:")) {
      IPkeys = line.split("\\s+");
    } /*else if (line.startsWith("Icmp:")) {
          ICMPkeys = line.split("\\s+");
      } else if (line.startsWith("Tcp:")) {
          TCPkeys = line.split("\\s+");
      } else if (line.startsWith("Udp:")) {
          UDPkeys = line.split("\\s+");
      }*/
  }

  private static void generateMap(String line) {
    Map<String, Long> map = null;
    String[] keys = null;
    if (line.startsWith("Ip:")) {
      map = currentMetrics.IPmetrics;
      keys = IPkeys;
    } /*else if (line.startsWith("Icmp:")) {
          map = currentMetrics.ICMPmetrics;
          keys = ICMPkeys;
      } else if (line.startsWith("Tcp:")) {
          map = currentMetrics.TCPmetrics;
          keys = TCPkeys;
      } else if (line.startsWith("Udp:")) {
          map = currentMetrics.UDPmetrics;
          keys = UDPkeys;
      }*/
    if (keys != null) {
      generateMap(line, keys, map);
    }
  }

  private static void generateMap(String line, String[] keys, Map<String, Long> map) {
    String[] values = line.split("\\s+");
    int count = values.length;
    map.put(keys[0], 0L);
    for (int i = 1; i < count; i++) {
      map.put(keys[i], Long.parseLong(values[i]));
    }
  }

  private static void addSample4() {
    int ln = 0;

    oldMetrics.clearAll();
    oldMetrics.putAll(currentMetrics);
    currentMetrics.clearAll();
    oldkvTimestamp = kvTimestamp;
    kvTimestamp = System.currentTimeMillis();

    try (FileReader fileReader = new FileReader(new File("/proc/net/snmp"));
        BufferedReader bufferedReader = new BufferedReader(fileReader); ) {
      String line = null;
      while ((line = bufferedReader.readLine()) != null) {
        if (ln % 2 == 0) { // keys
          getKeys(line);
        } else {
          generateMap(line);
        }
        ln++;
      }
    } catch (Exception e) {
      LOG.debug(
          "Exception in calling addSample4 with details: {} with ExceptionCode: {}",
          () -> e.toString(),
          () -> StatExceptionCode.NETWORK_COLLECTION_ERROR.toString());
      StatsCollector.instance().logException(StatExceptionCode.NETWORK_COLLECTION_ERROR);
    }
  }

  private static void addSample6() {
    oldMetrics6.clear();
    oldMetrics6.putAll(currentMetrics6);
    currentMetrics6.clear();

    try (FileReader fileReader = new FileReader(new File("/proc/net/snmp6"));
        BufferedReader bufferedReader = new BufferedReader(fileReader); ) {
      String line = null;
      while ((line = bufferedReader.readLine()) != null) {
        String[] toks = line.split("[ \\t]+");
        if (toks.length > 1) {
          currentMetrics6.put(toks[0], Long.parseLong(toks[1]));
        }
      }
    } catch (Exception e) {
      LOG.debug(
          "Exception in calling addSample6 with details: {} with ExceptionCode: {}",
          () -> e.toString(),
          () -> StatExceptionCode.NETWORK_COLLECTION_ERROR.toString());
      StatsCollector.instance().logException(StatExceptionCode.NETWORK_COLLECTION_ERROR);
    }
  }

  // this assumes that addSample4() is called
  private static void addDeviceStats() {
    try (FileReader fileReader = new FileReader(new File("/proc/net/dev"));
        BufferedReader bufferedReader = new BufferedReader(fileReader); ) {
      String line = null;
      long intotbytes = 0;
      long outtotbytes = 0;
      long intotpackets = 0;
      long outtotpackets = 0;
      while ((line = bufferedReader.readLine()) != null) {
        if (line.contains("Receive") || line.contains("packets")) {
          continue;
        }
        String[] toks = line.trim().split(" +");
        intotbytes += Long.parseLong(toks[1]);
        intotpackets += Long.parseLong(toks[2]);
        outtotbytes += Long.parseLong(toks[9]);
        outtotpackets += Long.parseLong(toks[10]);
      }
      currentMetrics.PHYmetrics.put("inbytes", intotbytes);
      currentMetrics.PHYmetrics.put("inpackets", intotpackets);
      currentMetrics.PHYmetrics.put("outbytes", outtotbytes);
      currentMetrics.PHYmetrics.put("outpackets", outtotpackets);
    } catch (Exception e) {
      LOG.debug(
          "Exception in calling addDeviceStats with details: {} with ExceptionCode: {}",
          () -> e.toString(),
          () -> StatExceptionCode.NETWORK_COLLECTION_ERROR.toString());
      StatsCollector.instance().logException(StatExceptionCode.NETWORK_COLLECTION_ERROR);
    }
  }

  public static void addSample() {
    addSampleHelper();
    calculateNetworkMetrics();
  }

  private static synchronized void addSampleHelper() {
    addSample4();
    addSample6();
    addDeviceStats();
  }

  public static void runOnce() {
    addSample();
  }

  @VisibleForTesting
  Map<String, Long> getCurrentPhyMetric() {
    return currentMetrics.PHYmetrics;
  }

  @VisibleForTesting
  Map<String, Long> getCurrentIpMetric() {
    return currentMetrics.IPmetrics;
  }

  @VisibleForTesting
  Map<String, Long> getOldPhyMetric() {
    return oldMetrics.PHYmetrics;
  }

  @VisibleForTesting
  Map<String, Long> getOldIpMetric() {
    return oldMetrics.IPmetrics;
  }

  @VisibleForTesting
  Map<String, Long> getCurrentMetrics6() {
    return currentMetrics6;
  }

  @VisibleForTesting
  Map<String, Long> getOldMetrics6() {
    return oldMetrics6;
  }

  @VisibleForTesting
  void putCurrentPhyMetric(String key, Long value) {
    currentMetrics.PHYmetrics.put(key, value);
  }

  @VisibleForTesting
  void putCurrentIpMetric(String key, Long value) {
    currentMetrics.IPmetrics.put(key, value);
  }

  @VisibleForTesting
  void putOldPhyMetric(String key, Long value) {
    oldMetrics.PHYmetrics.put(key, value);
  }

  @VisibleForTesting
  void putOldIpMetric(String key, Long value) {
    oldMetrics.IPmetrics.put(key, value);
  }

  @VisibleForTesting
  void putCurrentMetrics6(String key, Long value) {
    currentMetrics6.put(key, value);
  }

  @VisibleForTesting
  void putOldMetrics6(String key, Long value) {
    oldMetrics6.put(key, value);
  }

  @VisibleForTesting
  static void setKvTimestamp(long value) {
    NetworkInterface.kvTimestamp = value;
  }

  @VisibleForTesting
  static void setOldkvTimestamp(long oldkvTimestamp) {
    NetworkInterface.oldkvTimestamp = oldkvTimestamp;
  }

  @VisibleForTesting
  static long getKvTimestamp() {
    return kvTimestamp;
  }

  @VisibleForTesting
  static long getOldkvTimestamp() {
    return oldkvTimestamp;
  }
}
