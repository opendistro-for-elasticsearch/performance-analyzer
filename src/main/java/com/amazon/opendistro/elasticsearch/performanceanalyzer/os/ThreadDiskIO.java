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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.os;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatExceptionCode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics_generator.linux.LinuxDiskIOMetricsGenerator;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ThreadDiskIO {
  private static String pid = OSGlobals.getPid();
  private static List<String> tids = null;
  private static final Logger LOGGER = LogManager.getLogger(ThreadDiskIO.class);

  private static Map<String, Map<String, Long>> tidKVMap = new HashMap<>();
  private static Map<String, Map<String, Long>> oldtidKVMap = new HashMap<>();
  private static long kvTimestamp = 0;
  private static long oldkvTimestamp = 0;

  public static class IOMetrics {
    public double avgReadThroughputBps;
    public double avgWriteThroughputBps;
    public double avgTotalThroughputBps;

    public double avgReadSyscallRate;
    public double avgWriteSyscallRate;
    public double avgTotalSyscallRate;

    public double avgPageCacheReadThroughputBps;
    public double avgPageCacheWriteThroughputBps;
    public double avgPageCacheTotalThroughputBps;

    @SuppressWarnings("checkstyle:parameternumber")
    IOMetrics(
        double avgReadThroughputBps,
        double avgReadSyscallRate,
        double avgWriteThroughputBps,
        double avgWriteSyscallRate,
        double avgTotalThroughputBps,
        double avgTotalSyscallRate,
        double avgPageCacheReadThroughputBps,
        double avgPageCacheWriteThroughputBps,
        double avgPageCacheTotalThroughputBps) {
      this.avgReadThroughputBps = avgReadThroughputBps;
      this.avgWriteThroughputBps = avgWriteThroughputBps;
      this.avgTotalThroughputBps = avgTotalThroughputBps;
      this.avgReadSyscallRate = avgReadSyscallRate;
      this.avgWriteSyscallRate = avgWriteSyscallRate;
      this.avgTotalSyscallRate = avgTotalSyscallRate;
      this.avgPageCacheReadThroughputBps = avgPageCacheReadThroughputBps;
      this.avgPageCacheWriteThroughputBps = avgPageCacheWriteThroughputBps;
      this.avgPageCacheTotalThroughputBps = avgPageCacheTotalThroughputBps;
    }

    public String toString() {
      return new StringBuilder()
          .append("rBps:")
          .append(avgReadThroughputBps)
          .append(" wBps:")
          .append(avgWriteThroughputBps)
          .append(" totBps:")
          .append(avgTotalThroughputBps)
          .append(" rSysc:")
          .append(avgReadSyscallRate)
          .append(" wSysc:")
          .append(avgWriteSyscallRate)
          .append(" totSysc:")
          .append(avgTotalSyscallRate)
          .append(" rPcBps:")
          .append(avgPageCacheReadThroughputBps)
          .append(" wPcBps:")
          .append(avgPageCacheWriteThroughputBps)
          .append(" totPcBps:")
          .append(avgPageCacheTotalThroughputBps)
          .toString();
    }
  }

  private static void addSampleTid(String tid) {
    try (FileReader fileReader = new FileReader(new File("/proc/" + pid + "/task/" + tid + "/io"));
        BufferedReader bufferedReader = new BufferedReader(fileReader); ) {
      String line = null;
      Map<String, Long> kvmap = new HashMap<>();
      while ((line = bufferedReader.readLine()) != null) {
        String[] toks = line.split("[: ]+");
        String key = toks[0];
        long val = Long.parseLong(toks[1]);
        kvmap.put(key, val);
      }
      tidKVMap.put(tid, kvmap);
    } catch (FileNotFoundException e) {
      LOGGER.debug("FileNotFound in parse with exception: {}", () -> e.toString());
    } catch (Exception e) {
      LOGGER.debug(
          "Error In addSample Tid for: {}  with error: {} with ExceptionCode: {}",
          () -> tid,
          () -> e.toString(),
          () -> StatExceptionCode.THREAD_IO_ERROR.toString());
      StatsCollector.instance().logException(StatExceptionCode.THREAD_IO_ERROR);
    }
  }

  public static synchronized void addSample() {
    tids = OSGlobals.getTids();
    oldtidKVMap.clear();
    oldtidKVMap.putAll(tidKVMap);

    tidKVMap.clear();
    oldkvTimestamp = kvTimestamp;
    kvTimestamp = System.currentTimeMillis();
    for (String tid : tids) {
      addSampleTid(tid);
    }
  }

  public static synchronized LinuxDiskIOMetricsGenerator getIOUtilization() {

    LinuxDiskIOMetricsGenerator linuxDiskIOMetricsHandler = new LinuxDiskIOMetricsGenerator();
    if (oldkvTimestamp == kvTimestamp) {
      return linuxDiskIOMetricsHandler;
    }

    for (Map.Entry<String, Map<String, Long>> entry : tidKVMap.entrySet()) {
      Map<String, Long> v = entry.getValue();
      Map<String, Long> oldv = oldtidKVMap.get(entry.getKey());
      if (v != null && oldv != null) {
        double duration = 1.0e-3 * (kvTimestamp - oldkvTimestamp);
        double readBytes = v.get("read_bytes") - oldv.get("read_bytes");
        double writeBytes = v.get("write_bytes") - oldv.get("write_bytes");
        double readSyscalls = v.get("syscr") - oldv.get("syscr");
        double writeSyscalls = v.get("syscw") - oldv.get("syscw");
        double readPcBytes = v.get("rchar") - oldv.get("rchar") - readBytes;
        double writePcBytes = v.get("wchar") - oldv.get("wchar") - writeBytes;
        readBytes /= duration;
        readSyscalls /= duration;
        writeBytes /= duration;
        writeSyscalls /= duration;
        readPcBytes /= duration;
        writePcBytes /= duration;

        linuxDiskIOMetricsHandler.setDiskIOMetrics(
            entry.getKey(),
            new IOMetrics(
                readBytes,
                readSyscalls,
                writeBytes,
                writeSyscalls,
                readBytes + writeBytes,
                readSyscalls + writeSyscalls,
                readPcBytes,
                writePcBytes,
                readPcBytes + writePcBytes));
      }
    }
    return linuxDiskIOMetricsHandler;
  }
}
