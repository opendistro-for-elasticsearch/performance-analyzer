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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.jvm;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GCMetrics {
  private static GarbageCollectorMXBean fullGC = null;
  private static GarbageCollectorMXBean youngGC = null;

  private static long totYoungGCCollectionCount = 0;
  private static long totYoungGCCollectionTime = 0;
  private static long totFullGCCollectionCount = 0;
  private static long totFullGCCollectionTime = 0;

  private static long lastYoungGCCollectionCount = 0;
  private static long lastYoungGCCollectionTime = 0;
  private static long lastFullGCCollectionCount = 0;
  private static long lastFullGCCollectionTime = 0;
  private static final Logger LOGGER = LogManager.getLogger(GCMetrics.class);

  static {
    for (GarbageCollectorMXBean item : ManagementFactory.getGarbageCollectorMXBeans()) {
      if ("ConcurrentMarkSweep".equals(item.getName())
          || "MarkSweepCompact".equals(item.getName())
          || "PS MarkSweep".equals(item.getName())
          || "G1 Old Generation".equals(item.getName())
          || "Garbage collection optimized for short pausetimes Old Collector"
              .equals(item.getName())
          || "Garbage collection optimized for throughput Old Collector".equals(item.getName())
          || "Garbage collection optimized for deterministic pausetimes Old Collector"
              .equals(item.getName())) {
        fullGC = item;
      } else if ("ParNew".equals(item.getName())
          || "Copy".equals(item.getName())
          || "PS Scavenge".equals(item.getName())
          || "G1 Young Generation".equals(item.getName())
          || "Garbage collection optimized for short pausetimes Young Collector"
              .equals(item.getName())
          || "Garbage collection optimized for throughput Young Collector".equals(item.getName())
          || "Garbage collection optimized for deterministic pausetimes Young Collector"
              .equals(item.getName())) {
        youngGC = item;
      } else {
        LOGGER.error("MX bean missing: {}", () -> item.getName());
      }
    }
  }

  public static long getTotYoungGCCollectionCount() {
    return totYoungGCCollectionCount;
  }

  public static long getTotYoungGCCollectionTime() {
    return totYoungGCCollectionTime;
  }

  public static long getTotFullGCCollectionCount() {
    return totFullGCCollectionCount;
  }

  public static long getTotFullGCCollectionTime() {
    return totFullGCCollectionTime;
  }

  private static long getYoungGCCollectionCount() {
    if (youngGC == null) {
      return 0;
    }
    return youngGC.getCollectionCount();
  }

  private static long getYoungGCCollectionTime() {
    if (youngGC == null) {
      return 0;
    }
    return youngGC.getCollectionTime();
  }

  private static long getFullGCCollectionCount() {
    if (fullGC == null) {
      return 0;
    }
    return fullGC.getCollectionCount();
  }

  private static long getFullGCCollectionTime() {
    if (fullGC == null) {
      return 0;
    }
    return fullGC.getCollectionTime();
  }

  public static void runGCMetrics() {
    long YoungGCCollectionCount = getYoungGCCollectionCount();
    long YoungGCCollectionTime = getYoungGCCollectionTime();
    long FullGCCollectionCount = getFullGCCollectionCount();
    long FullGCCollectionTime = getFullGCCollectionTime();

    totYoungGCCollectionCount = YoungGCCollectionCount - lastYoungGCCollectionCount;
    totYoungGCCollectionTime = YoungGCCollectionTime - lastYoungGCCollectionTime;
    totFullGCCollectionCount = FullGCCollectionCount - lastFullGCCollectionCount;
    totFullGCCollectionTime = FullGCCollectionTime - lastFullGCCollectionTime;

    lastYoungGCCollectionCount = YoungGCCollectionCount;
    lastYoungGCCollectionTime = YoungGCCollectionTime;
    lastFullGCCollectionCount = FullGCCollectionCount;
    lastFullGCCollectionTime = FullGCCollectionTime;
  }

  static void printGCMetrics() {
    if (lastYoungGCCollectionCount >= 0) {
      System.out.println(
          "GC:: yC:"
              + getTotYoungGCCollectionCount()
              + " yT:"
              + getTotYoungGCCollectionTime()
              + " oC:"
              + getTotFullGCCollectionCount()
              + " oT:"
              + getTotFullGCCollectionTime());
    }
  }
}
