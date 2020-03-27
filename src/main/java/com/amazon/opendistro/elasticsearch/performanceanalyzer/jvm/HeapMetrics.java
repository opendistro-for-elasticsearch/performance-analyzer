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

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class HeapMetrics {
  private static final Map<String, Supplier<MemoryUsage>> memoryUsageSuppliers;

  static {
    memoryUsageSuppliers = new HashMap<>();
    MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    if (memoryMXBean != null) {
      memoryUsageSuppliers.put("Heap", () -> memoryMXBean.getHeapMemoryUsage());
      memoryUsageSuppliers.put("NonHeap", () -> memoryMXBean.getNonHeapMemoryUsage());
    }

    List<MemoryPoolMXBean> list = ManagementFactory.getMemoryPoolMXBeans();
    for (MemoryPoolMXBean item : list) {
      if ("CMS Perm Gen".equals(item.getName())
          || "Perm Gen".equals(item.getName())
          || "PS Perm Gen".equals(item.getName())
          || "G1 Perm Gen".equals(item.getName())
          || "Metaspace".equals(item.getName())) {
        memoryUsageSuppliers.put("PermGen", () -> item.getUsage());
      } else if ("CMS Old Gen".equals(item.getName())
          || "Tenured Gen".equals(item.getName())
          || "PS Old Gen".equals(item.getName())
          || "G1 Old Gen".equals(item.getName())) {
        memoryUsageSuppliers.put("OldGen", () -> item.getUsage());
      } else if ("Par Eden Space".equals(item.getName())
          || "Eden Space".equals(item.getName())
          || "PS Eden Space".equals(item.getName())
          || "G1 Eden".equals(item.getName())) {
        memoryUsageSuppliers.put("Eden", () -> item.getUsage());
      } else if ("Par Survivor Space".equals(item.getName())
          || "Survivor Space".equals(item.getName())
          || "PS Survivor Space".equals(item.getName())
          || "G1 Survivor".equals(item.getName())) {
        memoryUsageSuppliers.put("Survivor", () -> item.getUsage());
      }
    }
  }

  public static Map<String, Supplier<MemoryUsage>> getMemoryUsageSuppliers() {
    return memoryUsageSuppliers;
  }
}
