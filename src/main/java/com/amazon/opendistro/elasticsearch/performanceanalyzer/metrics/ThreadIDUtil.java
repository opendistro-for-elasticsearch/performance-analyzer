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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.jvm.ThreadList;

public final class ThreadIDUtil {
  private ThreadIDUtil() {}

  public static final ThreadIDUtil INSTANCE = new ThreadIDUtil();

  public long getNativeCurrentThreadId() {

    return getNativeThreadId(Thread.currentThread().getId());
  }

  public long getNativeThreadId(long jTid) {
    ThreadList.ThreadState threadState1 = ThreadList.getThreadState(jTid);

    long nid = -1;
    if (threadState1 != null) {
      nid = threadState1.nativeTid;
    }

    return nid;
  }
}
