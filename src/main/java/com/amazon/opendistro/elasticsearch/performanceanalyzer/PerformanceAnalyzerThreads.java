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

package com.amazon.opendistro.elasticsearch.performanceanalyzer;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatExceptionCode;

/**
 * Enum of threads that are spawned by Performance Analyzer agent. Each enum value encapsulates two
 * properties of every PA thread. 1) Name of the thread. 2) Counter name against which error metrics
 * need to be recorded when the thread runs into an unhandled exception.
 */
public enum PerformanceAnalyzerThreads {
  PA_READER("pa-reader", StatExceptionCode.READER_THREAD_STOPPED),
  PA_ERROR_HANDLER("pa-error-handler", StatExceptionCode.ERROR_HANDLER_THREAD_STOPPED),
  GRPC_SERVER("grpc-server", StatExceptionCode.GRPC_SERVER_THREAD_STOPPED),
  WEB_SERVER("web-server", StatExceptionCode.WEB_SERVER_THREAD_STOPPED),
  RCA_CONTROLLER("rca-controller", StatExceptionCode.RCA_CONTROLLER_THREAD_STOPPED),
  RCA_SCHEDULER("rca-scheduler", StatExceptionCode.RCA_SCHEDULER_THREAD_STOPPED);

  private final String value;
  private final StatExceptionCode threadExceptionCode;

  PerformanceAnalyzerThreads(final String value, final StatExceptionCode threadExceptionCode) {
    this.value = value;
    this.threadExceptionCode = threadExceptionCode;
  }


  /**
   * Returns the name of this enum constant, as contained in the declaration.  This method may be
   * overridden, though it typically isn't necessary or desirable.  An enum type should override
   * this method when a more "programmer-friendly" string form exists.
   *
   * @return the name of this enum constant
   */
  @Override
  public String toString() {
    return value;
  }

  /**
   * Returns the exception metric name.
   *
   * @return the name of the counter.
   */
  public StatExceptionCode getThreadExceptionCode() {
    return threadExceptionCode;
  }
}
