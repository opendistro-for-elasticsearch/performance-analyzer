/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.core;

import java.io.File;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;

public class Util {
  private static final Logger LOG = LogManager.getLogger(Util.class);
  public static final String METRICS_QUERY_URL = "/_opendistro/_performanceanalyzer/metrics";
  public static final String RCA_QUERY_URL = "/_opendistro/_performanceanalyzer/rca";
  public static final String ES_HOME = System.getProperty("es.path.home");
  // TODO: Make this configurable.
  public static final int RPC_PORT = 9650;
  public static final String PLUGIN_LOCATION =
          ES_HOME
          + File.separator
          + "plugins"
          + File.separator
          + "opendistro_performance_analyzer"
          + File.separator;
  public static final String READER_LOCATION =
          ES_HOME
          + File.separator
          + "performance-analyzer-rca"
          + File.separator;
  public static final String DATA_DIR =
          ES_HOME
          + File.separator
          + "data"
          + File.separator;

  public static void invokePrivileged(Runnable runner) {
    AccessController.doPrivileged(
        (PrivilegedAction<Void>)
            () -> {
              try {
                runner.run();
              } catch (Exception ex) {
                LOG.debug(
                    (Supplier<?>)
                        () ->
                            new ParameterizedMessage(
                                "Privileged Invocation failed {}", ex.toString()),
                    ex);
              }
              return null;
            });
  }

  public static void invokePrivilegedAndLogError(Runnable runner) {
    AccessController.doPrivileged(
        (PrivilegedAction<Void>)
            () -> {
              try {
                runner.run();
              } catch (Exception ex) {
                LOG.error(
                    (Supplier<?>)
                        () ->
                            new ParameterizedMessage(
                                "Privileged Invocation failed {}", ex.toString()),
                    ex);
              }
              return null;
            });
  }
}
