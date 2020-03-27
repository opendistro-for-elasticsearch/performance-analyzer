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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.config;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.core.Util;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class SettingsHelper {
  public static Properties getSettings(final String fileRelativePath) throws IOException {
    Properties prop = new Properties();

    try (InputStream input = new FileInputStream(Util.PLUGIN_LOCATION + fileRelativePath); ) {
      // load a properties file
      prop.load(input);
    }

    return prop;
  }
}
