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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.reader;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;
import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class FileHandler {
  private static final String[] EMPTY_STRING_ARRAY = new String[0];

  private String rootLocation;

  // find all relevant files for a metric
  public abstract List<File> findFiles4Metric(long timeBucket);

  public abstract List<Event> getMetricData(Map<String, List<Event>> metricDataMap);

  FileHandler() {
    this.rootLocation = PluginSettings.instance().getMetricsLocation();
  }

  public String[] processExtraDimensions(File file) throws IOException {
    if (filePathRegex().isEmpty()) {
      return EMPTY_STRING_ARRAY;
    }

    // Note the question mark in the 1st group is reluctant
    // quantifier.
    Pattern pattern = Pattern.compile(filePathRegex());
    // our regex uses '/' as file separator
    Matcher matcher = pattern.matcher(file.getCanonicalPath());
    if (matcher.find()) {
      int groupCount = matcher.groupCount();
      String[] extraDimensions = new String[groupCount];
      // group 0 is the entire match
      for (int i = 1; i <= groupCount; i++) {
        extraDimensions[i - 1] = matcher.group(i);
      }
      return extraDimensions;
    }
    throw new IOException(String.format("Cannot find a matching path %s", file.getCanonicalPath()));
  }

  // For something like
  // indices/nyc_taxis/9
  // The intention of this method to get:
  // [nyc_taxis, 9]
  String[] processExtraDimensions(String key) {
    if (filePathRegex().isEmpty()) {
      return EMPTY_STRING_ARRAY;
    }
    String[] temp = key.split(File.separatorChar == '\\' ? "\\\\" : File.separator);
    return Arrays.copyOfRange(temp, 1, temp.length);
  }

  // override this method if we need to extra dimensions from the file
  // path
  protected String filePathRegex() {
    return "";
  }

  public String getRootLocation() {
    return rootLocation;
  }

  @VisibleForTesting
  void setRootLocation(String location) {
    rootLocation = location;
  }
}
