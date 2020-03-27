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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import java.io.File;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MetricsPurgeActivity extends PerformanceAnalyzerMetricsCollector {
  private static final Logger LOG = LogManager.getLogger(MetricsPurgeActivity.class);

  public MetricsPurgeActivity() {
    super(
        MetricsConfiguration.CONFIG_MAP.get(MetricsPurgeActivity.class).samplingInterval,
        "MetricsPurgeActivity");
  }

  private static int purgeInterval =
      MetricsConfiguration.CONFIG_MAP.get(MetricsPurgeActivity.class).deletionInterval;

  @Override
  public void collectMetrics(long startTime) {
    deleteEventLogFiles(startTime);
  }

  private void deleteEventLogFiles(long referenceTime) {
    LOG.debug("Starting to delete old writer files");
    File root = new File(PluginSettings.instance().getMetricsLocation());
    String[] children = root.list();
    if (children == null) {
      return;
    }
    int filesDeletedCount = 0;
    for (String child : children) {
      File fileToDelete = new File(root, child);
      if (fileToDelete.lastModified()
          < PerformanceAnalyzerMetrics.getTimeInterval(referenceTime - purgeInterval)) {
        PerformanceAnalyzerMetrics.removeMetrics(fileToDelete);
        filesDeletedCount += 1;
      }
    }
    LOG.debug("'{}' Old writer files cleaned up.", filesDeletedCount);
  }
}
