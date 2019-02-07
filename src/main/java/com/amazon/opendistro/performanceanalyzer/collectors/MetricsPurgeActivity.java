/*
 * Copyright <2019> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.performanceanalyzer.collectors;

import java.io.File;

import com.amazon.opendistro.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;

public class MetricsPurgeActivity extends PerformanceAnalyzerMetricsCollector {
    public MetricsPurgeActivity() {
        super(MetricsConfiguration.CONFIG_MAP.get(MetricsPurgeActivity.class).samplingInterval,
            "MetricsPurgeActivity");
    }

    private static int purgeInterval = MetricsConfiguration.CONFIG_MAP.get(MetricsPurgeActivity.class).deletionInterval;

    @Override
    public void collectMetrics(long startTime) {
        File root = new File(PluginSettings.instance().getMetricsLocation());

        String[] children = root.list();
        if (children == null) {
            return;
        }
        for (int i = 0; i < children.length; i++) {
            if (Long.parseLong(children[i]) < PerformanceAnalyzerMetrics.getTimeInterval(startTime - purgeInterval)) {
                PerformanceAnalyzerMetrics.removeMetrics(new File(root, children[i]));
            }
        }
    }
}
