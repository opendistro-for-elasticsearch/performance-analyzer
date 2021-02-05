/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Before;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;

public class CustomMetricsLocationTestBase {

    private static final Path METRICS_LOCATION = Paths.get("build/tmp/junit_metrics");

    @Before
    public void setUp() throws Exception {
        if (!Files.exists(METRICS_LOCATION)) {
            Files.createDirectories(METRICS_LOCATION.getParent());
            Files.createDirectory(METRICS_LOCATION);
        }

        PluginSettings.instance().setMetricsLocation(METRICS_LOCATION + File.separator);
    }
}
