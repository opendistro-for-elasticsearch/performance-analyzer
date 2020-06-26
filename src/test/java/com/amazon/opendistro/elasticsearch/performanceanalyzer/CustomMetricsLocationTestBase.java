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
