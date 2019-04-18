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


package com.amazon.opendistro.elasticsearch.performanceanalyzer.config;

import java.io.File;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ConfigStatus;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerPlugin;

public class PluginSettings {
    private static final Logger LOG = LogManager.getLogger(PluginSettings.class);

    private static PluginSettings instance;
    private static final String DEFAULT_CONFIG_FILE_PATH = "pa_config/performance-analyzer.properties";
    private static final String METRICS_LOCATION_KEY = "metrics-location";
    private static final String METRICS_LOCATION_DEFAULT = "/dev/shm/performanceanalyzer/";
    private static final String DELETION_INTERVAL_KEY = "metrics-deletion-interval";
    private static final int DELETION_INTERVAL_DEFAULT = 1;
    private static final int DELETION_INTERVAL_MIN = 1;
    private static final int DELETION_INTERVAL_MAX = 60;
    private static final String HTTPS_ENABLED = "https-enabled";

    private String metricsLocation;
    private int metricsDeletionInterval;
    private boolean httpsEnabled;
    private Properties settings;
    private final String configFilePath;

    static {
        PerformanceAnalyzerPlugin.invokePrivilegedAndLogError(() -> createInstance());
    }

    public String getMetricsLocation() {
        return metricsLocation;
    }

    public int getMetricsDeletionInterval() {
        return metricsDeletionInterval * 60 * 1000;
    }

    public String getSettingValue(String settingName) {
        return settings.getProperty(settingName);
    }

    public String getSettingValue(String settingName, String defaultValue) {
        return settings.getProperty(settingName, defaultValue);
    }

    private void loadHttpsEnabled() throws Exception {
        String httpsEnabledString = settings.getProperty(HTTPS_ENABLED, "False");
        if (httpsEnabledString == null) {
            httpsEnabled = false;
        }
        try {
            httpsEnabled = Boolean.parseBoolean(httpsEnabledString);
        } catch (Exception ex) {
            LOG.error("Unable to parse httpsEnabled property with value {}", httpsEnabledString);
            httpsEnabled = false;
        }
    }

    public boolean getHttpsEnabled() {
        return this.httpsEnabled;
    }

    private PluginSettings(String cfPath) {
        metricsLocation = METRICS_LOCATION_DEFAULT;
        metricsDeletionInterval = DELETION_INTERVAL_DEFAULT;
        if (cfPath == null || cfPath.isEmpty()) {
            this.configFilePath = DEFAULT_CONFIG_FILE_PATH;
        } else {
            this.configFilePath = cfPath;
        }
        try {
            settings = getSettingsFromFile(this.configFilePath);
            loadMetricsDeletionIntervalFromConfig();
            loadMetricsLocationFromConfig();
            loadHttpsEnabled();
        } catch (ConfigFileException e) {
            LOG.error("Loading config file {} failed with error: {}. Using default values.",
                      this.configFilePath, e.toString());
        } catch (ConfigFatalException e) {
            LOG.error("Having issue to load all config items. Disabling plugin.", e);
            ConfigStatus.INSTANCE.setConfigurationInvalid();
        } catch (Exception e) {
            LOG.error("Unexpected exception while initializing config. Disabling plugin.", e);
            ConfigStatus.INSTANCE.setConfigurationInvalid();
        }

        LOG.error("Config: metricsLocation: {}, metricsDeletionInterval: {}, httpsEnabled: {}",
                metricsLocation, metricsDeletionInterval, httpsEnabled);
    }

    public static PluginSettings instance() {
        return instance;
    }

    private static void createInstance() {
        String cfPath = System.getProperty("configFilePath");
        instance = new PluginSettings(cfPath);
    }

    private static Properties getSettingsFromFile(String filePath) throws ConfigFileException {
        try {
            return SettingsHelper.getSettings(filePath);
        } catch (Exception e) {
            throw new ConfigFileException(e);
        }
    }

    private void loadMetricsLocationFromConfig()
            throws ConfigFatalException {
        if (!settings.containsKey(METRICS_LOCATION_KEY)) {
            LOG.info("Cannot find metrics-location, using default value. {}", METRICS_LOCATION_DEFAULT);
        }

        metricsLocation = settings.getProperty(METRICS_LOCATION_KEY, METRICS_LOCATION_DEFAULT);
        validateOrCreateDir(metricsLocation);
    }

    private static void validateOrCreateDir(String path) throws ConfigFatalException {
        File dict = new File(path);

        boolean dictCreated = true;
        if (!dict.exists()) {
            dictCreated = dict.mkdir();
            LOG.info("Trying to create directory {}.", path);
        }

        boolean valid = dictCreated && dict.isDirectory() && dict.canWrite();
        if (!valid) {
            LOG.error("Invalid metrics location {}." +
                            " Created: {} (Expect True), Directory: {} (Expect True)," +
                            " CanWrite: {} (Expect True)",
                    path, dict.exists(), dict.isDirectory(), dict.canWrite());
            throw new ConfigFatalException("Having issue to use path: " + path);
        }
    }

    private void loadMetricsDeletionIntervalFromConfig() {
        if (!settings.containsKey(DELETION_INTERVAL_KEY)) {
            return;
        }

        try {
            int interval = Integer.parseInt(settings.getProperty(DELETION_INTERVAL_KEY));
            if (interval < DELETION_INTERVAL_MIN || interval > DELETION_INTERVAL_MAX) {
                LOG.error("metrics-deletion-interval out of range. Value should in ({}-{}). Using default value {}.",
                        DELETION_INTERVAL_MIN, DELETION_INTERVAL_MAX, metricsDeletionInterval);
                return;
            }
            metricsDeletionInterval = interval;
        } catch (NumberFormatException e) {
            LOG.error(
                    (Supplier<?>) () -> new ParameterizedMessage(
                            "Invalid metrics-deletion-interval. Using default value {}.",
                            metricsDeletionInterval),
                    e);
        }
    }
}
