package com.amazon.opendistro.elasticsearch.performanceanalyzer.config;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ESResources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerPlugin;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.ScheduledMetricCollectorsExecutor;

public class PerformanceAnalyzerController {
    private static final String PERFORMANCE_ANALYZER_ENABLED_CONF = "performance_analyzer_enabled.conf";
    private static final String RCA_ENABLED_CONF = "rca_enabled.conf";
    private static final String LOGGING_ENABLED_CONF = "logging_enabled.conf";
    private static final String BATCH_METRICS_ENABLED_CONF = "batch_metrics_enabled.conf";
    private static final Logger LOG = LogManager.getLogger(PerformanceAnalyzerController.class);
    public static final int DEFAULT_NUM_OF_SHARDS_PER_COLLECTION = 0;

    private boolean paEnabled;
    private boolean rcaEnabled;
    private boolean loggingEnabled;
    private boolean batchMetricsEnabled;
    private volatile int shardsPerCollection;
    private boolean paEnabledDefaultValue = false;
    private boolean rcaEnabledDefaultValue = false;
    private boolean loggingEnabledDefaultValue = false;
    private boolean batchMetricsEnabledDefaultValue = false;
    private final ScheduledMetricCollectorsExecutor scheduledMetricCollectorsExecutor;

    public PerformanceAnalyzerController(final ScheduledMetricCollectorsExecutor scheduledMetricCollectorsExecutor) {
        this.scheduledMetricCollectorsExecutor = scheduledMetricCollectorsExecutor;
        initPerformanceAnalyzerStateFromConf();
        initRcaStateFromConf();
        initLoggingStateFromConf();
        initBatchMetricsStateFromConf();
        shardsPerCollection = DEFAULT_NUM_OF_SHARDS_PER_COLLECTION;
    }

    /**
     * Returns the current state of performance analyzer.
     * <p>
     * This setting is indicative of both the engine and the plugin state.
     * When enabled, both the writer and the engine are active. The writer captures metrics and stores it
     * while the engine processes the stored metrics.
     * When disabled, the writer stops capturing and no collector performs actual work of
     * collecting metrics even though the threads are still running. The reader will not do any processing
     * even though it is kept running.
     *
     * @return the state of performance analyzer.
     */
    public boolean isPerformanceAnalyzerEnabled() {
        return paEnabled;
    }

    /**
     * Returns the state of RCA framework.
     * When enabled, the RCA scheduler will run at a periodicity defined by the RCA scheduler.
     * When disabled, the RCA scheduler is stopped.
     *
     * @return the state of RCA framework.
     */
    public boolean isRcaEnabled() {
        return rcaEnabled;
    }

    public boolean isLoggingEnabled() {
        return loggingEnabled;
    }

    public boolean isBatchMetricsEnabled() { return batchMetricsEnabled; }

    /**
     * Reads the shardsPerCollection parameter in NodeStatsMetric
     *  @return the count of Shards per Collection
     */
    public int getNodeStatsShardsPerCollection() {
        return shardsPerCollection;
    }

    /**
     * Updates the shardsPerCollection parameter in NodeStatsMetric
     * @param value the desired integer value for Shards per Collection
     */
    public void updateNodeStatsShardsPerCollection(int value) {
        shardsPerCollection = value;
    }

    /**
     * Updates the state of performance analyzer(writer and engine).
     *
     * @param value The desired state of performance analyzer. False to disable, and true to enable.
     */
    public void updatePerformanceAnalyzerState(final boolean value) {
        this.paEnabled = value;
        if (scheduledMetricCollectorsExecutor != null) {
            scheduledMetricCollectorsExecutor.setEnabled(this.paEnabled);
        }
        saveStateToConf(this.paEnabled, PERFORMANCE_ANALYZER_ENABLED_CONF);
    }

    /**
     * Updates the state of RCA scheduler.
     * TODO: Migrate the updating of RCA toggling to the engine.
     *
     * @param shouldEnable The desired state of rca.
     *                     False to disable, true to enable if performance analyzer is also enabled.
     */
    public void updateRcaState(final boolean shouldEnable) {
        if (shouldEnable && !isPerformanceAnalyzerEnabled()) {
            return;
        }

        this.rcaEnabled = shouldEnable;
        saveStateToConf(this.rcaEnabled, RCA_ENABLED_CONF);
    }

    /**
     * Updates the state of performance analyzer logging.
     *
     * @param shouldEnable The desired state of performance analyzer logging. False to disable, and true to enable.
     */
    public void updateLoggingState(final boolean shouldEnable) {
        if (shouldEnable && !isPerformanceAnalyzerEnabled()) {
            return;
        }
        this.loggingEnabled = shouldEnable;
        if (scheduledMetricCollectorsExecutor != null) {
            PerformanceAnalyzerMetrics.setIsMetricsLogEnabled(this.loggingEnabled);
        }
        saveStateToConf(this.loggingEnabled, LOGGING_ENABLED_CONF);
    }

    /**
     * Updates the state of the batch metrics api.
     *
     * @param shouldEnable The desired state of the batch metrics api. False to disable, and true to enable.
     */
    public void updateBatchMetricsState(final boolean shouldEnable) {
        if (shouldEnable && !isPerformanceAnalyzerEnabled()) {
            return;
        }

        this.batchMetricsEnabled = shouldEnable;
        saveStateToConf(this.batchMetricsEnabled, BATCH_METRICS_ENABLED_CONF);
    }

    private void initPerformanceAnalyzerStateFromConf() {
        Path filePath = Paths.get(getDataDirectory(), PERFORMANCE_ANALYZER_ENABLED_CONF);
        PerformanceAnalyzerPlugin.invokePrivileged(() -> {
            boolean paEnabledFromConf;
            try {
                paEnabledFromConf = readBooleanFromFile(filePath);
            } catch (Exception e) {
                LOG.debug("Error reading Performance Analyzer state from Conf file", e);
                if (e instanceof NoSuchFileException) {
                    saveStateToConf(paEnabledDefaultValue, PERFORMANCE_ANALYZER_ENABLED_CONF);
                }
                paEnabledFromConf = paEnabledDefaultValue;
            }

            updatePerformanceAnalyzerState(paEnabledFromConf);
        });
    }

    private void initRcaStateFromConf() {
        Path filePath = Paths.get(getDataDirectory(), RCA_ENABLED_CONF);
        PerformanceAnalyzerPlugin.invokePrivileged(() -> {
            boolean rcaEnabledFromConf;
            try {
                rcaEnabledFromConf = readBooleanFromFile(filePath);
            } catch (Exception e) {
                LOG.debug("Error reading Performance Analyzer state from Conf file", e);
                if (e instanceof NoSuchFileException) {
                    saveStateToConf(rcaEnabledDefaultValue, RCA_ENABLED_CONF);
                }
                rcaEnabledFromConf = rcaEnabledDefaultValue;
            }

            // For RCA framework to be enabled, it needs both PA and RCA to be enabled.
            updateRcaState(paEnabled && rcaEnabledFromConf);
        });
    }

    private void initLoggingStateFromConf() {
        Path filePath = Paths.get(getDataDirectory(), LOGGING_ENABLED_CONF);
        PerformanceAnalyzerPlugin.invokePrivileged(() -> {
            boolean loggingEnabledFromConf;
            try {
                loggingEnabledFromConf = readBooleanFromFile(filePath);
            } catch (Exception e) {
                LOG.debug("Error reading logging state from Conf file", e);
                if (e instanceof NoSuchFileException) {
                    saveStateToConf(loggingEnabledDefaultValue, LOGGING_ENABLED_CONF);
                }
                loggingEnabledFromConf = loggingEnabledDefaultValue;
            }

            // For logging to be enabled, it needs both PA and Logging to be enabled.
            updateLoggingState(paEnabled && loggingEnabledFromConf);
        });
    }

    private void initBatchMetricsStateFromConf() {
        Path filePath = Paths.get(getDataDirectory(), BATCH_METRICS_ENABLED_CONF);
        PerformanceAnalyzerPlugin.invokePrivileged(() -> {
            boolean batchMetricsEnabledFromConf;
            try {
                batchMetricsEnabledFromConf = readBooleanFromFile(filePath);
            } catch (Exception e) {
                LOG.debug("Error reading Performance Analyzer state from Conf file", e);
                if (e instanceof NoSuchFileException) {
                    saveStateToConf(batchMetricsEnabledDefaultValue, BATCH_METRICS_ENABLED_CONF);
                }
                batchMetricsEnabledFromConf = batchMetricsEnabledDefaultValue;
            }

            // For batch metrics to be enabled, it needs both PA and Batch Metrics to be enabled.
            updateBatchMetricsState(paEnabled && batchMetricsEnabledFromConf);
        });
    }

    private boolean readBooleanFromFile(final Path filePath) throws Exception {
        try (Scanner sc = new Scanner(filePath)) {
            String nextLine = sc.nextLine();
            return Boolean.parseBoolean(nextLine);
        }
    }

    private String getDataDirectory() {
        return new org.elasticsearch.env.Environment(
            ESResources.INSTANCE.getSettings(), ESResources.INSTANCE.getConfigPath())
            .dataFiles()[0] // $ES_HOME/var/es/data
            .toFile()
            .getPath();
    }

    private void saveStateToConf(boolean featureEnabled, String fileName) {
        PerformanceAnalyzerPlugin.invokePrivileged(() -> {
            try {
                Files.write(
                        Paths.get(getDataDirectory() + File.separator + fileName),
                        String.valueOf(featureEnabled)
                              .getBytes());
            } catch (Exception ex) {
                LOG.error(ex.toString(), ex);
            }
        });
    }
}
