package com.amazon.opendistro.elasticsearch.performanceanalyzer.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ESResources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerPlugin;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.ScheduledMetricCollectorsExecutor;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.PerformanceAnalyzerConfigSettings.LOGGING_ENABLED_CONF;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.PerformanceAnalyzerConfigSettings.MUTED_RCAS_CONFIG;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.PerformanceAnalyzerConfigSettings.PERFORMANCE_ANALYZER_ENABLED_CONF;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.PerformanceAnalyzerConfigSettings.RCA_CONF_FILENAME;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.PerformanceAnalyzerConfigSettings.RCA_IDLE_MASTER_CONF_FILENAME;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.PerformanceAnalyzerConfigSettings.RCA_MASTER_CONF_FILENAME;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.PerformanceAnalyzerConfigSettings.RCA_ENABLED_CONF;

public class PerformanceAnalyzerController {
    private static final Logger LOG = LogManager.getLogger(PerformanceAnalyzerController.class);
    public static final int DEFAULT_NUM_OF_SHARDS_PER_COLLECTION = 0;

    private boolean paEnabled;
    private boolean rcaEnabled;
    private boolean loggingEnabled;
    private volatile int shardsPerCollection;
    private volatile String mutedRcas;
    private boolean paEnabledDefaultValue = false;
    private boolean rcaEnabledDefaultValue = false;
    private boolean loggingEnabledDefaultValue = false;
    private String mutedRcasDefaultValue = "";
    private final ScheduledMetricCollectorsExecutor scheduledMetricCollectorsExecutor;

    public PerformanceAnalyzerController(final ScheduledMetricCollectorsExecutor scheduledMetricCollectorsExecutor) {
        this.scheduledMetricCollectorsExecutor = scheduledMetricCollectorsExecutor;
        initPerformanceAnalyzerStateFromConf();
        initRcaStateFromConf();
        initLoggingStateFromConf();
        shardsPerCollection = DEFAULT_NUM_OF_SHARDS_PER_COLLECTION;
        initMutedRcasStateFromConf();
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
     * Reads the mutedRcas parameter
     * mutedRcas represents the list of RCAs currently muted for the cluster
     *
     * @return String representing muted RCAs
     */
    public String getMutedRcas() {
        return mutedRcas;
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
     * Updates the list of muted RCAs
     *
     * @param mutedRcas The desired RCAs to be muted.
     */
    public void updateMutedRcasState(final String mutedRcas) {
        if (mutedRcas != null && !isPerformanceAnalyzerEnabled() && !isRcaEnabled()) {
            return;
        }
        this.mutedRcas = mutedRcas;

        // Save the config to all the rca conf files
        saveMutedRcasToConf(this.mutedRcas, RCA_CONF_FILENAME);
        saveMutedRcasToConf(this.mutedRcas, RCA_MASTER_CONF_FILENAME);
        saveMutedRcasToConf(this.mutedRcas, RCA_IDLE_MASTER_CONF_FILENAME);
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
                LOG.debug("Error reading RCA state from Conf file", e);
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

    /**
     * Initializes the Muted RCA state from Muted RCA Conf file
     */
    private void initMutedRcasStateFromConf() {
        // `MUTED_RCAS_CONFIG` can be read from either of 3 rca config files
        Path filePath = Paths.get(getPAConfigDirectory(), RCA_CONF_FILENAME);
        String mutedRcasFromConf;
        try{
            // Read json object as string
            Scanner scanner = new Scanner(new FileInputStream(filePath.toString()), StandardCharsets.UTF_8.name());
            String jsonText = scanner.useDelimiter("\\A").next();

            ObjectMapper mapper = new ObjectMapper().enable(JsonParser.Feature.ALLOW_COMMENTS);
            mutedRcasFromConf = mapper.readTree(jsonText).
                    get(MUTED_RCAS_CONFIG).asText(mutedRcasDefaultValue);
            updateMutedRcasState(mutedRcasFromConf);
        } catch (Exception e) {
            LOG.debug("Error reading Muted RCA state from Conf file", e);
            if (e instanceof NullPointerException) {
                LOG.info("'{}' not present in config file, setting the default value: {}",
                        MUTED_RCAS_CONFIG, mutedRcasDefaultValue);
                saveMutedRcasToConf(mutedRcasDefaultValue, RCA_CONF_FILENAME);
                saveMutedRcasToConf(mutedRcasDefaultValue, RCA_MASTER_CONF_FILENAME);
                saveMutedRcasToConf(mutedRcasDefaultValue, RCA_IDLE_MASTER_CONF_FILENAME);
            }
        }
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

    private String getPAConfigDirectory() {
        return Paths.get(System.getProperty("es.path.home"),
                "opendistro_performance_analyzer", "pa_config").toString();
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

    private void saveMutedRcasToConf(String mutedRcas, String fileName) {
        try {
            // create the config json Object from rca config file
            String rcaConfPath = Paths.get(getPAConfigDirectory(), fileName).toString();
            Scanner scanner = new Scanner(new FileInputStream(rcaConfPath), StandardCharsets.UTF_8.name());
            String jsonText = scanner.useDelimiter("\\A").next();
            ObjectMapper mapper = new ObjectMapper();
            mapper.enable(JsonParser.Feature.ALLOW_COMMENTS);
            mapper.enable(SerializationFeature.INDENT_OUTPUT);
            JsonNode configObject = mapper.readTree(jsonText);

            // update the `MUTED_RCAS_CONFIG` value in config Object
            ((ObjectNode) configObject).put(MUTED_RCAS_CONFIG, mutedRcas);
            mapper.writeValue(new FileOutputStream(rcaConfPath), configObject);
        } catch (Exception ex) {
            LOG.error(ex.toString(), ex);
        }
    }
}
