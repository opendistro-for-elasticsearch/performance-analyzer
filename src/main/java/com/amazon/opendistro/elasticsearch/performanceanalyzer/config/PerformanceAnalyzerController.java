package com.amazon.opendistro.elasticsearch.performanceanalyzer.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
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

public class PerformanceAnalyzerController {
    private static final String PERFORMANCE_ANALYZER_ENABLED_CONF = "performance_analyzer_enabled.conf";
    private static final String RCA_ENABLED_CONF = "rca_enabled.conf";
    private static final String LOGGING_ENABLED_CONF = "logging_enabled.conf";
    private static final Logger LOG = LogManager.getLogger(PerformanceAnalyzerController.class);
    public static final int DEFAULT_NUM_OF_SHARDS_PER_COLLECTION = 0;

    public static final String MUTED_RCAS_CONFIG = "muted-rcas";
    public static final String DEFAULT_MUTED_RCAS = "";
    // RCA specific configurations are located in below 3 files, depending on the node type
    public static final String RCA_CONF_FILENAME = "rca.conf";
    public static final String RCA_MASTER_CONF_FILENAME = "rca_master.conf";
    public static final String RCA_IDLE_MASTER_CONF_FILENAME = "rca_idle_master.conf";

    private boolean paEnabled;
    private boolean rcaEnabled;
    private boolean loggingEnabled;
    private volatile int shardsPerCollection;
    private volatile String mutedRcas;
    private boolean paEnabledDefaultValue = false;
    private boolean rcaEnabledDefaultValue = false;
    private boolean loggingEnabledDefaultValue = false;
    private final ScheduledMetricCollectorsExecutor scheduledMetricCollectorsExecutor;

    public PerformanceAnalyzerController(final ScheduledMetricCollectorsExecutor scheduledMetricCollectorsExecutor) {
        this.scheduledMetricCollectorsExecutor = scheduledMetricCollectorsExecutor;
        initPerformanceAnalyzerStateFromConf();
        initRcaStateFromConf();
        initLoggingStateFromConf();
        shardsPerCollection = DEFAULT_NUM_OF_SHARDS_PER_COLLECTION;
        mutedRcas = DEFAULT_MUTED_RCAS;
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
     * Reads the mutedRcas parameter
     * mutedRcas represents the list of RCAs currently muted for the cluster
     *
     * @return String representing muted RCAs
     */
    public String getMutedRcas() {
        return mutedRcas;
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

    private Path getPAConfigDirectory() {
        return Paths.get(System.getProperty("es.path.home"),
                "opendistro_performance_analyzer", "pa_config");
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

    private synchronized void saveMutedRcasToConf(String mutedRcas, String fileName) {
        try {
            // create the config json Object from rca config file
            Path rcaConfPath = Paths.get(getPAConfigDirectory().toString(), fileName);
            Scanner scanner = new Scanner(new FileInputStream(rcaConfPath.toString()), StandardCharsets.UTF_8.name());
            String jsonText = scanner.useDelimiter("\\A").next();
            ObjectMapper mapper = new ObjectMapper();
            mapper.enable(JsonParser.Feature.ALLOW_COMMENTS);
            mapper.enable(SerializationFeature.INDENT_OUTPUT);
            JsonNode configObject = mapper.readTree(jsonText);

            // To ensure consistency on read for rca.conf and back-up purpose, we will maintain 2 files,
            // the (n) 'rca.conf' and the (n-1) 'rca_<timestamp_n1>.conf'. For updating the latest config, we will :
            //
            // 1. Back-up the current (n) 'rca.conf' as (n-1) 'rca_<timestamp_n1>.conf'
            // 2. Update the config object, write to a temp file and atomically move the temp file to rca.conf
            // 3. Delete the older (n-2) 'rca_<timestamp_n2>.conf' file to ensure we strictly maintain 2 files.
            String seperator = "_";

            String currTimeStamp = new SimpleDateFormat("yyyyMMddHHmm").format(new Date());
            Path backupRcaConfPath = Paths.get(getPAConfigDirectory().toString(), fileName + seperator + currTimeStamp);
            Files.copy(rcaConfPath, backupRcaConfPath);
            LOG.debug("Created the back-up Conf file: {} for : {}", backupRcaConfPath.toString(), rcaConfPath.toString());

            ((ObjectNode) configObject).put(MUTED_RCAS_CONFIG, mutedRcas);
            Path tmp = Files.createTempFile(getPAConfigDirectory(), null, null);
            mapper.writeValue(new FileOutputStream(tmp.toString()), configObject);
            Files.move(tmp, rcaConfPath, StandardCopyOption.ATOMIC_MOVE);
            LOG.debug("Updated the Conf File: {} for Muted RCAs: {}", rcaConfPath.toString(), mutedRcas);

            // Get all files which begin with filename_, for example : rca.conf_202004291057 for filename 'rca.conf'
            File rootFolder = new File(getPAConfigDirectory().toString());
            String[] targetFiles = rootFolder.list((path, name) -> name.startsWith(fileName + seperator));
            if(targetFiles != null && targetFiles.length > 1) {
                // Since the filename contain timestamp, in the sorted array, the last file is the (n-1) file.
                // Delete all file with exception of this (n-1) file
                Arrays.sort(targetFiles);
                for (int index = 0; index < (targetFiles.length - 1); index++) {
                    String fileToDelete = targetFiles[index];
                    Files.delete(Paths.get(getPAConfigDirectory().toString(), fileToDelete));
                    LOG.debug("Deleted the older back-up Conf Files: {}", fileToDelete);
                }
            }
        } catch (Exception ex) {
            LOG.error(ex.toString(), ex);
        }
    }
}
