package com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.google.gson.Gson;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.tools.StringUtils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics.addMetricEntry;

public class FaultDetectionMetricsCollector extends PerformanceAnalyzerMetricsCollector implements MetricsProcessor {
    public static final int SAMPLING_TIME_INTERVAL = MetricsConfiguration.CONFIG_MAP.
            get(FaultDetectionMetricsCollector.class).samplingInterval;
    private static final int KEYS_PATH_LENGTH = 3;
    private static final Logger LOG = LogManager.getLogger(FaultDetectionMetricsCollector.class);
    private static final String FAULT_DETECTION_HANDLER_NAME =
            "com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.ClusterFaultDetectionStatsHandler";
    private static final String FAULT_DETECTION_HANDLER_METRIC_QUEUE = "metricQueue";
    private StringBuilder value;

    public FaultDetectionMetricsCollector() {
        super(SAMPLING_TIME_INTERVAL, "FaultDetectionMetrics");
        value = new StringBuilder();
    }

    @Override
    @SuppressWarnings("unchecked")
    void collectMetrics(long startTime) {
        Class<?> faultDetectionHandler = null;
        try {
            faultDetectionHandler = Class.forName(FAULT_DETECTION_HANDLER_NAME);
        } catch (ClassNotFoundException e) {
            LOG.debug("No Handler Detected for Fault Detection. Skipping FaultDetectionMetricsCollector");
        }

        if(faultDetectionHandler == null) {
            return;
        }

        try {
            BlockingQueue<String> metricQueue = (BlockingQueue<String>)
                    getFaultDetectionHandlerMetricsQueue(faultDetectionHandler).get(null);
            List<String> metrics = new ArrayList<>();
            metricQueue.drainTo(metrics);
            Gson gson = new Gson();
            List<ClusterFaultDetectionContext> faultDetectionContextsList = metrics.stream()
                    .map(string -> gson.fromJson(string, ClusterFaultDetectionContext.class))
                    .collect(Collectors.toList());

            for(ClusterFaultDetectionContext clusterFaultDetectionContext : faultDetectionContextsList) {
                value.setLength(0);
                value.append(PerformanceAnalyzerMetrics.getCurrentTimeMetric());
                addMetricEntry(value, AllMetrics.FaultDetectionDimension.SOURCE_NODE_ID
                        .toString(), clusterFaultDetectionContext.getSourceNodeId());
                addMetricEntry(value, AllMetrics.FaultDetectionDimension.TARGET_NODE_ID
                        .toString(), clusterFaultDetectionContext.getTargetNodeId());

                if(StringUtils.isEmpty(clusterFaultDetectionContext.getStartTime())) {
                    addMetricEntry(value, AllMetrics.CommonMetric.FINISH_TIME.toString(),
                            clusterFaultDetectionContext.getFinishTime());
                    addMetricEntry(value, PerformanceAnalyzerMetrics.ERROR,
                            clusterFaultDetectionContext.getError());
                    saveMetricValues(value.toString(), startTime, clusterFaultDetectionContext.getType(),
                            clusterFaultDetectionContext.getThreadId(), PerformanceAnalyzerMetrics.FINISH_FILE_NAME);
                } else {
                    addMetricEntry(value, AllMetrics.CommonMetric.START_TIME.toString(),
                            clusterFaultDetectionContext.getStartTime());
                    saveMetricValues(value.toString(), startTime, clusterFaultDetectionContext.getType(),
                            clusterFaultDetectionContext.getThreadId(), PerformanceAnalyzerMetrics.START_FILE_NAME);
                }
            }
        } catch (Exception ex) {
            LOG.debug("Exception in Collecting FaultDetection Metrics: {} for startTime {}",
                    () -> ex.toString(), () -> startTime);
        }
    }

    Field getFaultDetectionHandlerMetricsQueue(Class<?> faultDetectionHandler) throws Exception {
        Field metricsQueue = faultDetectionHandler.getDeclaredField(FAULT_DETECTION_HANDLER_METRIC_QUEUE);
        metricsQueue.setAccessible(true);
        return metricsQueue;
    }

    /** Sample Event
     * ^fault_detection/follower_check/7627/finish
     * current_time":1601486201861
     * SourceNodeID:g52i9a93a762cd59dda8d3379b09a752a
     * TargetNodeID:b2a5a93a762cd59dda8d3379b09a752a
     * FinishTime:1566413987986
     * Error:0$
     *
     * @param startTime time at which collector is called
     * @param keysPath
     * @return metric path
     */
    @Override
    public String getMetricsPath(long startTime, String... keysPath) {
        if (keysPath.length != KEYS_PATH_LENGTH) {
            throw new RuntimeException("keys length should be " + KEYS_PATH_LENGTH);
        }

        return PerformanceAnalyzerMetrics.generatePath(startTime, PerformanceAnalyzerMetrics.sFaultDetection,
                keysPath[0], keysPath[1], keysPath[2]);
    }

    public static class ClusterFaultDetectionContext {
        String type;
        String sourceNodeId;
        String targetNodeId;
        String threadId;
        String error;
        String startTime;
        String finishTime;

        public String getType() {
            return this.type;
        }

        public String getSourceNodeId() {
            return this.sourceNodeId;
        }

        public String getTargetNodeId() {
            return this.targetNodeId;
        }

        public String getError() {
            return this.error;
        }

        public String getStartTime() {
            return this.startTime;
        }

        public String getFinishTime() {
            return this.finishTime;
        }

        public String getThreadId() {
            return this.threadId;
        }

    }
}
