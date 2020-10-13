package com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ESResources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.elasticsearch.cluster.service.MasterService;

import java.lang.reflect.Method;

public class MasterThrottlingMetricsCollector extends PerformanceAnalyzerMetricsCollector implements MetricsProcessor {

    public static final int SAMPLING_TIME_INTERVAL =
            MetricsConfiguration.CONFIG_MAP.get(MasterThrottlingMetricsCollector.class).samplingInterval;
    private static final Logger LOG = LogManager.getLogger(MasterThrottlingMetricsCollector.class);
    private static final int KEYS_PATH_LENGTH = 0;
    private static final String MASTER_THROTTLING_RETRY_LISTENER_PATH =
            "org.elasticsearch.action.support.master.MasterThrottlingRetryListener";
    private static final String THROTTLED_PENDING_TASK_COUNT_METHOD_NAME = "numberOfThrottledPendingTasks";
    private static final String RETRYING_TASK_COUNT_METHOD_NAME = "getRetryingTasksCount";
    private StringBuilder value;

    public MasterThrottlingMetricsCollector() {
        super(SAMPLING_TIME_INTERVAL, "MasterThrottlingMetricsCollector");
        value = new StringBuilder();
    }

    @Override
    void collectMetrics(long startTime) {
        try {
            if (ESResources.INSTANCE.getClusterService() == null
                    || ESResources.INSTANCE.getClusterService().getMasterService() == null) {
                return;
            }
            if(!isMasterThrottlingFeatureAvailable()) {
                LOG.error("master throttling is not available");
                return;
            }

            value.setLength(0);
            value.append(PerformanceAnalyzerMetrics.getJsonCurrentMilliSeconds())
                    .append(PerformanceAnalyzerMetrics.sMetricNewLineDelimitor);
            value.append(new MasterThrottlingMetrics(
                    getRetryingPendingTaskCount(),
                    getTotalMasterThrottledTaskCount()).serialize());

            saveMetricValues(value.toString(), startTime);

        } catch (Exception ex) {
            LOG.debug("Exception in Collecting Master Throttling Metrics: {} for startTime {}", () -> ex.toString(), () -> startTime);
        }
    }

    private boolean isMasterThrottlingFeatureAvailable() {
        Class throttlingRetryListener = null;
        Method getThrottledTasksCount = null;
        try {
            throttlingRetryListener = Class.forName(MASTER_THROTTLING_RETRY_LISTENER_PATH);
            getThrottledTasksCount = MasterService.class.getMethod(THROTTLED_PENDING_TASK_COUNT_METHOD_NAME);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            return false;
        }

        if(throttlingRetryListener == null || getThrottledTasksCount == null){
            return false;
        }
        return true;
    }

    private long getTotalMasterThrottledTaskCount() throws Exception {
        Method method = MasterService.class.getMethod(THROTTLED_PENDING_TASK_COUNT_METHOD_NAME);
        return (long) method.invoke(ESResources.INSTANCE.getClusterService().getMasterService());
    }

    private long getRetryingPendingTaskCount() throws Exception {
        Method method = Class.forName(MASTER_THROTTLING_RETRY_LISTENER_PATH).getMethod(RETRYING_TASK_COUNT_METHOD_NAME);
        return (long) method.invoke(null);
    }

    @Override
    public String getMetricsPath(long startTime, String... keysPath) {
        if (keysPath.length != KEYS_PATH_LENGTH) {
            throw new RuntimeException("keys length should be " + KEYS_PATH_LENGTH);
        }

        return PerformanceAnalyzerMetrics.generatePath(startTime, PerformanceAnalyzerMetrics.sMasterThrottledTasksPath);
    }

    public static class MasterThrottlingMetrics extends MetricStatus {
        private final long retryingTaskCount;
        private final long throttledPendingTasksCount;

        public MasterThrottlingMetrics(long retryingTaskCount, long throttledPendingTasksCount) {
            this.retryingTaskCount = retryingTaskCount;
            this.throttledPendingTasksCount = throttledPendingTasksCount;
        }

        @JsonProperty(AllMetrics.MasterThrottlingValue.Constants.RETRYING_TASK_COUNT)
        public long getRetryingTaskCount() {
            return retryingTaskCount;
        }

        @JsonProperty(AllMetrics.MasterThrottlingValue.Constants.THROTTLED_PENDING_TASK_COUNT)
        public long getThrottledPendingTasksCount() {
            return throttledPendingTasksCount;
        }
    }
}
