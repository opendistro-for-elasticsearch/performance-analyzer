package com.amazon.opendistro.elasticsearch.performanceanalyzer.tracker;

/**
 *  Class to track previous latency and failure metrics to calculate point in time metrics. These values are updated
 *  everytime collector calls respective stats API.
 */
public class MetricsTracker {
    private long prevTimeTakenInMillis;
    private long prevFailedCount;
    private long prevTotalCount;

    public MetricsTracker(long timeInMillis, long failedCount, long totalCount) {
        this.prevTimeTakenInMillis = timeInMillis;
        this.prevFailedCount = failedCount;
        this.prevTotalCount = totalCount;
    }

    public MetricsTracker() {

    }

    public Long getPrevTimeTakenInMillis() {
        return prevTimeTakenInMillis;
    }

    public Long getPrevFailedCount() {
        return prevFailedCount;
    }

    public Long getPrevTotalCount() {
        return prevTotalCount;
    }
}
