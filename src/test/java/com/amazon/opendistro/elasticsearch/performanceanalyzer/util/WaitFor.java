package com.amazon.opendistro.elasticsearch.performanceanalyzer.util;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This class allows you to wait at most a specified duration until a condition evaluates to true
 */
public class WaitFor {
    /**
     * Waits at most the specified time for the given task to evaluate to true
     * @param task The task which we hope evaluates to true before the time limit
     * @param maxWait The max amount of time to wait for the task to evaluate for true
     * @param unit The time unit of the maxWait parameter
     * @throws Exception If the time limit expires before the task evaluates to true
     */
    public static void waitFor(Callable<Boolean> task, long maxWait, TimeUnit unit) throws Exception {
        long maxWaitMillis = TimeUnit.MILLISECONDS.convert(maxWait, unit);
        long pollTime = System.currentTimeMillis();
        long curTime;
        while (!task.call() && maxWaitMillis >= 0) {
            curTime = System.currentTimeMillis();
            maxWaitMillis -= (curTime - pollTime);
            pollTime = curTime;
        }
        if (maxWait < 0) {
            throw new TimeoutException("WaitFor timed out before task evaluated to true");
        }
    }
}

