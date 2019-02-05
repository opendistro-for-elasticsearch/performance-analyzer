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

package com.amazon.opendistro.performanceanalyzer.reader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.performanceanalyzer.metrics.AllMetrics.MetricName;
import com.amazon.opendistro.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.performanceanalyzer.metricsdb.MetricsDB;
import com.google.common.annotations.VisibleForTesting;

public class ReaderMetricsProcessor implements Runnable {
    private static final Logger LOG = LogManager.getLogger(ReaderMetricsProcessor.class);

    private static final String DB_URL = "jdbc:sqlite:";
    private final Connection conn;
    private final DSLContext create;

    //This semaphore is used to control access to metricsDBMap from threads outside of
    //ReaderMetricsProcessor.
    private NavigableMap<Long, MetricsDB> metricsDBMap;
    private NavigableMap<Long, OSMetricsSnapshot> osMetricsMap;
    private NavigableMap<Long, ShardRequestMetricsSnapshot> shardRqMetricsMap;
    private NavigableMap<Long, HttpRequestMetricsSnapshot> httpRqMetricsMap;
    private Map<AllMetrics.MetricName, NavigableMap<Long, MemoryDBSnapshot>> nodeMetricsMap;
    private static final int MAX_DATABASES = 4;
    private static final int OS_SNAPSHOTS = 4;
    private static final int RQ_SNAPSHOTS = 4;
    private static final int HTTP_RQ_SNAPSHOTS = 4;
    private final MetricsParser metricsParser;
    private final String rootLocation;

    public static ReaderMetricsProcessor current = null;

    public ReaderMetricsProcessor(String rootLocation) throws Exception {
        conn = DriverManager.getConnection(DB_URL);
        create = DSL.using(conn, SQLDialect.SQLITE);
        metricsDBMap = new ConcurrentSkipListMap<>();
        osMetricsMap = new TreeMap<>();
        shardRqMetricsMap = new TreeMap<>();
        httpRqMetricsMap = new TreeMap<>();
        metricsParser = new MetricsParser();
        this.rootLocation = rootLocation;

        AllMetrics.MetricName[] names = AllMetrics.MetricName.values();
        nodeMetricsMap = new HashMap<>(names.length);
        for (int i=0; i<names.length; i++) {
            nodeMetricsMap.put(names[i], new TreeMap<>());
        }
    }

    @Override
    public void run() {
        long startTime = System.currentTimeMillis();
        try {
            Statement stmt = conn.createStatement();
            try {
                stmt.executeUpdate("PRAGMA journal_mode = OFF");
                stmt.executeUpdate("PRAGMA soft_heap_limit = 10000000");
            } catch (Exception e) {
                LOG.error("Unable to run PRAGMA");
            } finally {
                stmt.close();
            }

            while (true) {
                //Create snapshots.
                Statement vacuumStmt = conn.createStatement();
                try {
                vacuumStmt.executeUpdate("VACUUM");
                } catch (Exception e) {
                    LOG.error("Unable to run Vacuum.");
                } finally {
                    vacuumStmt.close();
                }
                conn.setAutoCommit(false);
                startTime = System.currentTimeMillis();
                //- Always read one sampling interval behind from current timestamp,
                // otherwise reader may be trying to read bucket, which writer might not have written yet
                ClusterLevelMetricsReader.collectNodeMetrics(startTime - MetricsConfiguration.SAMPLING_INTERVAL);
                processMetrics(rootLocation, startTime);
                trimOldSnapshots();
                conn.commit();
                conn.setAutoCommit(true);
                long duration = System.currentTimeMillis() - startTime;
                LOG.info("Total time taken: {}", duration);
                if (duration < MetricsConfiguration.SAMPLING_INTERVAL) {
                    Thread.sleep(MetricsConfiguration.SAMPLING_INTERVAL - duration);
                }
            }
        } catch (Throwable e) {
            LOG.error(
                    (Supplier<?>) () -> new ParameterizedMessage(
                            "READER PROCESSOR ERROR. NEEDS DEBUGGING {}.",
                            e.toString()),
                    e);

            try {
                long duration = System.currentTimeMillis() - startTime;
                if (duration < MetricsConfiguration.SAMPLING_INTERVAL) {
                    Thread.sleep(MetricsConfiguration.SAMPLING_INTERVAL - duration);
                }
            } catch (Exception ex) {
                LOG.debug("Exception in sleep: {}", () -> ex);
                //- nothing to do
            }
            throw new RuntimeException("READER ERROR");
        } finally {
            try {
                conn.close();
            } catch (Exception e) {
                LOG.error("Unable to close database connection.");
            }
            LOG.error("Connection to the database was closed.");
        }
    }

    public void trimOldSnapshots() throws Exception {
        trimMap(osMetricsMap, OS_SNAPSHOTS);
        trimMap(shardRqMetricsMap, RQ_SNAPSHOTS);
        trimMap(httpRqMetricsMap, HTTP_RQ_SNAPSHOTS);
        trimMap(metricsDBMap, MAX_DATABASES);
        for (NavigableMap<Long, MemoryDBSnapshot> snap : nodeMetricsMap
                .values()) {
            // do the same thing as OS_SNAPSHOTS.  Eventually MemoryDBSnapshot
            // will replace OSMetricsSnapshot as we want to our code to be
            // stable.
            trimMap(snap, OS_SNAPSHOTS);
        }
    }

    /**
     * Deletes the lowest entries in the map till the size of the map is equal to maxSize.
     */
    private void trimMap(NavigableMap<Long, ?> map, int maxSize) throws Exception {
        //Remove the oldest entries from the map
        while (map.size() > maxSize) {
            Map.Entry<Long, ?> lowestEntry = map.firstEntry();
            if (lowestEntry != null) {
                Removable value = (Removable) lowestEntry.getValue();
                value.remove();
                map.remove(lowestEntry.getKey());
            }
        }
    }

    /**
     * Parse per thread OS metrics.
     * OS metrics are generated per thread and written to files in
     * /dev/shm/performanceanalyzer/{rotation_window}/threads/{tid}/os_metrics.
     * This function parses the files written since the last successful run and populates an inmemory
     * sqlite table with the results. A few metrics available are - cpu, rss, minor pagefaults etc.
     *
     * @param rootLocation where to find metric files
     * @param currTimestamp when reader starts processing metric files
     * @throws Exception thrown if the metric file could not be parsed correctly.
     */
    public void parseOSMetrics(String rootLocation, long currTimestamp) throws Exception {
        long mCurrT = System.currentTimeMillis();
        LOG.info("Creating OsSnap {}", currTimestamp);
        OSMetricsSnapshot osSnap = new OSMetricsSnapshot(this.conn, "os_", currTimestamp);
        //parse metrics for last window.
        Map.Entry<Long, OSMetricsSnapshot> lastOSMetricsEntry = osMetricsMap.lastEntry();
        long lastOSMetricsSnapshotTime = 0;
        if (lastOSMetricsEntry == null) {
            lastOSMetricsSnapshotTime = 0L;
        } else {
            lastOSMetricsSnapshotTime = lastOSMetricsEntry.getKey();
        }

        if (metricsParser.parseOSMetrics(rootLocation, currTimestamp, osSnap, lastOSMetricsSnapshotTime)) {
            LOG.info("Adding new OS snapshot- currTimestamp {}, actualTime {}", currTimestamp, osSnap.getLastUpdatedTime());
            osMetricsMap.put(osSnap.getLastUpdatedTime(), osSnap);
        } else {
            LOG.info("Did not add values into osSnap. Clearing it {}", currTimestamp);
            osSnap.remove();
        }

        long mFinalT = System.currentTimeMillis();
        LOG.info("Total time taken for parsing OS Metrics: {}", mFinalT - mCurrT);
    }

    /**
     * Parse per node metric. Node level metrics are generated per node This
     * function parses metrics files written since the last successful run and
     * populates an in-memory sqlite table with the results. A few metrics
     * available are - Starting memory limit for overall parent-level breaker,
     *  maximum limit defined for the survivor memory pool (-1 means there is
     *   no maxium limit) etc.
     *
     * @param  currTimestamp when reader starts processing metric files
     * @throws Exception thrown if we have issues parsing metrics
     */
    public void parseNodeMetrics(long currTimestamp)
            throws Exception {
        long mCurrT = System.currentTimeMillis();
        LOG.info("Creating MemoryDBSnapshot {}", currTimestamp);
        for (Map.Entry<AllMetrics.MetricName, NavigableMap<Long, MemoryDBSnapshot>> entry :
            nodeMetricsMap.entrySet()) {
            AllMetrics.MetricName name = entry.getKey();
            NavigableMap<Long, MemoryDBSnapshot> currMap = entry.getValue();

            MemoryDBSnapshot currSnap = new MemoryDBSnapshot(this.conn, name,
                    currTimestamp);

            // parse metrics for last window.
            Map.Entry<Long, MemoryDBSnapshot> lastNodeMetricsEntry = currMap
                    .lastEntry();
            long lastNodeMetricsSnapshotTime = 0;
            if (lastNodeMetricsEntry == null) {
                lastNodeMetricsSnapshotTime = 0L;
            } else {
                lastNodeMetricsSnapshotTime = lastNodeMetricsEntry.getKey();
            }

            MetricProperties currParser = MetricPropertiesConfig.getInstance()
                    .getProperty(name);

            if (currParser.dispatch(
                    currSnap, currTimestamp, lastNodeMetricsSnapshotTime)) {
                LOG.info(
                        "Adding new {} snapshot- currTimestamp {}, actualTime {}",
                        name, currTimestamp, currSnap.getLastUpdatedTime());
                currMap.put(currSnap.getLastUpdatedTime(), currSnap);
            } else {
                LOG.info("Did not add values into {} snapshot. Clearing it {}",
                        name, currTimestamp);
                currSnap.remove();
            }

            long mFinalT = System.currentTimeMillis();
            LOG.info("Total time taken for parsing {} Metrics: {}", name,
                    mFinalT - mCurrT);
        }

    }

    /**
     * Parse all events generated by shard requests.
     * This function processes shard events such as shardBulk and shardSearch. Every operation
     * emits a start event and an end event. The events can be found in
     * /dev/shm/performanceanalyzer/{rotation_window}/threads/{tid}/{operation}/{rid}/. The start event data is written to
     * a file called start and the end event data is in a file called end.
     *
     * @param rootLocation where to find metric files
     * @param currWindowStartTime the start time of current sampling period.
     *   The bound of the period where that value is measured is
     *  MetricsConfiguration.SAMPLING_INTERVAL.
     * @param currWindowEndTime the end time of current sampling period.
     *   The bound of the period where that value is measured is
     *  MetricsConfiguration.SAMPLING_INTERVAL.
     *
     * @throws Exception thrown if we have issues parsing metrics
     */
    public void parseRequestMetrics(String rootLocation, long currWindowStartTime,
            long currWindowEndTime) throws Exception {
        long mCurrT = System.currentTimeMillis();
        if (shardRqMetricsMap.get(currWindowStartTime) == null) {
            ShardRequestMetricsSnapshot rqSnap = new ShardRequestMetricsSnapshot(this.conn, currWindowStartTime);
            Map.Entry<Long, ShardRequestMetricsSnapshot> entry = shardRqMetricsMap.lastEntry();
            if (entry != null) {
                rqSnap.rolloverInflightRequests(entry.getValue());
            }
            metricsParser.parseRequestMetrics(rootLocation, currWindowStartTime,
                    currWindowEndTime, rqSnap);

            LOG.debug(() -> rqSnap.fetchAll());
            shardRqMetricsMap.put(currWindowStartTime, rqSnap);
            LOG.info("Adding new RQ snapshot- currWindowStartTime {}", currWindowStartTime);
        }

        long mFinalT = System.currentTimeMillis();
        LOG.info("Total time taken for parsing Request Metrics: {}", mFinalT - mCurrT);
    }


    /**
     * Parse all http events generated by shard requests.
     * This function processes http events such as bulk and search. Every operation
     * emits a start event and an end event. The events can be found in
     * /dev/shm/performanceanalyzer/{rotation_window}/threads/{tid}/http/{operation}/{rid}/. The start event data
     * is written to * a file called start and the end event data is in a file called end.
     *
     * @param rootLocation where to find metric files
     * @param currWindowStartTime the start time of current sampling period.
     *   The bound of the period where that value is measured is
     *  MetricsConfiguration.SAMPLING_INTERVAL.
     * @param currWindowEndTime the end time of current sampling period.
     *   The bound of the period where that value is measured is
     *  MetricsConfiguration.SAMPLING_INTERVAL.
     *
     * @throws Exception thrown if we have issues parsing metrics
     */
    public void parseHttpRequestMetrics(String rootLocation, long currWindowStartTime,
            long currWindowEndTime) throws Exception {
        long mCurrT = System.currentTimeMillis();
        if (httpRqMetricsMap.get(currWindowStartTime) == null) {
            HttpRequestMetricsSnapshot httpRqSnap = new HttpRequestMetricsSnapshot(this.conn, currWindowStartTime);
            Map.Entry<Long, HttpRequestMetricsSnapshot> entry = httpRqMetricsMap.lastEntry();
            if (entry != null) {
                httpRqSnap.rolloverInflightRequests(entry.getValue());
            }
            metricsParser.parseHttpMetrics(rootLocation, currWindowStartTime,
                    currWindowEndTime, httpRqSnap);
            LOG.debug(() -> httpRqSnap.fetchAll());
            httpRqMetricsMap.put(currWindowStartTime, httpRqSnap);
            LOG.info("Adding new HTTP RQ snapshot- currTimestamp {}", currWindowStartTime);
        }
        long mFinalT = System.currentTimeMillis();
        LOG.info("Total time taken for parsing HTTP Request Metrics: {}", mFinalT - mCurrT);
    }

    /**
     * Enrich event data with OS metrics and calculate aggregated metrics on dimensions like (shard, index, operation, role).
     * We emit metrics for the previous window interval as we need two metric windows to align OSMetrics.
     * Ex: To emit metrics between 5-10, we need OSMetrics emitted at 8 and 13, to be able to calculate the
     * metrics correctly. The aggregated metrics are then written to a metricsDB.
     *
     * @param currWindowStartTime the start time of current sampling period.
     *   The bound of the period where that value is measured is
     *  MetricsConfiguration.SAMPLING_INTERVAL.
     * @throws Exception thrown if we have issues parsing metrics
     */
    public void emitMetrics(long currWindowStartTime) throws Exception {
        long prevWindowStartTime = currWindowStartTime - MetricsConfiguration.SAMPLING_INTERVAL;

        if (metricsDBMap.get(prevWindowStartTime) != null) {
            LOG.info("The metrics for this timestamp already exist. Skipping.");
            return;
        }

        long mCurrT = System.currentTimeMillis();

        Map.Entry<Long, ShardRequestMetricsSnapshot> prevRqEntry = shardRqMetricsMap.floorEntry(prevWindowStartTime);
        if (prevRqEntry == null) {
            LOG.info("Request snapshot for the previous window does not exist. Not emitting metrics.");
            return;
        }
        ShardRequestMetricsSnapshot prevRqSnap = prevRqEntry.getValue();
        prevWindowStartTime = prevRqEntry.getKey();

        //This is object holds a reference to the temporary os snapshot. It is used to delete tables at the end of this
        //reader cycle. The OSMetricsSnapshot expects windowEndTime in the constructor.
        OSMetricsSnapshot alignedOSSnapHolder = new OSMetricsSnapshot(this.conn, "os_aligned_",
                prevWindowStartTime + MetricsConfiguration.SAMPLING_INTERVAL);

        OSMetricsSnapshot osAlignedSnap = alignOSMetrics(prevWindowStartTime,
                prevWindowStartTime + MetricsConfiguration.SAMPLING_INTERVAL, alignedOSSnapHolder);

        long mFinalT = System.currentTimeMillis();
        LOG.info("Total time taken for aligning OS Metrics: {}", mFinalT - mCurrT);

        if (osAlignedSnap == null) {
            LOG.info("OS snapshot for the previous window does not exist. Not emitting metrics.");
            alignedOSSnapHolder.remove();
            return;
        }

        MetricsDB metricsDB = createMetricsDB(prevWindowStartTime);
        mCurrT = System.currentTimeMillis();
        MetricsEmitter.emitAggregatedOSMetrics(create, metricsDB, osAlignedSnap, prevRqSnap);
        MetricsEmitter.emitWorkloadMetrics(create, metricsDB, prevRqSnap);
        MetricsEmitter.emitThreadNameMetrics(create, metricsDB, osAlignedSnap);
        HttpRequestMetricsSnapshot prevHttpRqSnap = httpRqMetricsMap.get(prevWindowStartTime);
        MetricsEmitter.emitHttpMetrics(create, metricsDB, prevHttpRqSnap);
        alignedOSSnapHolder.remove();
        emitNodeMetrics(currWindowStartTime, metricsDB);
        metricsDB.commit();
        metricsDBMap.put(prevWindowStartTime, metricsDB);
        mFinalT = System.currentTimeMillis();
        LOG.info("Total time taken for emitting Metrics: {}", mFinalT - mCurrT);
    }

    public void processMetrics(String rootLocation, long currTimestamp) throws Exception {
        parseOSMetrics(rootLocation, currTimestamp);
        parseNodeMetrics(currTimestamp);
        long currWindowEndTime = PerformanceAnalyzerMetrics.getTimeInterval(currTimestamp, MetricsConfiguration.SAMPLING_INTERVAL);
        long currWindowStartTime = currWindowEndTime - MetricsConfiguration.SAMPLING_INTERVAL;
        parseRequestMetrics(rootLocation, currWindowStartTime, currWindowEndTime);
        parseHttpRequestMetrics(rootLocation, currWindowStartTime, currWindowEndTime);
        emitMetrics(currWindowStartTime);
    }

    /**
     * Returns per thread OSMetrics between startTime and endTime.
     * OSMetrics might have been collected for windows that dont completely overlap with startTime and endTime.
     * This function calculates the weighted average of metrics in each overlapping window and sums them up to find
     * the average metrics in the requested window.
     *
     * @param startTime  the start time of the previous sampling period.
     *   The bound of the period where that value is measured is
     *  MetricsConfiguration.SAMPLING_INTERVAL.
     * @param endTime the end time of the previous sampling period.
     *   The bound of the period where that value is measured is
     *  MetricsConfiguration.SAMPLING_INTERVAL.
     * @param alignedWindow where we store aligned snapshot
     *
     * @return
     *   alignedWindow if we have two sampled snapshot;
     *   a sampled snapshot if we have only one sampled snapshot within
     *     startTime and endTime;
     *   null if the number of total snapshots is less than OS_SNAPSHOTS or
     *     if there is no snapshot taken after startTime or
     *     right window snapshot ends at or before endTime
     *
     * @throws Exception thrown when we have issues in aligning window
     */
    public OSMetricsSnapshot alignOSMetrics(long startTime, long endTime, OSMetricsSnapshot alignedWindow)
            throws Exception {
        LOG.info("Aligning metrics for {}, {}", startTime, endTime);
        //Find osmetric windows that overlap with the expected window.
        //This is atmost 2 but maybe less than 2. If less than 2, simply return the existing window.

        //If we have insufficient snapshots just return
        if (osMetricsMap.size() < OS_SNAPSHOTS) {
            LOG.error("Exited due to too few snapshots");
            return null;
        }

        Map.Entry<Long, OSMetricsSnapshot> entry = osMetricsMap.ceilingEntry(startTime);
        //There is no snapshot taken after startTime.
        if (entry == null) {
            LOG.error("No OS snapshot above startTime.");
            return null;
        }

        //Start time of the previous snapshot.
        Long t1 = entry.getKey();
        if (t1 == null) {
            LOG.error("We dont have an OS snapshot above startTime.");
            return null;
        }
        //Next higher key.
        Long t2 = osMetricsMap.higherKey(t1);

        if (t2 == null) {
            LOG.error("We dont have the next OS snapshot above startTime.");
            return entry.getValue();
        }

        //t1 and startTime are already aligned. Just return the snapshot between t2 and t1.
        if (t1 == startTime) {
            LOG.info("Found matching OS snapshot.");
            return osMetricsMap.get(t2);
        }

        if (t2 <= endTime) {
            LOG.error("Right window snapshot ends at or before endTime. rw: {}, lw: {}, startTime: {}, endTime: {}",
                    t2, t1, startTime, endTime);
            //TODO: As a quick fix we ignore this window. We might want to consider multiple windows instead.
            return null;
        }

        LOG.info("Adding new scaled OS snapshot- actualTime {}", startTime);
        OSMetricsSnapshot leftWindow = osMetricsMap.get(t1);
        OSMetricsSnapshot rightWindow = osMetricsMap.get(t2);
        OSMetricsSnapshot.alignWindow(leftWindow, rightWindow, alignedWindow.getTableName(),
                t1, startTime, endTime);
        return alignedWindow;
    }

    /**
     * Returns per node metrics between startTime and endTime.
     * These metrics might have been collected for windows that dont completely
     *  overlap with startTime and endTime.
     * This function calculates the weighted average of metrics in each
     *  overlapping window and sums them up to find the average metrics in the
     *   requested window.
     *
     * So in the code, startTime is "a" below, endTime is "b" below. Reader
     *  window is [a, b]. We want to find "x", the cut-off point between two
     *  writer window.
     *
     * Given metrics in two writer windows calculates a new reader window which
     *  overlaps with the given windows.
     * |------leftWindow-------|-------rightWindow--------|
     *                         x
     *            a                              b
     *            |-----------alignedWindow------|
     *
     *
     * We are emitting aligned metrics for previous window, not current window.
     *  This is to make sure we have two windows to align. Otherwise, if we
     *   emit metrics for current window, we might not have two writer window
     *    metrics.
     *
     * If this is the time line:
     *
     *    + writer writes to the left window at 2000l
     *    + reader reads at 6000l
     *    + writer writes to the right window at 7000l
     *    + reader reads at 11000l
     * Then according to PerformanceAnalyzerMetrics.getTimeInterval, the previous reader
     *  window is [0, 5000], current reader window is [5000, 10000].
     *
     * If we align for current reader window, we need writer window ends in
     *  7000l and 12000l. But we don't have 12000l at 11000l.
     *
     * @param metricName the name of the metric we want to align
     * @param metricMap the in-memory database for this metric
     * @param readerStartTime the start time of the previous sampling period.
     *   The bound of the period where that value is measured is
     *  MetricsConfiguration.SAMPLING_INTERVAL.
     * @param readerEndTime the end time of the previous sampling period.
     *   The bound of the period where that value is measured is
     *  MetricsConfiguration.SAMPLING_INTERVAL.
     * @param alignedWindow where we store aligned snapshot
     *
     * @return
     *   alignedWindow if we have two sampled snapshot;
     *   a sampled snapshot if we have only one sampled snapshot within
     *     startTime and endTime;
     *   null if the number of total snapshots is less than OS_SNAPSHOTS or
     *     if there is no snapshot taken after startTime or
     *     right window snapshot ends at or before endTime
     *
     * @throws Exception thrown when we have issues in aligning window
     */
    public MemoryDBSnapshot alignNodeMetrics(AllMetrics.MetricName metricName,
            NavigableMap<Long, MemoryDBSnapshot> metricMap, long readerStartTime,
            long readerEndTime, MemoryDBSnapshot alignedWindow) throws Exception {

        LOG.info("Aligning metrics for {}, from {} to {}", metricName, readerStartTime,
                readerEndTime);
        // Find metric windows that overlap with the expected window.
        // This is at most 2 but maybe less than 2. If less than 2, simply
        // return the existing window.

        // If we have insufficient snapshots just return
        // We need left writer window, right writer window. Also since we are
        // dealing with previous reader window, we need at least 3 snapshots.
        if (metricMap.size() < 3) {
            LOG.error("Exited due to too few snapshots");
            return null;
        }

        // retrieve a snapshot ending at t1 = x
        Map.Entry<Long, MemoryDBSnapshot> entry = metricMap
                .ceilingEntry(readerStartTime);
        // There is no snapshot taken after startTime.
        if (entry == null) {
            LOG.error("No {} snapshot above startTime.", metricName);
            return null;
        }

        // Start time of the previous snapshot.
        Long t1 = entry.getKey();
        if (t1 == null) {
            LOG.error("We dont have an {} snapshot above startTime.", metricName);
            return null;
        }
        // Next higher key representing the end time of the rightWindow above

        Long t2 = metricMap.higherKey(t1);

        if (t2 == null) {
            LOG.error("We dont have the next {} snapshot above startTime.",
                    metricName);
            return entry.getValue();
        }

        // t1 and startTime are already aligned. Just return the snapshot
        // between t2 and t1.
        if (t1 == readerStartTime) {
            LOG.info("Found matching {} snapshot.", metricName);
            return metricMap.get(t2);
        }

        if (t2 <= readerEndTime) {
            LOG.error(
                    "Right window {} snapshot ends at or before endTime. rw: {}, lw: {}, startTime: {}, endTime: {}",
                    metricName, t2, t1, readerStartTime, readerEndTime);
            //TODO: As a quick fix we ignore this window. We might want to consider multiple windows instead.
            return null;
        }

        LOG.info("Adding new scaled {} snapshot- actualTime {}", metricName,
                readerStartTime);
        // retrieve left and right window using osMetricsMap, whose key is the
        // largest last modification time.  We use values in the future to
        // represent values in the past.  So if at t1, writer writes values 1,
        // the interval [t1-sample interval, t1] has value 1.
        MemoryDBSnapshot leftWindow = metricMap.get(t1);
        MemoryDBSnapshot rightWindow = metricMap.get(t2);

        alignedWindow.alignWindow(leftWindow, rightWindow, t1, readerStartTime,
                readerEndTime);
        return alignedWindow;
    }

    public Connection getConnection() {
        return this.conn;
    }

    public DSLContext getDSLContext() {
        return this.create;
    }

    /**
     * This is called by operations outside of the ReaderMetricsProcessor.
     *
     * @return the latest on-disk database
     */
    public Map.Entry<Long, MetricsDB> getMetricsDB() {
        //If metricsDBMap is being trimmed we wait and acquire the latest
        return metricsDBMap.lastEntry();
    }

    public MetricsDB createMetricsDB(long timestamp) throws Exception {
        MetricsDB db = new MetricsDB(timestamp);
        return db;
    }

    public void deleteDBs() throws Exception {
        for (MetricsDB db: metricsDBMap.values()) {
            db.remove();
        }
    }

    /**
     * Enrich event data with node metrics and calculate aggregated metrics on
     * dimensions like (shard, index, operation, role). The aggregated metrics
     * are then written to a metricsDB.
     *
     * @param currWindowStartTime the start time of current sampling period.
     *   The bound of the period where that value is measured is
     *   MetricsConfiguration.SAMPLING_INTERVAL.
     * @param metricsDB on-disk database to which we want to emit metrics
     *
     * @throws Exception if we have issues emitting or aligning metrics
     */
    public void emitNodeMetrics(long currWindowStartTime, MetricsDB metricsDB)
            throws Exception {
        long prevWindowStartTime = currWindowStartTime
                - MetricsConfiguration.SAMPLING_INTERVAL;

        for (Map.Entry<AllMetrics.MetricName, NavigableMap<Long, MemoryDBSnapshot>> entry : nodeMetricsMap
                .entrySet()) {

            MetricName metricName = entry.getKey();

            NavigableMap<Long, MemoryDBSnapshot> metricMap = entry.getValue();

            if (metricMap.get(prevWindowStartTime) != null) {
                LOG.info(
                        "The metrics in {} for this timestamp already exist. Skipping.",
                        metricName);
                return;
            }

            long mCurrT = System.currentTimeMillis();

            // This is object holds a reference to the temporary memory db
            // snapshot. It is used to delete tables at the end of this
            // reader cycle.

            MemoryDBSnapshot alignedSnapshotHolder = new MemoryDBSnapshot(
                    getConnection(), metricName, currWindowStartTime, true);
            MemoryDBSnapshot alignedSnapshot = alignNodeMetrics(metricName,
                    metricMap, prevWindowStartTime, currWindowStartTime,
                    alignedSnapshotHolder);

            long mFinalT = System.currentTimeMillis();
            LOG.info("Total time taken for aligning {} Metrics: {}", metricName,
                    mFinalT - mCurrT);

            if (alignedSnapshot == null) {
                alignedSnapshotHolder.remove();
                LOG.info(
                        "{} snapshot for the previous window does not exist. Not emitting metrics.",
                        metricName);
                continue;
            }

            mCurrT = System.currentTimeMillis();
            MetricsEmitter.emitNodeMetrics(create, metricsDB, alignedSnapshot);

            // alignedSnapshotHolder cannot be the left or right window we are
            // trying to align, so we can safely remove.
            alignedSnapshotHolder.remove();

            mFinalT = System.currentTimeMillis();
            LOG.info("Total time taken for emitting node metrics: {}",
                    mFinalT - mCurrT);
        }
    }

    @VisibleForTesting
    Map<AllMetrics.MetricName, NavigableMap<Long, MemoryDBSnapshot>> getNodeMetricsMap() {
        return nodeMetricsMap;
    }

    @VisibleForTesting
    void putNodeMetricsMap(AllMetrics.MetricName name,
            NavigableMap<Long, MemoryDBSnapshot> metricsMap) {
        this.nodeMetricsMap.put(name, metricsMap);
    }
}

