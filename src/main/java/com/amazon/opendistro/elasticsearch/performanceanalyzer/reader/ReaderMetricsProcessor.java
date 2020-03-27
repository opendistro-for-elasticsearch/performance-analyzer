/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.reader;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatExceptionCode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.MetricName;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.EventLog;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.EventLogFileHandler;
import com.google.common.annotations.VisibleForTesting;
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

public class ReaderMetricsProcessor implements Runnable {
  private static final Logger LOG = LogManager.getLogger(ReaderMetricsProcessor.class);

  private static final String DB_URL = "jdbc:sqlite:";
  private final Connection conn;
  private final DSLContext create;

  // This semaphore is used to control access to metricsDBMap from threads outside of
  // ReaderMetricsProcessor.
  private NavigableMap<Long, MetricsDB> metricsDBMap;
  private NavigableMap<Long, OSMetricsSnapshot> osMetricsMap;
  private NavigableMap<Long, ShardRequestMetricsSnapshot> shardRqMetricsMap;
  private NavigableMap<Long, HttpRequestMetricsSnapshot> httpRqMetricsMap;
  private NavigableMap<Long, MasterEventMetricsSnapshot> masterEventMetricsMap;
  private Map<MetricName, NavigableMap<Long, MemoryDBSnapshot>> nodeMetricsMap;
  private static final int MAX_DATABASES = 2;
  private static final int OS_SNAPSHOTS = 4;
  private static final int RQ_SNAPSHOTS = 4;
  private static final int HTTP_RQ_SNAPSHOTS = 4;
  private static final int MASTER_EVENT_SNAPSHOTS = 4;
  private final MetricsParser metricsParser;
  private final String rootLocation;
  private static final Map<String, Double> TIMING_STATS = new HashMap<>();
  private static final Map<String, String> STATS_DATA = new HashMap<>();

  static {
    STATS_DATA.put("MethodName", "ProcessMetrics");
  }

  private final boolean processNewFormat;
  private final EventLogFileHandler eventLogFileHandler;
  private static ReaderMetricsProcessor current = null;

  public static void setCurrentInstance(ReaderMetricsProcessor currentInstance) {
    current = currentInstance;
  }

  public static ReaderMetricsProcessor getInstance() {
    return current;
  }

  public ReaderMetricsProcessor(String rootLocation) throws Exception {
    this(rootLocation, false);
  }

  public ReaderMetricsProcessor(String rootLocation, boolean processNewFormat) throws Exception {
    conn = DriverManager.getConnection(DB_URL);
    create = DSL.using(conn, SQLDialect.SQLITE);
    metricsDBMap = new ConcurrentSkipListMap<>();
    osMetricsMap = new TreeMap<>();
    shardRqMetricsMap = new TreeMap<>();
    httpRqMetricsMap = new TreeMap<>();
    masterEventMetricsMap = new TreeMap<>();
    metricsParser = new MetricsParser();
    this.rootLocation = rootLocation;

    AllMetrics.MetricName[] names = AllMetrics.MetricName.values();
    nodeMetricsMap = new HashMap<>(names.length);
    for (int i = 0; i < names.length; i++) {
      nodeMetricsMap.put(names[i], new TreeMap<>());
    }
    eventLogFileHandler = new EventLogFileHandler(new EventLog(), rootLocation);
    this.processNewFormat = processNewFormat;
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

      long runInterval = MetricsConfiguration.SAMPLING_INTERVAL / 2;

      while (true) {
        // Create snapshots.
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
        processMetrics(rootLocation, startTime);
        trimOldSnapshots();
        conn.commit();
        conn.setAutoCommit(true);
        long duration = System.currentTimeMillis() - startTime;
        LOG.debug("Total time taken: {}", duration);
        if (duration < runInterval) {
          Thread.sleep(runInterval - duration);
        }
      }
    } catch (Throwable e) {
      LOG.error(
          (Supplier<?>)
              () ->
                  new ParameterizedMessage(
                      "READER PROCESSOR ERROR. NEEDS DEBUGGING {} ExceptionCode: {}.",
                      StatExceptionCode.OTHER.toString(),
                      e.toString()),
          e);
      StatsCollector.instance().logException();

      try {
        long duration = System.currentTimeMillis() - startTime;
        if (duration < MetricsConfiguration.SAMPLING_INTERVAL) {
          Thread.sleep(MetricsConfiguration.SAMPLING_INTERVAL - duration);
        }
      } catch (Exception ex) {
        LOG.error("Exception in sleep: {}", () -> ex);
      }
      throw new RuntimeException("READER ERROR");
    } finally {
      try {
        shutdown();
        LOG.error("Connection to the database was closed.");
      } catch (Exception e) {
        LOG.error("Unable to close all database connections and shutdown cleanly.");
      }
    }
  }

  public void shutdown() {
    try {
      conn.close();
    } catch (Exception e) {
      LOG.error("Unable to close inmemory database connection.");
    }

    for (MetricsDB db : metricsDBMap.values()) {
      try {
        db.close();
      } catch (Exception e) {
        LOG.error("Unable to close database - {}", db.getDBFilePath());
      }
    }
  }

  public void trimOldSnapshots() throws Exception {
    trimMap(osMetricsMap, OS_SNAPSHOTS);
    trimMap(shardRqMetricsMap, RQ_SNAPSHOTS);
    trimMap(httpRqMetricsMap, HTTP_RQ_SNAPSHOTS);
    trimMap(masterEventMetricsMap, MASTER_EVENT_SNAPSHOTS);
    trimDatabases(
        metricsDBMap, MAX_DATABASES, PluginSettings.instance().shouldCleanupMetricsDBFiles());

    for (NavigableMap<Long, MemoryDBSnapshot> snap : nodeMetricsMap.values()) {
      // do the same thing as OS_SNAPSHOTS.  Eventually MemoryDBSnapshot
      // will replace OSMetricsSnapshot as we want to our code to be
      // stable.
      trimMap(snap, OS_SNAPSHOTS);
    }
  }

  /** Deletes the lowest entries in the map till the size of the map is equal to maxSize. */
  private void trimMap(NavigableMap<Long, ?> map, int maxSize) throws Exception {
    // Remove the oldest entries from the map
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
   * Deletes the MetricsDB entries in the map till the size of the map is equal to maxSize. The
   * actual on-disk files is deleted ony if the config is not set or set to true.
   */
  public static void trimDatabases(
      NavigableMap<Long, MetricsDB> map, int maxSize, boolean deleteDBFiles) throws Exception {
    // Remove the oldest entries from the map, upto maxSize.
    while (map.size() > maxSize) {
      Map.Entry<Long, MetricsDB> lowestEntry = map.firstEntry();
      if (lowestEntry != null) {
        MetricsDB value = lowestEntry.getValue();
        map.remove(lowestEntry.getKey());
        value.remove();
        if (deleteDBFiles) {
          value.deleteOnDiskFile();
        }
      }
    }
  }

  /**
   * Enrich event data with OS metrics and calculate aggregated metrics on dimensions like (shard,
   * index, operation, role). We emit metrics for the previous window interval as we need two metric
   * windows to align OSMetrics. Ex: To emit metrics between 5-10, we need OSMetrics emitted at 8
   * and 13, to be able to calculate the metrics correctly. The aggregated metrics are then written
   * to a metricsDB.
   *
   * @param currWindowStartTime the start time of current sampling period. The bound of the period
   *     where that value is measured is MetricsConfiguration.SAMPLING_INTERVAL.
   * @throws Exception thrown if we have issues parsing metrics
   */
  private void emitMetrics(long currWindowStartTime) throws Exception {
    long prevWindowStartTime = currWindowStartTime - MetricsConfiguration.SAMPLING_INTERVAL;

    if (metricsDBMap.get(prevWindowStartTime) != null) {
      LOG.debug("The metrics for this timestamp already exist. Skipping.");
      return;
    }

    long mCurrT = System.currentTimeMillis();
    // This is object holds a reference to the temporary os snapshot. It is used to delete tables at
    // the end of this
    // reader cycle. The OSMetricsSnapshot expects windowEndTime in the constructor.
    OSMetricsSnapshot alignedOSSnapHolder =
        new OSMetricsSnapshot(this.conn, "os_aligned_", currWindowStartTime);
    OSMetricsSnapshot osAlignedSnap =
        alignOSMetrics(
            prevWindowStartTime,
            prevWindowStartTime + MetricsConfiguration.SAMPLING_INTERVAL,
            alignedOSSnapHolder);

    long mFinalT = System.currentTimeMillis();
    LOG.debug("Total time taken for aligning OS Metrics: {}", mFinalT - mCurrT);

    mCurrT = System.currentTimeMillis();
    MetricsDB metricsDB = createMetricsDB(prevWindowStartTime);

    emitMasterMetrics(prevWindowStartTime, metricsDB);
    emitShardRequestMetrics(prevWindowStartTime, alignedOSSnapHolder, osAlignedSnap, metricsDB);
    emitHttpRequestMetrics(prevWindowStartTime, metricsDB);
    emitNodeMetrics(currWindowStartTime, metricsDB);

    metricsDB.commit();
    metricsDBMap.put(prevWindowStartTime, metricsDB);
    mFinalT = System.currentTimeMillis();
    LOG.debug("Total time taken for emitting Metrics: {}", mFinalT - mCurrT);
    TIMING_STATS.put("emitMetrics", (double) (mFinalT - mCurrT));
  }

  private void emitHttpRequestMetrics(long prevWindowStartTime, MetricsDB metricsDB)
      throws Exception {

    if (httpRqMetricsMap.containsKey(prevWindowStartTime)) {

      HttpRequestMetricsSnapshot prevHttpRqSnap = httpRqMetricsMap.get(prevWindowStartTime);
      MetricsEmitter.emitHttpMetrics(create, metricsDB, prevHttpRqSnap);
    } else {
      LOG.debug(
          "Http request snapshot for the previous window does not exist. Not emitting metrics.");
    }
  }

  private void emitShardRequestMetrics(
      long prevWindowStartTime,
      OSMetricsSnapshot alignedOSSnapHolder,
      OSMetricsSnapshot osAlignedSnap,
      MetricsDB metricsDB)
      throws Exception {

    if (shardRqMetricsMap.containsKey(prevWindowStartTime)) {

      ShardRequestMetricsSnapshot preShardRequestMetricsSnapshot =
          shardRqMetricsMap.get(prevWindowStartTime);
      LOG.debug(
          "shard emit time {}, {}",
          prevWindowStartTime,
          preShardRequestMetricsSnapshot.windowStartTime);
      MetricsEmitter.emitWorkloadMetrics(
          create, metricsDB, preShardRequestMetricsSnapshot); // calculate latency
      if (osAlignedSnap != null) {
        // LOG.info(osAlignedSnap.fetchAll());
        // LOG.info(preShardRequestMetricsSnapshot.fetchAll());
        MetricsEmitter.emitAggregatedOSMetrics(
            create, metricsDB, osAlignedSnap, preShardRequestMetricsSnapshot); // table join
        MetricsEmitter.emitThreadNameMetrics(
            create, metricsDB, osAlignedSnap); // threads other than bulk and query
      } else {
        LOG.debug("OS METRICS NULL");
      }
      alignedOSSnapHolder.remove();
    } else {
      LOG.debug(
          "Shard request snapshot for the previous window does not exist. Not emitting metrics.");
    }
  }

  private void emitMasterMetrics(long prevWindowStartTime, MetricsDB metricsDB) {

    if (masterEventMetricsMap.containsKey(prevWindowStartTime)) {

      MasterEventMetricsSnapshot preMasterEventSnapshot =
          masterEventMetricsMap.get(prevWindowStartTime);
      MetricsEmitter.emitMasterEventMetrics(metricsDB, preMasterEventSnapshot);
    } else {
      LOG.debug("Master snapshot for the previous window does not exist. Not emitting metrics.");
    }
  }

  /**
   * OS, Request, Http and master first aligns the currentTimeStamp with a 5 second interval but
   * while looking for the actual file on the disk, they do a further aligning with the 30 second
   * bucket. The node metrics doesn't do the initial 5 second aligning but does a direct 30 second
   * aligning of the given time, before reading files on the disk. OS metrics can do (in some cases)
   * and the Node metrics definitely reads a file from the previous 30 second aligned timestamp.
   *
   * <p>Note that previously, the collectors would sample metrics every 5 seconds but write it to a
   * 30 second aligned bucket. So, the files in the current bucket gets overwritten on every write
   * until we hit the 30 second boundary after which a new directory is created and that is used the
   * same way for the next 30 seconds. So any time we are reading from the last bucket, we are
   * reading the last file before the bucket got rotated. So, at the first 5 seconds of the current
   * bucket, if you read the last bucket you are actually reading the file written in last 5 second.
   * If you are reading from the previous bucket in the second 5 second window, then you are
   * actually reading the file written in the last 10 seconds.
   *
   * <p>In the new format, a file (not a directory) is written every 5 seconds. So we can actually
   * read the last 5 second of the data.
   *
   * @param rootLocation Where to read the files from
   * @param currTimestamp The timestamp of the file that will be picked.
   * @throws Exception It can throw exception
   */
  public void processMetrics(String rootLocation, long currTimestamp) throws Exception {
    TIMING_STATS.clear();

    /*
     Querying a file by timestamp:
     1. Get the current system timestamp.
     2. Round it to the SAMPLING_TIME bucket. So, for a bucket width of
        5, a timestamp of 17 is dropped into the bucket numbered 15.
     3. Go, three windows back from the value you get in step 2. But you
        ask why ?
        The reason is the purger thread on the writer runs a sampling
        window behind the current timestamp. So for a current wall-clock
        time of 17 seconds, it will be purging events generated between
        wall clock time of 10 - 14 seconds. The purger does that
        because all collectors for the time window, 15 - 20 may not
        have run so far and we don't want to drop events on the wrong
        files. Now because the purger gathers the events in the window
        10 - 14 and writes them to a file named '10'.
        Now if the reader looks for the bucket 15, it will most certainly
        not find it, because it has not been created yet. If it looks
        for the bucket 10, it may or may not find it based on the fact
        if the purger has purged everything for the window of 10. There
        is a race here. The safest thing to do is to look for the
        window 5 because that is guaranteed to be be written by the
        purger at the moment, unless it has crashed and missed writing
        the file.
        However, there is a race condition here if let's say the writer is
        writing data at time 19.99 seconds. This write still falls into the
        bucket (10-15). At 20.01 the reader assumes that the bucket (10-15)
        is ready so it starts to read that file (go back two windows and
        fetch the file 10) But since writer just finished writing to
        the 10.tmp, it might not get enough to rotate that file before
        20.01. So race condition occurs. We have to add one additional window
        on reader to avoid this.
    */

    // Step 1 from above.
    long start = System.currentTimeMillis();

    // Step 2 from above.
    long currWindowStartTime =
        PerformanceAnalyzerMetrics.getTimeInterval(
            currTimestamp, MetricsConfiguration.SAMPLING_INTERVAL);

    // Step 3 from above.
    currWindowStartTime = currWindowStartTime - (3 * MetricsConfiguration.SAMPLING_INTERVAL);
    long currWindowEndTime = currWindowStartTime + MetricsConfiguration.SAMPLING_INTERVAL;

    EventProcessor osProcessor =
        OSEventProcessor.buildOSMetricEventsProcessor(
            currWindowStartTime, currWindowEndTime, conn, osMetricsMap);
    EventProcessor requestProcessor =
        RequestEventProcessor.buildRequestMetricEventsProcessor(
            currWindowStartTime, currWindowEndTime, conn, shardRqMetricsMap);
    EventProcessor httpProcessor =
        HttpRequestEventProcessor.buildHttpRequestMetricEventsProcessor(
            currWindowStartTime, currWindowEndTime, conn, httpRqMetricsMap);
    EventProcessor masterEventsProcessor =
        MasterMetricsEventProcessor.buildMasterMetricEventsProcessor(
            currWindowStartTime, conn, masterEventMetricsMap);
    EventProcessor nodeEventsProcessor =
        NodeMetricsEventProcessor.buildNodeMetricEventsProcessor(
            currWindowStartTime, conn, nodeMetricsMap);
    EventProcessor clusterDetailsEventsProcessor = new ClusterDetailsEventProcessor();

    // The event dispatcher dispatches events to each of the registered event processors.
    // In addition to event processing each processor has an initialize/finalize function that is
    // called
    // at the beginning and end of processing respectively.
    // We need to ensure that all the processors are registered, before the initialize function is
    // called.
    // After all events have been processed, we call the finalizeProcessing function.
    EventDispatcher eventDispatcher = new EventDispatcher();

    eventDispatcher.registerEventProcessor(osProcessor);
    eventDispatcher.registerEventProcessor(requestProcessor);
    eventDispatcher.registerEventProcessor(httpProcessor);
    eventDispatcher.registerEventProcessor(nodeEventsProcessor);
    eventDispatcher.registerEventProcessor(masterEventsProcessor);
    eventDispatcher.registerEventProcessor(clusterDetailsEventsProcessor);

    eventDispatcher.initializeProcessing(
        currWindowStartTime, currWindowStartTime + MetricsConfiguration.SAMPLING_INTERVAL);

    eventLogFileHandler.read(currWindowStartTime, eventDispatcher);

    eventDispatcher.finalizeProcessing();

    emitMetrics(currWindowStartTime);

    StatsCollector.instance()
                  .logStatsRecord(null, STATS_DATA, TIMING_STATS, start, System.currentTimeMillis());
  }

  /**
   * Returns per thread OSMetrics between startTime and endTime. OSMetrics might have been collected
   * for windows that dont completely overlap with startTime and endTime. This function calculates
   * the weighted average of metrics in each overlapping window and sums them up to find the average
   * metrics in the requested window.
   *
   * @param startTime the start time of the previous sampling period. The bound of the period where
   *     that value is measured is MetricsConfiguration.SAMPLING_INTERVAL.
   * @param endTime the end time of the previous sampling period. The bound of the period where that
   *     value is measured is MetricsConfiguration.SAMPLING_INTERVAL.
   * @param alignedWindow where we store aligned snapshot
   * @return alignedWindow if we have two sampled snapshot; a sampled snapshot if we have only one
   *     sampled snapshot within startTime and endTime; null if the number of total snapshots is
   *     less than OS_SNAPSHOTS or if there is no snapshot taken after startTime or right window
   *     snapshot ends at or before endTime
   * @throws Exception thrown when we have issues in aligning window
   */
  public OSMetricsSnapshot alignOSMetrics(
      long startTime, long endTime, OSMetricsSnapshot alignedWindow) throws Exception {
    LOG.debug("Aligning metrics for {}, {}", startTime, endTime);
    // Find osmetric windows that overlap with the expected window.
    // This is atmost 2 but maybe less than 2. If less than 2, simply return the existing window.

    // If we have insufficient snapshots just return
    if (osMetricsMap.size() < OS_SNAPSHOTS) {
      LOG.warn("Exited due to too few snapshots - {}", osMetricsMap.size());
      return null;
    }

    Map.Entry<Long, OSMetricsSnapshot> entry = osMetricsMap.higherEntry(startTime);
    // There is no snapshot taken after startTime.
    if (entry == null) {
      LOG.warn("No OS snapshot above startTime.");
      return null;
    }

    // Start time of the previous snapshot.
    Long t1 = entry.getKey();
    if (t1 == null) {
      LOG.error("We dont have an OS snapshot above startTime.");
      return null;
    }
    // Next higher key.
    Long t2 = osMetricsMap.higherKey(t1);

    if (t2 == null) {
      LOG.error("We dont have the next OS snapshot above startTime.");
      return entry.getValue();
    }

    if (t2 < endTime) {
      LOG.error(
          "Right window snapshot ends before endTime. rw: {}, lw: {}, startTime: {}, endTime: {}",
          t2,
          t1,
          startTime,
          endTime);
      // TODO: As a quick fix we ignore this window. We might want to consider multiple windows
      // instead.
      return null;
    }

    LOG.debug("Adding new scaled OS snapshot- actualTime {}", startTime);
    OSMetricsSnapshot leftWindow = osMetricsMap.get(t1);
    OSMetricsSnapshot rightWindow = osMetricsMap.get(t2);
    OSMetricsSnapshot.alignWindow(
        leftWindow, rightWindow, alignedWindow.getTableName(), startTime, endTime);
    return alignedWindow;
  }

  /**
   * Returns per node metrics between startTime and endTime. These metrics might have been collected
   * for windows that dont completely overlap with startTime and endTime. This function calculates
   * the weighted average of metrics in each overlapping window and sums them up to find the average
   * metrics in the requested window.
   *
   * <p>So in the code, startTime is "a" below, endTime is "b" below. Reader window is [a, b]. We
   * want to find "x", the cut-off point between two writer window.
   *
   * <p>Given metrics in two writer windows calculates a new reader window which overlaps with the
   * given windows. |------leftWindow-------|-------rightWindow--------| x a b
   * |-----------alignedWindow------|
   *
   * <p>We are emitting aligned metrics for previous window, not current window. This is to make
   * sure we have two windows to align. Otherwise, if we emit metrics for current window, we might
   * not have two writer window metrics.
   *
   * <p>If this is the time line:
   *
   * <p>+ writer writes to the left window at 2000l + reader reads at 6000l + writer writes to the
   * right window at 7000l + reader reads at 11000l Then according to
   * PerformanceAnalyzerMetrics.getTimeInterval, the previous reader window is [0, 5000], current
   * reader window is [5000, 10000].
   *
   * <p>If we align for current reader window, we need writer window ends in 7000l and 12000l. But
   * we don't have 12000l at 11000l.
   *
   * @param metricName the name of the metric we want to align
   * @param metricMap the in-memory database for this metric
   * @param readerStartTime the start time of the previous sampling period. The bound of the period
   *     where that value is measured is MetricsConfiguration.SAMPLING_INTERVAL.
   * @param readerEndTime the end time of the previous sampling period. The bound of the period
   *     where that value is measured is MetricsConfiguration.SAMPLING_INTERVAL.
   * @param alignedWindow where we store aligned snapshot
   * @return alignedWindow if we have two sampled snapshot; a sampled snapshot if we have only one
   *     sampled snapshot within startTime and endTime; null if the number of total snapshots is
   *     less than OS_SNAPSHOTS or if there is no snapshot taken after startTime or right window
   *     snapshot ends at or before endTime
   * @throws Exception thrown when we have issues in aligning window
   */
  public MemoryDBSnapshot alignNodeMetrics(
      AllMetrics.MetricName metricName,
      NavigableMap<Long, MemoryDBSnapshot> metricMap,
      long readerStartTime,
      long readerEndTime,
      MemoryDBSnapshot alignedWindow)
      throws Exception {

    LOG.debug(
        "Aligning node metrics for {}, from {} to {}", metricName, readerStartTime, readerEndTime);
    // Find metric windows that overlap with the expected window.
    // This is at most 2 but maybe less than 2. If less than 2, simply
    // return the existing window.

    // If we have insufficient snapshots just return
    // We need left writer window, right writer window. Also since we are
    // dealing with previous reader window, we need at least 3 snapshots.
    if (metricMap.size() < 3) {
      LOG.warn("Exited node metrics for {}, due to too few snapshots", metricName);
      return null;
    }

    // retrieve a snapshot ending at t1 = x
    Map.Entry<Long, MemoryDBSnapshot> entry = metricMap.ceilingEntry(readerStartTime);
    // There is no snapshot taken after startTime.
    if (entry == null) {
      LOG.warn("No {} metrics snapshot above startTime.", metricName);
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
      LOG.error("We dont have the next {} snapshot above startTime.", metricName);
      return entry.getValue();
    }

    // t1 and startTime are already aligned. Just return the snapshot
    // between t2 and t1.
    if (t1 == readerStartTime) {
      LOG.debug("Found matching {} snapshot.", metricName);
      return metricMap.get(t2);
    }

    if (t2 <= readerEndTime) {
      LOG.error(
          "Right window {} snapshot ends at or before endTime. rw: {}, lw: {}, startTime: {}, endTime: {}",
          metricName,
          t2,
          t1,
          readerStartTime,
          readerEndTime);
      // TODO: As a quick fix we ignore this window. We might want to consider multiple windows
      // instead.
      return null;
    }

    LOG.debug("Adding new scaled {} snapshot- actualTime {}", metricName, readerStartTime);
    // retrieve left and right window using osMetricsMap, whose key is the
    // largest last modification time.  We use values in the future to
    // represent values in the past.  So if at t1, writer writes values 1,
    // the interval [t1-sample interval, t1] has value 1.
    MemoryDBSnapshot leftWindow = metricMap.get(t1);
    MemoryDBSnapshot rightWindow = metricMap.get(t2);

    alignedWindow.alignWindow(leftWindow, rightWindow, t1, readerStartTime, readerEndTime);
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
    // If metricsDBMap is being trimmed we wait and acquire the latest
    return metricsDBMap.lastEntry();
  }

  public MetricsDB createMetricsDB(long timestamp) throws Exception {
    MetricsDB db = new MetricsDB(timestamp);
    return db;
  }

  public void deleteDBs() throws Exception {
    for (MetricsDB db : metricsDBMap.values()) {
      db.remove();
    }
  }

  /**
   * Enrich event data with node metrics and calculate aggregated metrics on dimensions like (shard,
   * index, operation, role). The aggregated metrics are then written to a metricsDB.
   *
   * @param currWindowStartTime the start time of current sampling period. The bound of the period
   *     where that value is measured is MetricsConfiguration.SAMPLING_INTERVAL.
   * @param metricsDB on-disk database to which we want to emit metrics
   * @throws Exception if we have issues emitting or aligning metrics
   */
  public void emitNodeMetrics(long currWindowStartTime, MetricsDB metricsDB) throws Exception {
    long prevWindowStartTime = currWindowStartTime - MetricsConfiguration.SAMPLING_INTERVAL;

    for (Map.Entry<MetricName, NavigableMap<Long, MemoryDBSnapshot>> entry :
        nodeMetricsMap.entrySet()) {

      MetricName metricName = entry.getKey();

      NavigableMap<Long, MemoryDBSnapshot> metricMap = entry.getValue();

      long mCurrT = System.currentTimeMillis();

      // This is object holds a reference to the temporary memory db
      // snapshot. It is used to delete tables at the end of this
      // reader cycle.

      MemoryDBSnapshot alignedSnapshotHolder =
          new MemoryDBSnapshot(getConnection(), metricName, currWindowStartTime, true);
      MemoryDBSnapshot alignedSnapshot =
          alignNodeMetrics(
              metricName,
              metricMap,
              prevWindowStartTime,
              currWindowStartTime,
              alignedSnapshotHolder);

      long mFinalT = System.currentTimeMillis();
      LOG.debug("Total time taken for aligning {} Metrics: {}", metricName, mFinalT - mCurrT);

      if (alignedSnapshot == null) {
        alignedSnapshotHolder.remove();
        LOG.debug(
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
      LOG.debug("Total time taken for emitting node metrics: {}", mFinalT - mCurrT);
    }
  }

  @VisibleForTesting
  Map<MetricName, NavigableMap<Long, MemoryDBSnapshot>> getNodeMetricsMap() {
    return nodeMetricsMap;
  }

  @VisibleForTesting
  NavigableMap<Long, OSMetricsSnapshot> getOsMetricsMap() {
    return osMetricsMap;
  }

  @VisibleForTesting
  EventLogFileHandler getEventLogFileHandler() {
    return eventLogFileHandler;
  }

  @VisibleForTesting
  NavigableMap<Long, ShardRequestMetricsSnapshot> getShardRequestMetricsMap() {
    return shardRqMetricsMap;
  }

  @VisibleForTesting
  NavigableMap<Long, HttpRequestMetricsSnapshot> getHttpRqMetricsMap() {
    return httpRqMetricsMap;
  }

  @VisibleForTesting
  NavigableMap<Long, MasterEventMetricsSnapshot> getMasterEventMetricsMap() {
    return masterEventMetricsMap;
  }

  @VisibleForTesting
  void putNodeMetricsMap(
      AllMetrics.MetricName name, NavigableMap<Long, MemoryDBSnapshot> metricsMap) {
    this.nodeMetricsMap.put(name, metricsMap);
  }
}
