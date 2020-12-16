package com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors;

import static org.junit.Assert.*;
import static org.mockito.MockitoAnnotations.initMocks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ESResources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.TestUtil;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.apache.commons.lang3.SystemUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.SourcePrioritizedRunnable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class MasterServiceEventMetricsTests {
  private long startTimeInMills = 1153721339;
  private MasterServiceEventMetrics masterServiceEventMetrics;
  private ThreadPool threadPool;

  @Before
  public void init() {
    // this test only runs in Linux system
    // as some of the static members of the ThreadList class are specific to Linux
    org.junit.Assume.assumeTrue(SystemUtils.IS_OS_LINUX);

    threadPool = new TestThreadPool("test");
    ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
    ESResources.INSTANCE.setClusterService(clusterService);

    MetricsConfiguration.CONFIG_MAP.put(MasterServiceEventMetrics.class, MetricsConfiguration.cdefault);
    masterServiceEventMetrics = new MasterServiceEventMetrics();
  }

  @After
  public void tearDown() throws Exception {
    threadPool.shutdownNow();
  }

  @Test
  public void testGetMetricsPath() {
    String expectedPath = PluginSettings.instance().getMetricsLocation()
            + PerformanceAnalyzerMetrics.getTimeInterval(startTimeInMills) + "/"
            + PerformanceAnalyzerMetrics.sThreadsPath + "/" + "thread123" + "/"
            + PerformanceAnalyzerMetrics.sMasterTaskPath + "/" + "task123" +"/"
            + PerformanceAnalyzerMetrics.FINISH_FILE_NAME;
    String actualPath = masterServiceEventMetrics.getMetricsPath(startTimeInMills, "thread123", "task123", PerformanceAnalyzerMetrics.FINISH_FILE_NAME);
    assertEquals(expectedPath, actualPath);

    try {
      masterServiceEventMetrics.getMetricsPath(startTimeInMills, "thread123", "task123");
      fail("Negative scenario test: Should have been a RuntimeException");
    } catch (RuntimeException ex) {
      //- expecting exception...2 values passed; 3 expected
    }
  }


  @Test
  public void testGenerateFinishMetrics() {
    assertEquals(-1 ,     masterServiceEventMetrics.lastTaskInsertionOrder);
    masterServiceEventMetrics.generateFinishMetrics(startTimeInMills);

    masterServiceEventMetrics.lastTaskInsertionOrder = 1;
    masterServiceEventMetrics.generateFinishMetrics(startTimeInMills);
    List<Event> metrics = TestUtil.readEvents();
    String[] jsonStrs = metrics.get(0).value.split("\n");
    assert jsonStrs.length == 2;
    assertTrue(jsonStrs[1].contains(AllMetrics.MasterMetricValues.FINISH_TIME.toString()));
    assertEquals(-1 ,     masterServiceEventMetrics.lastTaskInsertionOrder);
  }

  @Test
  public void testCollectMetrics() throws Exception {
    PrioritizedEsThreadPoolExecutor prioritizedEsThreadPoolExecutor = (PrioritizedEsThreadPoolExecutor) masterServiceEventMetrics
        .getMasterServiceTPExecutorField().get(ESResources.INSTANCE.getClusterService().getMasterService());
    SourcePrioritizedRunnable runnable = new SourcePrioritizedRunnable(Priority.HIGH, "_add_listener_") {
      @Override
      public void run() {
        try {
          Thread.sleep(100); //dummy runnable
        } catch (InterruptedException e) {
        }
      }
    };

    prioritizedEsThreadPoolExecutor.submit(runnable);
    System.out.println("after submit: " + System.currentTimeMillis());
    System.out.println("before collect: " + System.currentTimeMillis());
    masterServiceEventMetrics.collectMetrics(startTimeInMills);
    List<String> jsonStrs = readMetricsInJsonString();
    assertTrue(jsonStrs.get(0).contains(AllMetrics.MasterMetricDimensions.MASTER_TASK_PRIORITY.toString()));
    assertTrue(jsonStrs.get(1).contains(AllMetrics.MasterMetricValues.START_TIME.toString()));
    assertTrue(jsonStrs.get(2).contains(AllMetrics.MasterMetricDimensions.MASTER_TASK_TYPE.toString()));
    assertTrue(jsonStrs.get(3).contains(AllMetrics.MasterMetricDimensions.MASTER_TASK_METADATA.toString()));
    assertTrue(jsonStrs.get(4).contains(AllMetrics.MasterMetricDimensions.MASTER_TASK_QUEUE_TIME.toString()));
  }


  private List<String> readMetricsInJsonString() {
    List<Event> metrics = TestUtil.readEvents();
    assert metrics.size() == 1;
    String[] jsonStrs = metrics.get(0).value.split("\n");
    assert jsonStrs.length == 6;
    return Arrays.asList(jsonStrs).subList(1, jsonStrs.length);
  }
}
