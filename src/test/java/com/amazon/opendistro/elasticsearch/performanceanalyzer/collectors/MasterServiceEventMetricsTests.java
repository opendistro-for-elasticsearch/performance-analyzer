/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ESResources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.TestUtil;
import java.util.List;
import org.apache.commons.lang3.SystemUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.SourcePrioritizedRunnable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class MasterServiceEventMetricsTests {
  private long startTimeInMills = 1153721339;
  private MasterServiceEventMetrics masterServiceEventMetrics;
  private ThreadPool threadPool;

  @BeforeClass
  public static void setup() {
    // this test only runs in Linux system
    // as some of the static members of the ThreadList class are specific to Linux
    org.junit.Assume.assumeTrue(SystemUtils.IS_OS_LINUX);
  }

  @Before
  public void init() {
    threadPool = new TestThreadPool("test");
    ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
    ESResources.INSTANCE.setClusterService(clusterService);

    MetricsConfiguration.CONFIG_MAP.put(MasterServiceEventMetrics.class, MetricsConfiguration.cdefault);
    masterServiceEventMetrics = new MasterServiceEventMetrics();

    //clean metricQueue before running every test
    TestUtil.readEvents();
  }

  @After
  public void tearDown(){
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
          Thread.sleep(100L); //dummy runnable
        } catch (InterruptedException e) {
        }
      }
    };

    prioritizedEsThreadPoolExecutor.submit(runnable);
    Thread.sleep(1L); // don't delete it

    masterServiceEventMetrics.collectMetrics(startTimeInMills);
    List<String> jsonStrs = TestUtil.readMetricsInJsonString(6);
    assertTrue(jsonStrs.get(0).contains(AllMetrics.MasterMetricDimensions.MASTER_TASK_PRIORITY.toString()));
    assertTrue(jsonStrs.get(1).contains(AllMetrics.MasterMetricValues.START_TIME.toString()));
    assertTrue(jsonStrs.get(2).contains(AllMetrics.MasterMetricDimensions.MASTER_TASK_TYPE.toString()));
    assertTrue(jsonStrs.get(3).contains(AllMetrics.MasterMetricDimensions.MASTER_TASK_METADATA.toString()));
    assertTrue(jsonStrs.get(4).contains(AllMetrics.MasterMetricDimensions.MASTER_TASK_QUEUE_TIME.toString()));
  }
}
