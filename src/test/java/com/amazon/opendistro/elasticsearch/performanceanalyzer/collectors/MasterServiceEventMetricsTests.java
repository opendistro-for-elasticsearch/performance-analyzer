package com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors;

import static org.mockito.MockitoAnnotations.initMocks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ESResources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
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

@Ignore
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class MasterServiceEventMetricsTests extends ESTestCase {
  private long startTimeInMills = 1153721339;
  private MasterServiceEventMetrics masterServiceEventMetrics;
  private ThreadPool threadPool;

//  @Mock
//  private ThreadIDUtil mockThreadIDUtil;

  @Before
  public void init() {
    initMocks(this);
//    setMock(mockThreadIDUtil);
//    when(mockThreadIDUtil.getNativeThreadId(anyLong())).thenReturn(24L);
    threadPool = new TestThreadPool("test");
    ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
    ESResources.INSTANCE.setClusterService(clusterService);

    MetricsConfiguration.CONFIG_MAP.put(MasterServiceEventMetrics.class, MetricsConfiguration.cdefault);
    masterServiceEventMetrics = new MasterServiceEventMetrics();
  }

//  private void setMock(ThreadIDUtil mockThreadIDUtil) {
//    try {
//      Field instance = ThreadIDUtil.class.getDeclaredField("INSTANCE");
//      instance.setAccessible(true);
//      instance.set(instance, mockThreadIDUtil);
//    } catch (Exception e) {
//      throw new RuntimeException(e);
//    }
//  }
//
//  private void resetSingleton() throws Exception {
//    Field instance = ThreadIDUtil.class.getDeclaredField("INSTANCE");
//    instance.setAccessible(true);
//    instance.set(null, null);
//  }

  @After
  public void tearDown() throws Exception {
//    resetSingleton();
    threadPool.shutdownNow();
    super.tearDown();
  }

  @Test
  public void testCollectMetrics() throws Exception {
    PrioritizedEsThreadPoolExecutor prioritizedEsThreadPoolExecutor = (PrioritizedEsThreadPoolExecutor) masterServiceEventMetrics
        .getMasterServiceTPExecutorField().get(ESResources.INSTANCE.getClusterService().getMasterService());
    SourcePrioritizedRunnable runnable = new SourcePrioritizedRunnable(Priority.HIGH, "_add_listener_") {
      @Override
      public void run() {
        System.out.println("dummy runnable");
      }
    };
    prioritizedEsThreadPoolExecutor.submit(runnable);
    masterServiceEventMetrics.collectMetrics(startTimeInMills);
  }
}
