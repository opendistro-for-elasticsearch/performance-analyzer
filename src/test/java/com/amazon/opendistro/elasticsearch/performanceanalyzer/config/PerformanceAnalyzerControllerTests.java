package com.amazon.opendistro.elasticsearch.performanceanalyzer.config;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PerformanceAnalyzerController.DEFAULT_NUM_OF_SHARDS_PER_COLLECTION;
import static org.junit.Assert.assertEquals;
import static org.mockito.MockitoAnnotations.initMocks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ESResources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.ScheduledMetricCollectorsExecutor;
import java.nio.file.Paths;
import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;

public class PerformanceAnalyzerControllerTests {
  private static final int NUM_OF_SHARDS_PER_COLLECTION = 1;
  private Settings settings;
  private PerformanceAnalyzerController controller;


  @Before
  public void init() {
    initMocks(this);
    settings = Settings.builder().put("path.home", "./").build();
    ESResources.INSTANCE.setSettings(settings);
    ESResources.INSTANCE.setConfigPath(Paths.get("build/tmp/junit_metrics"));
    controller = new PerformanceAnalyzerController(new ScheduledMetricCollectorsExecutor());

  }

  @Test
  public void testGetNodeStatsShardsPerCollection() {
    assertEquals(DEFAULT_NUM_OF_SHARDS_PER_COLLECTION, controller.getNodeStatsShardsPerCollection());

    controller.updateNodeStatsShardsPerCollection(NUM_OF_SHARDS_PER_COLLECTION);
    assertEquals(NUM_OF_SHARDS_PER_COLLECTION, controller.getNodeStatsShardsPerCollection());


  }
}
