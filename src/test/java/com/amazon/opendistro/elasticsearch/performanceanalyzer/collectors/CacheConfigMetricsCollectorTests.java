package com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ESResources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.CacheConfigMetricsCollector.CacheMaxSizeStatus;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.TestUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.paranamer.ParanamerModule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CacheConfigMetricsCollectorTests extends ESSingleNodeTestCase {
  private static final String TEST_INDEX = "test";
  private CacheConfigMetricsCollector collector;

  @Before
  public void init() {
    IndicesService indicesService = getInstanceFromNode(IndicesService.class);
    ESResources.INSTANCE.setIndicesService(indicesService);

    MetricsConfiguration.CONFIG_MAP.put(CacheConfigMetricsCollector.class, MetricsConfiguration.cdefault);
    collector = new CacheConfigMetricsCollector();
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Test
  public void testCollectMetrics() throws IOException {
    long startTimeInMills = 1153721339;

    createIndex(TEST_INDEX);
    collector.collectMetrics(startTimeInMills);

    List<CacheMaxSizeStatus> metrics = readMetrics();
    assertEquals(2, metrics.size());
    CacheMaxSizeStatus filedDataCache = metrics.get(0);
    CacheMaxSizeStatus shardRequestCache = metrics.get(1);
    assertEquals("field_data_cache", filedDataCache.getCacheType());
    assertEquals("shard_request_cache", shardRequestCache.getCacheType());
  }

  private List<CacheMaxSizeStatus> readMetrics() throws IOException {
    List<Event> metrics = TestUtil.readEvents();
    assert metrics.size() == 1;
    ObjectMapper objectMapper = new ObjectMapper().registerModule(new ParanamerModule());

    List<CacheMaxSizeStatus> list = new ArrayList<>();
    String[] jsonStrs = metrics.get(0).value.split("\n");
    assert jsonStrs.length == 3;
    for (int i = 1; i < 3; i++) {
      list.add(objectMapper.readValue(jsonStrs[i], CacheMaxSizeStatus.class));
    }
    return list;
  }
}
