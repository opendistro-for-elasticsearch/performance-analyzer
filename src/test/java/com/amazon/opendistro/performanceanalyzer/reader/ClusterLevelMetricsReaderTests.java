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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.sql.SQLException;

//import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;

import com.amazon.opendistro.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.performanceanalyzer.reader.ClusterLevelMetricsReader.NodeDetails;

@PowerMockIgnore({ "org.apache.logging.log4j.*" })
@RunWith(PowerMockRunner.class)
@PrepareForTest({ PerformanceAnalyzerMetrics.class, PluginSettings.class })
@SuppressStaticInitializationFor({ "PluginSettings" })
public class ClusterLevelMetricsReaderTests extends AbstractReaderTests {

    public ClusterLevelMetricsReaderTests() throws SQLException, ClassNotFoundException {
        super();
        // TODO Auto-generated constructor stub
    }

    //@Test
    public void testCollectNodeMetrics() throws Exception {
        PluginSettings config = Mockito.mock(PluginSettings.class);
        Mockito.when(config.getMetricsLocation()).thenReturn(rootLocation);

        PowerMockito.mockStatic(PluginSettings.class);
        PowerMockito.when(PluginSettings.instance()).thenReturn(config);

        long currTimestamp = System.currentTimeMillis();
        long currTimeBucket = PerformanceAnalyzerMetrics.getTimeInterval(currTimestamp);
        String currentTimeBucketStr = String.valueOf(currTimeBucket);
        temporaryFolder.newFolder(currentTimeBucketStr);
        File output = temporaryFolder.newFile(createRelativePath(
                currentTimeBucketStr, PerformanceAnalyzerMetrics.sNodesPath));

        String nodeId1 = "s7gDCVnCSiuBgHoYLji1gw";
        String address1 = "10.212.49.140";

        String nodeId2 = "Zn1QcSUGT--DciD1Em5wRg";
        String address2 = "10.212.52.241";

        write(output, false,
                PerformanceAnalyzerMetrics.getJsonCurrentMilliSeconds(),
                createNodeDetailsMetrics(nodeId1, address1),
                createNodeDetailsMetrics(nodeId2, address2)
                );

//        setFinalStatic(PerformanceAnalyzerMetrics.class.getDeclaredField("sDevShmLocation"),
//                rootLocation);

        ClusterLevelMetricsReader.collectNodeMetrics(currTimestamp);

        NodeDetails[] nodes = ClusterLevelMetricsReader.getNodes();

        assertEquals(nodeId1, nodes[0].getId());
        assertEquals(address1, nodes[0].getHostAddress());

        assertEquals(nodeId2, nodes[1].getId());
        assertEquals(address2, nodes[1].getHostAddress());
    }


}
