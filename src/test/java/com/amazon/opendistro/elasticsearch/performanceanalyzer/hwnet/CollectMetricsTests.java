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


package com.amazon.opendistro.elasticsearch.performanceanalyzer.hwnet;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AbstractTests;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.ESResources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.OSMetricsGeneratorFactory;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.CircuitBreakerCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.NetInterfaceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.NetworkE2ECollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.NetworkInterfaceCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.NodeDetailsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides.ConfigOverridesWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CircuitBreakerDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CircuitBreakerValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.IPDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.IPValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.NodeDetailColumns;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.TCPDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.TCPValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics_generator.IPMetricsGenerator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics_generator.TCPMetricsGenerator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.os.OSGlobals;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.JsonConverter;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

//import org.junit.Test;

@PowerMockIgnore({ "org.apache.logging.log4j.*" })
@RunWith(PowerMockRunner.class)
@PrepareForTest({ ESResources.class, NetworkE2E.class,
        NetworkInterface.class, PluginSettings.class,
        OSGlobals.class })
@SuppressStaticInitializationFor({ "PluginSettings",
        "OSGlobals" })
public class CollectMetricsTests extends AbstractTests {
    @Mock
    CircuitBreakerService circuitBreakerService;

    @Mock
    AllCircuitBreakerStats circuitBreakerStats;

    @Mock
    ClusterService clusterService;

    @Mock
    ClusterState clusterState;

    private long parseLong(Object obj) {
        return Long.parseLong(obj.toString());
    }

    private double parseDouble(Object obj) {
        return Double.parseDouble(obj.toString());
    }

    private int parseInt(Object obj) {
        return Integer.parseInt(obj.toString());
    }

    private void mockWriteValue(File file, MetricsProcessor collector) {
        Mockito.doAnswer((Answer<Void>) invocation -> {
            String contents = (String)invocation.getArguments()[0];
            try (PrintStream out = new PrintStream(
                    new FileOutputStream(file))) {
                out.print(contents);
            }
            return null;

        }).when(collector).saveMetricValues(Mockito.any(String.class),
                Mockito.anyLong());
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        System.setProperty("performanceanalyzer.metrics.log.enabled", "False");

        PluginSettings config = Mockito.mock(PluginSettings.class);
        Mockito.when(config.getMetricsDeletionInterval()).thenReturn(0);

        PowerMockito.mockStatic(PluginSettings.class);
        PowerMockito.when(PluginSettings.instance()).thenReturn(config);

        PowerMockito.mockStatic(OSGlobals.class);
        PowerMockito.when(OSGlobals.getPid()).thenReturn("1");
    }

    //@Test
    public void testCircuitBreakerMetric() throws Exception {

        CircuitBreakerStats[] stats = new CircuitBreakerStats[2];

        String requestType = "request";
        String fieldDataType = "fielddata";

        long limit1 = 19607637196L;
        long estimated1 = 0;
        long tripped1 = 2;
        stats[0] = new CircuitBreakerStats(requestType, limit1, estimated1, 1,
                tripped1);
        long limit2 = 19607637196L;
        long estimated2 = 3;
        long tripped2 = 5;
        stats[1] = new CircuitBreakerStats(fieldDataType, limit2, estimated2, 4,
                tripped2);

        when(circuitBreakerStats.getAllStats()).thenReturn(stats);

        when(circuitBreakerService.stats()).thenReturn(circuitBreakerStats);

        ESResources resource = PowerMockito.mock(ESResources.class);
        when(resource.getCircuitBreakerService()).thenReturn(circuitBreakerService);

        Field f = ESResources.class.getDeclaredField("INSTANCE");
        f.setAccessible(true);
        f.set(ESResources.class, resource);

        long timeBeforeCollectorWriting = System.currentTimeMillis();

        CircuitBreakerCollector collector = new CircuitBreakerCollector();
        CircuitBreakerCollector spyCollector = Mockito.spy(collector);
        String metricFilePath = rootLocation + File.separator
                + PerformanceAnalyzerMetrics.sCircuitBreakerPath;
        File metricFile = new File(metricFilePath);

        mockWriteValue(metricFile, spyCollector);

        spyCollector.collectMetrics(0);

        try (BufferedReader bufferedReader = new BufferedReader(
                new FileReader(metricFile))) {
            String line = bufferedReader.readLine();
            long lastModifiedTime = JsonConverter.getLongValue(line,
                    PerformanceAnalyzerMetrics.METRIC_CURRENT_TIME);
            // metric writing time should be larger than we created the
            // collector object
            assertTrue(lastModifiedTime > timeBeforeCollectorWriting);

            // request circuit breaker
            line = bufferedReader.readLine();
            Map<String, Object> map = JsonConverter.createMapFrom(line);

            String type = CircuitBreakerDimension.CB_TYPE.toString();
            String limitConfigured = CircuitBreakerValue.CB_CONFIGURED_SIZE.toString();
            String estimated = CircuitBreakerValue.CB_ESTIMATED_SIZE.toString();
            String tripped = CircuitBreakerValue.CB_TRIPPED_EVENTS.toString();
            assertEquals(requestType, map.get(type));
            assertTrue(limit1 == parseLong(map.get(limitConfigured)));
            assertTrue(estimated1 == parseLong(map.get(estimated)));
            assertTrue(tripped1 == parseLong(map.get(tripped)));

            // fielddata circuit breaker
            line = bufferedReader.readLine();
            map = JsonConverter.createMapFrom(line);
            assertEquals(fieldDataType, map.get(type));
            assertTrue(limit2 == parseLong(map.get(limitConfigured)));
            assertTrue(estimated2 == parseLong(map.get(estimated)));
            assertTrue(tripped2 == parseLong(map.get(tripped)));
        }
    }

    //@Test
    public void testTCP() throws Exception {

        Map<String, NetworkE2E.destTCPFlowMetrics> destnodeFlowMetricsMap = new HashMap<>();

        NetworkE2E.TCPFlowMetrics tcpFlow1 = new NetworkE2E.TCPFlowMetrics();
        tcpFlow1.destIP = "0000000000000000FFFF0000E03DD40A";
        tcpFlow1.txQueue = 0;
        tcpFlow1.rxQueue = 1;
        tcpFlow1.currentLost = 2;
        tcpFlow1.sendCWND = 7;
        tcpFlow1.SSThresh = 3;
        NetworkE2E.destTCPFlowMetrics destTcp1 = new NetworkE2E.destTCPFlowMetrics(tcpFlow1);
        destTcp1.numFlows = 24;

        NetworkE2E.TCPFlowMetrics tcpFlow2 = new NetworkE2E.TCPFlowMetrics();
        tcpFlow2.destIP = "0000000000000000FFFF00006733D40A";
        tcpFlow2.txQueue = 4;
        tcpFlow2.rxQueue = 5;
        tcpFlow2.currentLost = 8;
        tcpFlow2.sendCWND = 6;
        tcpFlow2.SSThresh = 9;
        NetworkE2E.destTCPFlowMetrics destTcp2 = new NetworkE2E.destTCPFlowMetrics(tcpFlow2);
        destTcp2.numFlows = 23;
        destnodeFlowMetricsMap.put(tcpFlow1.destIP, destTcp1);
        destnodeFlowMetricsMap.put(tcpFlow2.destIP, destTcp2);

        long timeBeforeCollectorWriting = System.currentTimeMillis();

        NetworkE2ECollector collector = new NetworkE2ECollector();
        NetworkE2ECollector spyCollector = Mockito.spy(collector);
        String metricFilePath = rootLocation + File.separator
                + PerformanceAnalyzerMetrics.sTCPPath;
        File metricFile = new File(metricFilePath);

        mockWriteValue(metricFile, spyCollector);

        NetworkE2E.setDestnodeFlowMetricsMap(destnodeFlowMetricsMap);

        PowerMockito.spy(NetworkE2E.class);
        PowerMockito.doNothing().when(NetworkE2E.class);
        NetworkE2E.addSample();

        TCPMetricsGenerator tcpMetricsGenerator = OSMetricsGeneratorFactory.getInstance().getTCPMetricsGenerator();
        TCPMetricsGenerator spyTCPMetricsGenerator = Mockito.spy(tcpMetricsGenerator);
        Mockito.doNothing().when(spyTCPMetricsGenerator).addSample();

        NetworkE2E.calculateTCPMetrics();
        spyCollector.collectMetrics(0);

        try (BufferedReader bufferedReader = new BufferedReader(
                new FileReader(metricFile))) {
            String line = bufferedReader.readLine();
            long lastModifiedTime = JsonConverter.getLongValue(line,
                    PerformanceAnalyzerMetrics.METRIC_CURRENT_TIME);
            // metric writing time should be larger than we created the
            // collector object
            assertTrue(lastModifiedTime > timeBeforeCollectorWriting);

            // destTcp1
            line = bufferedReader.readLine();

            Map<String, Object> map = JsonConverter.createMapFrom(line);

            String dest = TCPDimension.DEST_ADDR.toString();
            String txQ = TCPValue.Net_TCP_TXQ.toString();
            String numFlows = TCPValue.Net_TCP_NUM_FLOWS.toString();
            String rxQ = TCPValue.Net_TCP_RXQ.toString();
            String curLost = TCPValue.Net_TCP_LOST.toString();
            String sndCWND = TCPValue.Net_TCP_SEND_CWND.toString();
            String SSThresh = TCPValue.Net_TCP_SSTHRESH.toString();
            assertEquals(tcpFlow1.destIP, map.get(dest));
            assertEquals(destTcp1.txQueueTot * 1.0 / destTcp1.numFlows,
                    parseDouble(map.get(txQ)), 0.001);
            assertTrue(destTcp1.numFlows == parseInt(map.get(numFlows)));
            assertEquals(destTcp1.rxQueueTot * 1.0 / destTcp1.numFlows,
                    parseDouble(map.get(rxQ)), 0.001);
            assertEquals(destTcp1.currentLostTot * 1.0 / destTcp1.numFlows,
                    parseDouble(map.get(curLost)), 0.001);
            assertEquals(destTcp1.sendCWNDTot * 1.0 / destTcp1.numFlows,
                    parseDouble(map.get(sndCWND)), 0.001);
            assertEquals(destTcp1.SSThreshTot * 1.0 / destTcp1.numFlows,
                    parseDouble(map.get(SSThresh)), 0.001);

            // destTcp2
            line = bufferedReader.readLine();
            map = JsonConverter.createMapFrom(line);
            assertEquals(tcpFlow2.destIP, map.get(dest));
            assertEquals(destTcp2.txQueueTot * 1.0 / destTcp2.numFlows,
                    parseDouble(map.get(txQ)), 0.001);
            assertTrue(destTcp2.numFlows == parseInt(map.get(numFlows)));
            assertEquals(destTcp2.rxQueueTot * 1.0 / destTcp2.numFlows,
                    parseDouble(map.get(rxQ)), 0.001);
            assertEquals(destTcp2.currentLostTot * 1.0 / destTcp2.numFlows,
                    parseDouble(map.get(curLost)), 0.001);
            assertEquals(destTcp2.sendCWNDTot * 1.0 / destTcp2.numFlows,
                    parseDouble(map.get(sndCWND)), 0.001);
            assertEquals(destTcp2.SSThreshTot * 1.0 / destTcp2.numFlows,
                    parseDouble(map.get(SSThresh)), 0.001);
        }
    }

    //@Test
    public void testIP() throws Exception {

        long timeBeforeCollectorWriting = System.currentTimeMillis();

        NetworkInterface netInterface = new NetworkInterface();
        NetworkInterface spyNetInterface = Mockito.spy(netInterface);

        spyNetInterface.putCurrentIpMetric("InReceives", 1L);
        spyNetInterface.putOldIpMetric("InReceives", 2L);
        spyNetInterface.putCurrentIpMetric("OutRequests", 3L);
        spyNetInterface.putOldIpMetric("OutRequests", 4L);
        spyNetInterface.putCurrentIpMetric("InDelivers", 5L);
        spyNetInterface.putOldIpMetric("InDelivers", 6L);
        spyNetInterface.putCurrentIpMetric("OutDiscards", 7L);
        spyNetInterface.putCurrentIpMetric("OutNoRoutes", 8L);
        spyNetInterface.putOldIpMetric("OutDiscards", 9L);
        spyNetInterface.putOldIpMetric("OutNoRoutes", 10L);
        spyNetInterface.putCurrentMetrics6("Ip6InReceives", 11L);
        spyNetInterface.putCurrentMetrics6("Ip6OutRequests", 12L);
        spyNetInterface.putCurrentMetrics6("Ip6InDelivers", 13L);
        spyNetInterface.putCurrentMetrics6("Ip6OutDiscards", 14L);
        spyNetInterface.putCurrentMetrics6("Ip6OutNoRoutes", 15L);
        spyNetInterface.putOldMetrics6("Ip6InReceives", 16L);
        spyNetInterface.putOldMetrics6("Ip6OutRequests", 17L);
        spyNetInterface.putOldMetrics6("Ip6InDelivers", 18L);
        spyNetInterface.putOldMetrics6("Ip6OutDiscards", 19L);
        spyNetInterface.putOldMetrics6("Ip6OutNoRoutes", 20L);
        spyNetInterface.putCurrentPhyMetric("inbytes", 21L);
        spyNetInterface.putOldPhyMetric("inbytes", 22L);
        spyNetInterface.putCurrentPhyMetric("outbytes", 23L);
        spyNetInterface.putOldPhyMetric("outbytes", 24L);

        NetworkInterface.setKvTimestamp(30L);
        NetworkInterface.setOldkvTimestamp(20L);

        String metricFilePath = rootLocation + File.separator
                + PerformanceAnalyzerMetrics.sIPPath;
        File metricFile = new File(metricFilePath);

        NetworkInterfaceCollector collector2 = new NetworkInterfaceCollector();
        NetworkInterfaceCollector spyCollector2 = Mockito.spy(collector2);

        mockWriteValue(metricFile, spyCollector2);

        PowerMockito.spy(NetworkInterface.class);
        PowerMockito.doNothing().when(NetworkInterface.class);
        NetworkInterface.addSample();

        IPMetricsGenerator ipMetricsGenerator = OSMetricsGeneratorFactory.getInstance().getIPMetricsGenerator();
        IPMetricsGenerator spyIPMetricsGenerator = Mockito.spy(ipMetricsGenerator);
        Mockito.doNothing().when(spyIPMetricsGenerator).addSample();

        NetworkInterface.calculateNetworkMetrics();

        spyCollector2.collectMetrics(0);

        try (BufferedReader bufferedReader = new BufferedReader(
                new FileReader(metricFile))) {
            String line = bufferedReader.readLine();
            long lastModifiedTime = JsonConverter.getLongValue(line,
                    PerformanceAnalyzerMetrics.METRIC_CURRENT_TIME);
            // metric writing time should be larger than we created the
            // collector object
            assertTrue(lastModifiedTime > timeBeforeCollectorWriting);

            String direction = IPDimension.DIRECTION.toString();
            String packetRate4 = IPValue.NET_PACKET_RATE4.toString();
            String dropRate4 = IPValue.NET_PACKET_DROP_RATE4.toString();
            String packetRate6 = IPValue.NET_PACKET_RATE6.toString();
            String dropRate6 = IPValue.NET_PACKET_DROP_RATE6.toString();
            String bps = IPValue.NET_THROUGHPUT.toString();

            Map<String, Long> curphy = spyNetInterface.getCurrentPhyMetric();
            Map<String, Long> curipv4 = spyNetInterface.getCurrentIpMetric();
            Map<String, Long> oldphy = spyNetInterface.getOldPhyMetric();
            Map<String, Long> oldipv4 = spyNetInterface.getOldIpMetric();
            Map<String, Long> currentMetrics6 = spyNetInterface
                    .getCurrentMetrics6();
            Map<String, Long> oldMetrics6 = spyNetInterface.getOldMetrics6();

            long nin = curipv4.get("InReceives") - oldipv4.get("InReceives");
            long nout = curipv4.get("OutRequests") - oldipv4.get("OutRequests");
            long delivin = curipv4.get("InDelivers")
                    - oldipv4.get("InDelivers");
            long dropout = curipv4.get("OutDiscards")
                    + curipv4.get("OutNoRoutes") - oldipv4.get("OutDiscards")
                    - oldipv4.get("OutNoRoutes");
            long nin6 = currentMetrics6.get("Ip6InReceives")
                    - oldMetrics6.get("Ip6InReceives");
            long nout6 = currentMetrics6.get("Ip6OutRequests")
                    - oldMetrics6.get("Ip6OutRequests");
            long delivin6 = currentMetrics6.get("Ip6InDelivers")
                    - oldMetrics6.get("Ip6InDelivers");
            long dropout6 = currentMetrics6.get("Ip6OutDiscards")
                    + currentMetrics6.get("Ip6OutNoRoutes")
                    - oldMetrics6.get("Ip6OutDiscards")
                    - oldMetrics6.get("Ip6OutNoRoutes");

            long timeDelta = NetworkInterface.getKvTimestamp()
                    - NetworkInterface.getOldkvTimestamp();
            double inbps = 8 * 1.0e3
                    * (curphy.get("inbytes") - oldphy.get("inbytes"))
                    / timeDelta;
            double outbps = 8 * 1.0e3
                    * (curphy.get("outbytes") - oldphy.get("outbytes"))
                    / timeDelta;
            double inPacketRate4 = 1.0e3 * (nin) / timeDelta;
            double outPacketRate4 = 1.0e3 * (nout) / timeDelta;
            double inDropRate4 = 1.0e3 * (nin - delivin) / timeDelta;
            double outDropRate4 = 1.0e3 * (dropout) / timeDelta;
            double inPacketRate6 = 1.0e3 * (nin6) / timeDelta;
            double outPacketRate6 = 1.0e3 * (nout6) / timeDelta;
            double inDropRate6 = 1.0e3 * (nin6 - delivin6) / timeDelta;
            double outDropRate6 = 1.0e3 * (dropout6) / timeDelta;

            line = bufferedReader.readLine();
            Map<String, Object> map = JsonConverter.createMapFrom(line);

            assertEquals(NetInterfaceSummary.Direction.in.toString(), map.get(direction));
            assertEquals(inbps, parseDouble(map.get(bps)), 0.001);
            assertEquals(inPacketRate4, parseDouble(map.get(packetRate4)),
                    0.001);
            assertEquals(inDropRate4, parseDouble(map.get(dropRate4)), 0.001);
            assertEquals(inPacketRate6, parseDouble(map.get(packetRate6)),
                    0.001);
            assertEquals(inDropRate6, parseDouble(map.get(dropRate6)), 0.001);

            line = bufferedReader.readLine();

            map = JsonConverter.createMapFrom(line);

            assertEquals(NetInterfaceSummary.Direction.out.toString(), map.get(direction));
            assertEquals(outbps, parseDouble(map.get(bps)), 0.001);
            assertEquals(outPacketRate4, parseDouble(map.get(packetRate4)),
                    0.001);
            assertEquals(outDropRate4, parseDouble(map.get(dropRate4)), 0.001);
            assertEquals(outPacketRate6, parseDouble(map.get(packetRate6)),
                    0.001);
            assertEquals(outDropRate6, parseDouble(map.get(dropRate6)), 0.001);
        }
    }

    private static List<DiscoveryNode> buildNumNodes(String nodeId1, String nodeId2, InetAddress address1, InetAddress address2)
            throws UnknownHostException {
        List<DiscoveryNode> nodesList = new ArrayList<>();

        final DiscoveryNode node1 = new DiscoveryNode("s7gDCVn", nodeId1,
                new TransportAddress(address1, 9200), emptyMap(),
                singleton(DiscoveryNodeRole.MASTER_ROLE), Version.CURRENT);
        nodesList.add(node1);

        final DiscoveryNode node2 = new DiscoveryNode("Zn1QcSU", nodeId2,
                new TransportAddress(address2, 9200), emptyMap(),
                singleton(DiscoveryNodeRole.DATA_ROLE), Version.CURRENT);
        nodesList.add(node2);

        return nodesList;
    }

    //@Test
    public void testNodeDetails() throws Exception {

        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();

        String nodeId1 = "s7gDCVnCSiuBgHoYLji1gw";
        InetAddress address1 = InetAddress.getByName("10.212.49.140");

        String nodeId2 = "Zn1QcSUGT--DciD1Em5wRg";
        InetAddress address2 = InetAddress.getByName("10.212.52.241");

        ConfigOverridesWrapper testOverridesWrapper = new ConfigOverridesWrapper();

        List<DiscoveryNode> nodeList = buildNumNodes(nodeId1, nodeId2, address1, address2);

        for (DiscoveryNode node : nodeList) {
            discoBuilder = discoBuilder.add(node);
        }

        discoBuilder.masterNodeId(nodeId1);
        discoBuilder.localNodeId(nodeId2);

        DiscoveryNodes discoveryNodes = discoBuilder.build();

        when(clusterState.nodes()).thenReturn(discoveryNodes);

        when(clusterService.state()).thenReturn(clusterState);

        ESResources resource = PowerMockito.mock(ESResources.class);
        when(resource.getClusterService()).thenReturn(clusterService);

        Field f = ESResources.class.getDeclaredField("INSTANCE");
        f.setAccessible(true);
        f.set(ESResources.class, resource);

        long timeBeforeCollectorWriting = System.currentTimeMillis();

        NodeDetailsCollector collector = new NodeDetailsCollector(testOverridesWrapper);
        NodeDetailsCollector spyCollector = Mockito.spy(collector);
        String metricFilePath = rootLocation + File.separator
                + PerformanceAnalyzerMetrics.sNodesPath;
        File metricFile = new File(metricFilePath);

        mockWriteValue(metricFile, spyCollector);

        spyCollector.collectMetrics(0);

        try (BufferedReader bufferedReader = new BufferedReader(
                new FileReader(metricFile))) {
            String line = bufferedReader.readLine();
            long lastModifiedTime = JsonConverter.getLongValue(line,
                    PerformanceAnalyzerMetrics.METRIC_CURRENT_TIME);
            // metric writing time should be larger than we created the
            // collector object
            assertTrue(lastModifiedTime > timeBeforeCollectorWriting);

            String ID = NodeDetailColumns.ID.toString();
            String hostAddress = NodeDetailColumns.HOST_ADDRESS.toString();

            // local node first
            line = bufferedReader.readLine();
            Map<String, Object>  map = JsonConverter.createMapFrom(line);

            assertEquals(nodeId2, map.get(ID));
            assertEquals(address2.getHostAddress(), map.get(hostAddress));

            // remote node later
            line = bufferedReader.readLine();
            map = JsonConverter.createMapFrom(line);

            assertEquals(nodeId1, map.get(ID));
            assertEquals(address1.getHostAddress(), map.get(hostAddress));
        }
    }
}
