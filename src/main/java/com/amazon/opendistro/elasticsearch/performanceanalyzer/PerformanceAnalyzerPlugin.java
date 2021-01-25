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

package com.amazon.opendistro.elasticsearch.performanceanalyzer;

import static java.util.Collections.singletonList;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.action.PerformanceAnalyzerActionFilter;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.CacheConfigMetricsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.CircuitBreakerCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.DisksCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.FaultDetectionMetricsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.GCInfoCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.HeapMetricsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.MasterServiceEventMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.MasterServiceMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.MasterThrottlingMetricsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.MetricsPurgeActivity;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.NetworkInterfaceCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.NodeDetailsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.NodeStatsAllShardsMetricsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.NodeStatsFixedShardsMetricsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.OSMetricsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.ScheduledMetricCollectorsExecutor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.ShardStateCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.ThreadPoolMetricsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PerformanceAnalyzerController;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides.ConfigOverridesWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.ClusterSettingsManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.PerformanceAnalyzerClusterSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.handler.ConfigOverridesClusterSettingHandler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.handler.NodeStatsSettingHandler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.setting.handler.PerformanceAnalyzerClusterSettingHandler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.http_action.config.PerformanceAnalyzerClusterConfigAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.http_action.config.PerformanceAnalyzerConfigAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.http_action.config.PerformanceAnalyzerOverridesClusterConfigAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.http_action.config.PerformanceAnalyzerResourceProvider;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.http_action.whoami.TransportWhoAmIAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.http_action.whoami.WhoAmIAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.listener.PerformanceAnalyzerSearchListener;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.EventLog;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.EventLogFileHandler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.transport.PerformanceAnalyzerTransportInterceptor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.Utils;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.writer.EventLogQueueProcessor;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.watcher.ResourceWatcherService;

public final class PerformanceAnalyzerPlugin extends Plugin implements ActionPlugin, NetworkPlugin, SearchPlugin {
    private static final Logger LOG = LogManager.getLogger(PerformanceAnalyzerPlugin.class);
    public static final String PLUGIN_NAME = "opendistro-performance-analyzer";
    private static final String ADD_FAULT_DETECTION_METHOD = "addFaultDetectionListener";
    private static final String LISTENER_INJECTOR_CLASS_PATH =
            "com.amazon.opendistro.elasticsearch.performanceanalyzer.listener.ListenerInjector";
    public static final int QUEUE_PURGE_INTERVAL_MS = 1000;
    private static SecurityManager sm = null;
    private final PerformanceAnalyzerClusterSettingHandler perfAnalyzerClusterSettingHandler;
    private final NodeStatsSettingHandler nodeStatsSettingHandler;
    private final ConfigOverridesClusterSettingHandler configOverridesClusterSettingHandler;
    private final ConfigOverridesWrapper configOverridesWrapper;
    private final PerformanceAnalyzerController performanceAnalyzerController;
    private final ClusterSettingsManager clusterSettingsManager;

    static {
        SecurityManager sm = System.getSecurityManager();
        Utils.configureMetrics();
        if(sm != null) {
            // unprivileged code such as scripts do not have SpecialPermission
            sm.checkPermission(new SpecialPermission());
        }
    }

    public static void invokePrivileged(Runnable runner) {
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            try {
                runner.run();
            } catch(Exception ex) {
                LOG.debug((Supplier<?>) () -> new ParameterizedMessage("Privileged Invocation failed {}",
                        ex.toString()), ex);
            }
            return null;
        } );
    }

    private final ScheduledMetricCollectorsExecutor scheduledMetricCollectorsExecutor;

    public PerformanceAnalyzerPlugin(final Settings settings, final java.nio.file.Path configPath) {
        OSMetricsGeneratorFactory.getInstance();

        ESResources.INSTANCE.setSettings(settings);
        ESResources.INSTANCE.setConfigPath(configPath);
        ESResources.INSTANCE.setPluginFileLocation(new Environment(settings, configPath).
                pluginsFile().toAbsolutePath().toString() + File.separator + PLUGIN_NAME + File.separator);
        //initialize plugin settings. Accessing plugin settings before this
        //point will break, as the plugin location will not be initialized.
        PluginSettings.instance();
        scheduledMetricCollectorsExecutor = new ScheduledMetricCollectorsExecutor();
        this.performanceAnalyzerController = new PerformanceAnalyzerController(scheduledMetricCollectorsExecutor);

        configOverridesWrapper = new ConfigOverridesWrapper();
        clusterSettingsManager = new ClusterSettingsManager(Arrays.asList(PerformanceAnalyzerClusterSettings.COMPOSITE_PA_SETTING,
                        PerformanceAnalyzerClusterSettings.PA_NODE_STATS_SETTING),
                Collections.singletonList(PerformanceAnalyzerClusterSettings.CONFIG_OVERRIDES_SETTING));
        configOverridesClusterSettingHandler = new ConfigOverridesClusterSettingHandler(configOverridesWrapper, clusterSettingsManager,
                PerformanceAnalyzerClusterSettings.CONFIG_OVERRIDES_SETTING);
        clusterSettingsManager.addSubscriberForStringSetting(PerformanceAnalyzerClusterSettings.CONFIG_OVERRIDES_SETTING,
                configOverridesClusterSettingHandler);
        perfAnalyzerClusterSettingHandler = new PerformanceAnalyzerClusterSettingHandler(performanceAnalyzerController,
                clusterSettingsManager);
        clusterSettingsManager.addSubscriberForIntSetting(PerformanceAnalyzerClusterSettings.COMPOSITE_PA_SETTING,
                perfAnalyzerClusterSettingHandler);

        nodeStatsSettingHandler = new NodeStatsSettingHandler(performanceAnalyzerController,
                clusterSettingsManager);
        clusterSettingsManager.addSubscriberForIntSetting(PerformanceAnalyzerClusterSettings.PA_NODE_STATS_SETTING,
                nodeStatsSettingHandler);

        scheduledMetricCollectorsExecutor.addScheduledMetricCollector(new ThreadPoolMetricsCollector());
        scheduledMetricCollectorsExecutor.addScheduledMetricCollector(new CacheConfigMetricsCollector());
        scheduledMetricCollectorsExecutor.addScheduledMetricCollector(new CircuitBreakerCollector());
        scheduledMetricCollectorsExecutor.addScheduledMetricCollector(new OSMetricsCollector());
        scheduledMetricCollectorsExecutor.addScheduledMetricCollector(new HeapMetricsCollector());

        scheduledMetricCollectorsExecutor.addScheduledMetricCollector(new MetricsPurgeActivity());

        scheduledMetricCollectorsExecutor.addScheduledMetricCollector(new NodeDetailsCollector(configOverridesWrapper));
        scheduledMetricCollectorsExecutor.addScheduledMetricCollector(new
                NodeStatsAllShardsMetricsCollector(performanceAnalyzerController));
        scheduledMetricCollectorsExecutor.addScheduledMetricCollector(new
                NodeStatsFixedShardsMetricsCollector(performanceAnalyzerController));
        scheduledMetricCollectorsExecutor.addScheduledMetricCollector(new MasterServiceMetrics());
        scheduledMetricCollectorsExecutor.addScheduledMetricCollector(new MasterServiceEventMetrics());
        scheduledMetricCollectorsExecutor.addScheduledMetricCollector(new DisksCollector());
        scheduledMetricCollectorsExecutor.addScheduledMetricCollector(new NetworkInterfaceCollector());
        scheduledMetricCollectorsExecutor.addScheduledMetricCollector(new GCInfoCollector());
        scheduledMetricCollectorsExecutor.addScheduledMetricCollector(StatsCollector.instance());
        scheduledMetricCollectorsExecutor.addScheduledMetricCollector(new FaultDetectionMetricsCollector(
                performanceAnalyzerController, configOverridesWrapper));
        scheduledMetricCollectorsExecutor.addScheduledMetricCollector(new ShardStateCollector(
                performanceAnalyzerController,configOverridesWrapper));
        scheduledMetricCollectorsExecutor.addScheduledMetricCollector(new MasterThrottlingMetricsCollector(
                performanceAnalyzerController,configOverridesWrapper));
        scheduledMetricCollectorsExecutor.start();

        EventLog eventLog = new EventLog();
        EventLogFileHandler eventLogFileHandler = new EventLogFileHandler(eventLog, PluginSettings.instance().getMetricsLocation());
        new EventLogQueueProcessor(eventLogFileHandler,
                MetricsConfiguration.SAMPLING_INTERVAL,
                QUEUE_PURGE_INTERVAL_MS, performanceAnalyzerController).scheduleExecutor();
    }

    // - http level: bulk, search
    @Override
    public List<ActionFilter> getActionFilters() {
        return singletonList(new PerformanceAnalyzerActionFilter(performanceAnalyzerController));
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> actions = new ArrayList<>(1);
        actions.add(new ActionHandler<>(WhoAmIAction.INSTANCE,
                TransportWhoAmIAction.class));
        return actions;
    }

    //- shardquery, shardfetch
    @Override
    public void onIndexModule(IndexModule indexModule) {
        PerformanceAnalyzerSearchListener performanceanalyzerSearchListener =
            new PerformanceAnalyzerSearchListener(performanceAnalyzerController);
        indexModule.addSearchOperationListener(performanceanalyzerSearchListener);
    }

    //follower check, leader check
    public void onDiscovery(Discovery discovery) {
        try {
            Class<?> listenerInjector = Class.forName(LISTENER_INJECTOR_CLASS_PATH);
            Object listenerInjectorInstance = listenerInjector.getDeclaredConstructor().newInstance();
            Method addListenerMethod = listenerInjectorInstance.getClass().getMethod(ADD_FAULT_DETECTION_METHOD,
                Discovery.class);
            addListenerMethod.invoke(listenerInjectorInstance, discovery);
        } catch (InstantiationException | InvocationTargetException | NoSuchMethodException  |
            IllegalAccessException e) {
            LOG.debug("Exception while calling addFaultDetectionListener in Discovery");
        } catch (ClassNotFoundException e) {
            LOG.debug("No Class for ListenerInjector detected");
        }
    }

    //- shardbulk
    @Override
    public List<TransportInterceptor> getTransportInterceptors(NamedWriteableRegistry namedWriteableRegistry, ThreadContext threadContext) {
        return singletonList(new PerformanceAnalyzerTransportInterceptor(performanceAnalyzerController));
    }

    @Override
    public List<org.elasticsearch.rest.RestHandler> getRestHandlers(final Settings settings,
                                                                    final RestController restController,
                                                                    final ClusterSettings clusterSettings,
                                                                    final IndexScopedSettings indexScopedSettings,
                                                                    final SettingsFilter settingsFilter,
                                                                    final IndexNameExpressionResolver indexNameExpressionResolver,
                                                                    final Supplier<DiscoveryNodes> nodesInCluster) {
        PerformanceAnalyzerConfigAction performanceanalyzerConfigAction = new PerformanceAnalyzerConfigAction(
                restController, performanceAnalyzerController);
        PerformanceAnalyzerConfigAction.setInstance(performanceanalyzerConfigAction);
        PerformanceAnalyzerResourceProvider performanceAnalyzerRp = new PerformanceAnalyzerResourceProvider(settings, restController);
        PerformanceAnalyzerClusterConfigAction paClusterConfigAction = new PerformanceAnalyzerClusterConfigAction(settings,
                restController, perfAnalyzerClusterSettingHandler, nodeStatsSettingHandler);
        PerformanceAnalyzerOverridesClusterConfigAction paOverridesConfigClusterAction =
                new PerformanceAnalyzerOverridesClusterConfigAction(settings, restController,
                        configOverridesClusterSettingHandler, configOverridesWrapper);
        return Arrays.asList(performanceanalyzerConfigAction, paClusterConfigAction, performanceAnalyzerRp, paOverridesConfigClusterAction);
    }

    @Override
    @SuppressWarnings("checkstyle:parameternumber")
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry, Environment environment,
                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry,
                                               IndexNameExpressionResolver indexNameExpressionResolver,
                                               Supplier<RepositoriesService> repositoriesServiceSupplier) {
        ESResources.INSTANCE.setClusterService(clusterService);
        ESResources.INSTANCE.setThreadPool(threadPool);
        ESResources.INSTANCE.setEnvironment(environment);
        ESResources.INSTANCE.setClient(client);

        // ClusterSettingsManager needs ClusterService to have been created before we can
        // initialize it. This is the earliest point at which we know ClusterService is created.
        // So, call the initialize method here.
        clusterSettingsManager.initialize();
        return Collections.singletonList(performanceAnalyzerController);
    }

    @Override
    public Map<String, Supplier<Transport>> getTransports(Settings settings, ThreadPool threadPool,
                                                          PageCacheRecycler pageCacheRecycler,
                                                          CircuitBreakerService circuitBreakerService,
                                                          NamedWriteableRegistry namedWriteableRegistry,
                                                          NetworkService networkService) {
        ESResources.INSTANCE.setSettings(settings);
        ESResources.INSTANCE.setCircuitBreakerService(circuitBreakerService);
        return Collections.emptyMap();
    }

    /**
     * Returns a list of additional {@link Setting} definitions for this plugin.
     */
    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(PerformanceAnalyzerClusterSettings.COMPOSITE_PA_SETTING,
                             PerformanceAnalyzerClusterSettings.PA_NODE_STATS_SETTING,
                             PerformanceAnalyzerClusterSettings.CONFIG_OVERRIDES_SETTING);
    }

}
