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

import java.io.File;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.http_action.config.PerformanceAnalyzerConfigAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.action.PerformanceAnalyzerActionFilter;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.CircuitBreakerCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.HeapMetricsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.MasterServiceMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.MasterServiceEventMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.NetworkE2ECollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.NetworkInterfaceCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.NodeDetailsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.http_action.whoami.TransportWhoAmIAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.http_action.whoami.WhoAmIAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.listener.PerformanceAnalyzerSearchListener;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.transport.PerformanceAnalyzerTransportInterceptor;
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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.watcher.ResourceWatcherService;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.DisksCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.MetricsPurgeActivity;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.NodeStatsMetricsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.OSMetricsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.ScheduledMetricCollectorsExecutor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.ThreadPoolMetricsCollector;

public class PerformanceAnalyzerPlugin extends Plugin implements ActionPlugin, NetworkPlugin, SearchPlugin {
    private static final Logger LOG = LogManager.getLogger(PerformanceAnalyzerPlugin.class);
    public static final String PLUGIN_NAME = "opendistro_performance_analyzer";
    private static SecurityManager sm = null;

    static {
        SecurityManager sm = System.getSecurityManager();

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

    public static void invokePrivilegedAndLogError(Runnable runner) {
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            try {
                runner.run();
            } catch(Exception ex) {
                LOG.error((Supplier<?>) () -> new ParameterizedMessage("Privileged Invocation failed {}",
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
        //Initialize plugin settings. Accessing plugin settings before this
        //point will break, as the plugin location will not be initialized.
        PluginSettings.instance();
        scheduledMetricCollectorsExecutor = new ScheduledMetricCollectorsExecutor();
        scheduledMetricCollectorsExecutor.addScheduledMetricCollector(new ThreadPoolMetricsCollector());
        scheduledMetricCollectorsExecutor.addScheduledMetricCollector(new NodeStatsMetricsCollector());
        scheduledMetricCollectorsExecutor.addScheduledMetricCollector(new CircuitBreakerCollector());
        scheduledMetricCollectorsExecutor.addScheduledMetricCollector(new OSMetricsCollector());
        scheduledMetricCollectorsExecutor.addScheduledMetricCollector(new HeapMetricsCollector());
        scheduledMetricCollectorsExecutor.addScheduledMetricCollector(new MetricsPurgeActivity());
        scheduledMetricCollectorsExecutor.addScheduledMetricCollector(new NodeDetailsCollector());
        scheduledMetricCollectorsExecutor.addScheduledMetricCollector(new MasterServiceMetrics());
        scheduledMetricCollectorsExecutor.addScheduledMetricCollector(new MasterServiceEventMetrics());
        scheduledMetricCollectorsExecutor.addScheduledMetricCollector(new DisksCollector());
        scheduledMetricCollectorsExecutor.addScheduledMetricCollector(new NetworkE2ECollector());
        scheduledMetricCollectorsExecutor.addScheduledMetricCollector(new NetworkInterfaceCollector());
        scheduledMetricCollectorsExecutor.addScheduledMetricCollector(StatsCollector.instance());
        scheduledMetricCollectorsExecutor.start();
    }

    // - http level: bulk, search
    @Override
    public List<ActionFilter> getActionFilters() {
        return singletonList(new PerformanceAnalyzerActionFilter());
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
        PerformanceAnalyzerSearchListener performanceanalyzerSearchListener = new PerformanceAnalyzerSearchListener();
        indexModule.addSearchOperationListener(performanceanalyzerSearchListener);
    }

    //- shardbulk
    @Override
    public List<TransportInterceptor> getTransportInterceptors(NamedWriteableRegistry namedWriteableRegistry, ThreadContext threadContext) {
        return singletonList(new PerformanceAnalyzerTransportInterceptor());
    }

    @Override
    public List<org.elasticsearch.rest.RestHandler> getRestHandlers(final Settings settings,
                                                                    final RestController restController,
                                                                    final ClusterSettings clusterSettings,
                                                                    final IndexScopedSettings indexScopedSettings,
                                                                    final SettingsFilter settingsFilter,
                                                                    final IndexNameExpressionResolver indexNameExpressionResolver,
                                                                    final Supplier<DiscoveryNodes> nodesInCluster) {
        PerformanceAnalyzerConfigAction performanceanalyzerConfigAction = new PerformanceAnalyzerConfigAction(restController);
        PerformanceAnalyzerConfigAction.setInstance(performanceanalyzerConfigAction);
        return singletonList(performanceanalyzerConfigAction);
    }

    @Override
    @SuppressWarnings("checkstyle:parameternumber")
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry, Environment environment,
                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry,
                                               IndexNameExpressionResolver indexNameExpressionResolver) {
        ESResources.INSTANCE.setClusterService(clusterService);
        ESResources.INSTANCE.setThreadPool(threadPool);
        ESResources.INSTANCE.setEnvironment(environment);
        return Collections.emptyList();
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


}

