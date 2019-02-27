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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.env.Environment;

public final class ESResources {
    public static final ESResources INSTANCE = new ESResources();

    private ThreadPool threadPool;
    private CircuitBreakerService circuitBreakerService;
    private ClusterService clusterService;
    private IndicesService indicesService;
    private Settings settings;
    private Environment environment;
    private java.nio.file.Path configPath;
    private String pluginFileLocation;

    private ESResources() {
        threadPool = null;
        circuitBreakerService = null;
        clusterService = null;
        settings = null;
        indicesService = null;
        environment = null;
        configPath = null;
        pluginFileLocation = null;
    }

    public void setPluginFileLocation(String pluginFileLocation) {
        this.pluginFileLocation = pluginFileLocation;
    }

    public String getPluginFileLocation() {
        return pluginFileLocation;
    }

    public void setConfigPath(java.nio.file.Path configPath) {
        this.configPath = configPath;
    }

    public java.nio.file.Path getConfigPath() {
        return configPath;
    }

    public Environment getEnvironment() {
        return environment;
    }

    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    public ThreadPool getThreadPool() {
        return threadPool;
    }

    public void setThreadPool(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    public CircuitBreakerService getCircuitBreakerService() {
        return circuitBreakerService;
    }

    public void setCircuitBreakerService(CircuitBreakerService circuitBreakerService) {
        this.circuitBreakerService = circuitBreakerService;
    }

    public ClusterService getClusterService() {
        return clusterService;
    }

    public void setClusterService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public Settings getSettings() {
        return settings;
    }

    public void setSettings(Settings settings) {
        this.settings = settings;
    }

    public IndicesService getIndicesService() {
        return indicesService;
    }

    public void setIndicesService(IndicesService indicesService) {
        this.indicesService = indicesService;
    }
}
