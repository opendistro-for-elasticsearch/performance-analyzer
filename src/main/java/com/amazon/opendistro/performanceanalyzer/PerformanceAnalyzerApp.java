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

package com.amazon.opendistro.performanceanalyzer;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import com.amazon.opendistro.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.performanceanalyzer.config.TroubleshootingConfig;
import com.amazon.opendistro.performanceanalyzer.reader.ReaderMetricsProcessor;
import com.amazon.opendistro.performanceanalyzer.rest.QueryMetricsRequestHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.sun.net.httpserver.HttpServer;

public class PerformanceAnalyzerApp {
    private static final int WEBSERVICE_DEFAULT_PORT = 9600;
    private static final String WEBSERVICE_PORT_CONF_NAME = "webservice-listener-port";
    //Use system default for max backlog.
    private static final int INCOMING_QUEUE_LENGTH = 1;
    public static final String QUERY_URL = "/_performanceanalyzer/metrics";
    private static final Logger LOG = LogManager.getLogger(PerformanceAnalyzerApp.class);

    public static void main(String[] args) throws Exception {
        ESResources.INSTANCE.setPluginFileLocation(System.getProperty("es.path.home") +
                File.separator + "plugins" + File.separator + PerformanceAnalyzerPlugin.PLUGIN_NAME + File.separator);

        Thread readerThread = new Thread( new Runnable() {
            public void run() {
                while(true) {
                    try {
                        ReaderMetricsProcessor mp = new ReaderMetricsProcessor(PluginSettings.instance().getMetricsLocation());
                        ReaderMetricsProcessor.current = mp;
                        mp.run();
                    } catch (Throwable e) {
                        if (TroubleshootingConfig.EnableDevAssert) {
                            break;
                        }
                        LOG.error("Error in ReaderMetricsProcessor...restarting");
                    }
                }
            }
        });
        readerThread.start();

        int readerPort= getPortNumber();
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(readerPort), INCOMING_QUEUE_LENGTH);
            server.createContext(QUERY_URL, new QueryMetricsRequestHandler());

            server.setExecutor(Executors.newCachedThreadPool());
            server.start();
        } catch(java.net.BindException ex) {
            LOG.error("Port  {} is already in use...exiting", readerPort);
            Runtime.getRuntime().halt(1);
        }
    }

    private static int getPortNumber() {
        String readerPortValue;
        try {
            readerPortValue = PluginSettings.instance().getSettingValue(WEBSERVICE_PORT_CONF_NAME);

            if(readerPortValue == null) {
                LOG.info("{} not configured; using default value: {}", WEBSERVICE_PORT_CONF_NAME, WEBSERVICE_DEFAULT_PORT);
                return WEBSERVICE_DEFAULT_PORT;
            }

            return Integer.valueOf(readerPortValue);
        } catch (Exception ex) {
            LOG.error("Invalid Configured: {} Using default value: {} AND Error: {}",
                    WEBSERVICE_PORT_CONF_NAME, WEBSERVICE_DEFAULT_PORT, ex.toString());
            return WEBSERVICE_DEFAULT_PORT;
        }
    }
}

