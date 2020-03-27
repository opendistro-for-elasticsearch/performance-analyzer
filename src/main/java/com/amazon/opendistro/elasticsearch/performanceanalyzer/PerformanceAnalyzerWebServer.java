/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.security.Security;
import java.security.cert.X509Certificate;
import java.util.concurrent.Executors;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

public class PerformanceAnalyzerWebServer {

  private static final Logger LOG = LogManager.getLogger(PerformanceAnalyzerWebServer.class);
  private static final int WEBSERVICE_DEFAULT_PORT = 9600;
  private static final String WEBSERVICE_PORT_CONF_NAME = "webservice-listener-port";
  private static final String WEBSERVICE_BIND_HOST_NAME = "webservice-bind-host";
  // Use system default for max backlog.
  private static final int INCOMING_QUEUE_LENGTH = 1;

  public static HttpServer createInternalServer(PluginSettings settings) {
    int internalPort = getPortNumber();
    try {
      Security.addProvider(new BouncyCastleProvider());
      HttpServer server;
      if (settings.getHttpsEnabled()) {
        server = createHttpsServer(internalPort);
      } else {
        server = createHttpServer(internalPort);
      }
      server.setExecutor(Executors.newCachedThreadPool());
      return server;
    } catch (java.net.BindException ex) {
      Runtime.getRuntime().halt(1);
    } catch (Exception ex) {
      ex.printStackTrace();
      Runtime.getRuntime().halt(1);
    }

    return null;
  }

  private static HttpServer createHttpsServer(int readerPort) throws Exception {
    HttpsServer server = null;
    String bindHost = getBindHost();
    if (bindHost != null && !bindHost.trim().isEmpty()) {
      LOG.info("Binding to Interface: {}", bindHost);
      server =
          HttpsServer.create(
              new InetSocketAddress(InetAddress.getByName(bindHost.trim()), readerPort),
              INCOMING_QUEUE_LENGTH);
    } else {
      LOG.info(
          "Value Not Configured for: {} Using default value: binding to all interfaces",
          WEBSERVICE_BIND_HOST_NAME);
      server = HttpsServer.create(new InetSocketAddress(readerPort), INCOMING_QUEUE_LENGTH);
    }

    TrustManager[] trustAllCerts =
        new TrustManager[] {
            new X509TrustManager() {

              public X509Certificate[] getAcceptedIssuers() {
                return null;
              }

              public void checkClientTrusted(X509Certificate[] certs, String authType) {}

              public void checkServerTrusted(X509Certificate[] certs, String authType) {}
            }
        };

    HostnameVerifier allHostsValid =
        new HostnameVerifier() {
          public boolean verify(String hostname, SSLSession session) {
            return true;
          }
        };

    // Install the all-trusting trust manager
    SSLContext sslContext = SSLContext.getInstance("TLSv1.2");

    KeyStore ks = CertificateUtils.createKeyStore();
    KeyManagerFactory kmf = KeyManagerFactory.getInstance("NewSunX509");
    kmf.init(ks, CertificateUtils.IN_MEMORY_PWD.toCharArray());
    sslContext.init(kmf.getKeyManagers(), trustAllCerts, null);

    HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
    HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
    server.setHttpsConfigurator(new HttpsConfigurator(sslContext));
    return server;
  }

  private static HttpServer createHttpServer(int readerPort) throws Exception {
    HttpServer server = null;
    String bindHost = getBindHost();
    if (bindHost != null && !bindHost.trim().isEmpty()) {
      LOG.info("Binding to Interface: {}", bindHost);
      server =
          HttpServer.create(
              new InetSocketAddress(InetAddress.getByName(bindHost.trim()), readerPort),
              INCOMING_QUEUE_LENGTH);
    } else {
      LOG.info(
          "Value Not Configured for: {} Using default value: binding to all interfaces",
          WEBSERVICE_BIND_HOST_NAME);
      server = HttpServer.create(new InetSocketAddress(readerPort), INCOMING_QUEUE_LENGTH);
    }

    return server;
  }

  private static int getPortNumber() {
    String readerPortValue;
    try {
      readerPortValue = PluginSettings.instance().getSettingValue(WEBSERVICE_PORT_CONF_NAME);

      if (readerPortValue == null) {
        LOG.info(
            "{} not configured; using default value: {}",
            WEBSERVICE_PORT_CONF_NAME,
            WEBSERVICE_DEFAULT_PORT);
        return WEBSERVICE_DEFAULT_PORT;
      }

      return Integer.parseInt(readerPortValue);
    } catch (Exception ex) {
      LOG.error(
          "Invalid Configuration: {} Using default value: {} AND Error: {}",
          WEBSERVICE_PORT_CONF_NAME,
          WEBSERVICE_DEFAULT_PORT,
          ex.toString());
      return WEBSERVICE_DEFAULT_PORT;
    }
  }

  private static String getBindHost() {
    return PluginSettings.instance().getSettingValue(WEBSERVICE_BIND_HOST_NAME);
  }
}
