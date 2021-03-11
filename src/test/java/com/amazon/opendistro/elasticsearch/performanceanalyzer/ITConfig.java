/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

public class ITConfig {
  // Whether or not to use https clients for the integration tests
  private boolean https;
  // The username to use for basic https authentication (if https is specified)
  private String user;
  // The password of the user provided above
  private String password;
  // The Elasticsearch REST endpoint to initialize clients against of the form $host:$port
  private String restEndpoint;
  // The Elasticsearch transport endpoint to initialize clients against of the form $host:$port
  // see https://discuss.elastic.co/t/transport-client-vs-rest-client/13936 for a synopsis of the
  // difference between REST and transport endpoints
  private String transportEndpoint;

  // The port number to use for
  private int paPort = 9600;

  public ITConfig() {
    https = Boolean.parseBoolean(System.getProperty("tests.https"));
    user = System.getProperty("tests.user");
    password = System.getProperty("tests.password");
    restEndpoint = System.getProperty("tests.rest.cluster");
    transportEndpoint = System.getProperty("tests.cluster");
    String paPortTmp = System.getProperty("tests.paPort");
    paPort = paPortTmp == null ? paPort : Integer.parseInt(paPortTmp);
  }

  public boolean isHttps() {
    return https;
  }

  public void setHttps(boolean https) {
    this.https = https;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getRestEndpoint() {
    return restEndpoint;
  }

  public void setRestEndpoint(String restEndpoint) {
    this.restEndpoint = restEndpoint;
  }

  public String getTransportEndpoint() {
    return transportEndpoint;
  }

  public void setTransportEndpoint(String transportEndpoint) {
    this.transportEndpoint = transportEndpoint;
  }

  public int getPaPort() {
    return paPort;
  }

  public void setPaPort(int paPort) {
    this.paPort = paPort;
  }
}
