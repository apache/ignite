/*
 * Copyright 2020 Red Hat, Inc.
 *
 * Red Hat licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.vertx.spi.cluster.ignite;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

import static org.apache.ignite.configuration.DataStorageConfiguration.*;
import static org.apache.ignite.configuration.IgniteConfiguration.*;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.*;

/**
 * @author Lukas Prettenthaler
 */
@DataObject(generateConverter = true)
public class IgniteOptions {
  private String localHost;
  private int localPort;
  private int connectionsPerNode;
  private long connectTimeout;
  private long idleConnectionTimeout;
  private long maxConnectTimeout;
  private int reconnectCount;
  private long metricsLogFrequency;
  private IgniteDiscoveryOptions discoveryOptions;
  private List<IgniteCacheOptions> cacheConfiguration;
  private IgniteSslOptions sslOptions;
  private boolean shutdownOnSegmentation;
  private int pageSize;
  private long defaultRegionInitialSize;
  private long defaultRegionMaxSize;
  private boolean defaultRegionMetricsEnabled;
  private boolean shutdownOnNodeStop;
  private long metricsUpdateFrequency;
  private long clientFailureDetectionTimeout;
  private int metricsHistorySize;
  private long metricsExpireTime;
  private IgniteMetricExporterOptions metricExporterOptions;
  private long delayAfterStart;

  /**
   * Default constructor
   */
  public IgniteOptions() {
    localPort = DFLT_PORT;
    connectionsPerNode = DFLT_CONN_PER_NODE;
    connectTimeout = DFLT_CONN_TIMEOUT;
    idleConnectionTimeout = DFLT_IDLE_CONN_TIMEOUT;
    reconnectCount = DFLT_RECONNECT_CNT;
    maxConnectTimeout = DFLT_MAX_CONN_TIMEOUT;
    metricsLogFrequency = DFLT_METRICS_LOG_FREQ;
    discoveryOptions = new IgniteDiscoveryOptions();
    cacheConfiguration = new ArrayList<>();
    shutdownOnSegmentation = true;
    pageSize = DFLT_PAGE_SIZE;
    defaultRegionInitialSize = DFLT_DATA_REGION_INITIAL_SIZE;
    defaultRegionMaxSize = DFLT_DATA_REGION_MAX_SIZE;
    defaultRegionMetricsEnabled = DFLT_METRICS_ENABLED;
    shutdownOnNodeStop = false;
    metricsUpdateFrequency = DFLT_METRICS_UPDATE_FREQ;
    clientFailureDetectionTimeout = DFLT_CLIENT_FAILURE_DETECTION_TIMEOUT;
    metricsHistorySize = DFLT_METRICS_HISTORY_SIZE;
    metricsExpireTime = DFLT_METRICS_EXPIRE_TIME;
    metricExporterOptions = new IgniteMetricExporterOptions();
    delayAfterStart = 100L;
  }

  /**
   * Copy constructor
   *
   * @param options the one to copy
   */
  public IgniteOptions(IgniteOptions options) {
    this.localHost = options.localHost;
    this.localPort = options.localPort;
    this.connectionsPerNode = options.connectionsPerNode;
    this.connectTimeout = options.connectTimeout;
    this.idleConnectionTimeout = options.idleConnectionTimeout;
    this.reconnectCount = options.reconnectCount;
    this.maxConnectTimeout = options.maxConnectTimeout;
    this.metricsLogFrequency = options.metricsLogFrequency;
    this.discoveryOptions = options.discoveryOptions;
    this.cacheConfiguration = options.cacheConfiguration;
    this.sslOptions = options.sslOptions;
    this.shutdownOnSegmentation = options.shutdownOnSegmentation;
    this.pageSize = options.pageSize;
    this.defaultRegionInitialSize = options.defaultRegionInitialSize;
    this.defaultRegionMaxSize = options.defaultRegionMaxSize;
    this.defaultRegionMetricsEnabled = options.defaultRegionMetricsEnabled;
    this.shutdownOnNodeStop = options.shutdownOnNodeStop;
    this.metricsUpdateFrequency = options.metricsUpdateFrequency;
    this.clientFailureDetectionTimeout = options.clientFailureDetectionTimeout;
    this.metricsHistorySize = options.metricsHistorySize;
    this.metricsExpireTime = options.metricsExpireTime;
    this.metricExporterOptions = options.metricExporterOptions;
    this.delayAfterStart = options.delayAfterStart;
  }

  /**
   * Constructor from JSON
   *
   * @param options the JSON
   */
  public IgniteOptions(JsonObject options) {
    this();
    IgniteOptionsConverter.fromJson(options, this);
  }

  /**
   * Gets system-wide local address or host for all Ignite components to bind to. If provided it will
   * override all default local bind settings within Ignite or any of its SPIs.
   *
   * @return Local address or host to bind to.
   */
  public String getLocalHost() {
    return localHost;
  }

  /**
   * Sets system-wide local address or host for all Ignite components to bind to. If provided it will
   * override all default local bind settings within Ignite or any of its SPIs.
   *
   * @param localHost Local IP address or host to bind to.
   * @return reference to this, for fluency
   */
  public IgniteOptions setLocalHost(String localHost) {
    this.localHost = localHost;
    return this;
  }

  /**
   * See {@link #setLocalPort(int)}.
   *
   * @return Port number.
   */
  public int getLocalPort() {
    return localPort;
  }

  /**
   * Sets local port for socket binding.
   *
   * @param localPort Port number.
   * @return reference to this, for fluency
   */
  public IgniteOptions setLocalPort(int localPort) {
    this.localPort = localPort;
    return this;
  }

  /**
   * See {@link #setConnectionsPerNode(int)}.
   *
   * @return Number of connections per node.
   */
  public int getConnectionsPerNode() {
    return connectionsPerNode;
  }

  /**
   * Sets number of connections to each remote node.
   *
   * @param connectionsPerNode Number of connections per node.
   * @return reference to this, for fluency
   */
  public IgniteOptions setConnectionsPerNode(int connectionsPerNode) {
    this.connectionsPerNode = connectionsPerNode;
    return this;
  }

  /**
   * See {@link #setConnectTimeout(long)}.
   *
   * @return Connect timeout.
   */
  public long getConnectTimeout() {
    return connectTimeout;
  }

  /**
   * Sets connect timeout used when establishing connection
   * with remote nodes.
   *
   * @param connectTimeout Connect timeout.
   * @return reference to this, for fluency
   */
  public IgniteOptions setConnectTimeout(long connectTimeout) {
    this.connectTimeout = connectTimeout;
    return this;
  }

  /**
   * See {@link #setIdleConnectionTimeout(long)}.
   *
   * @return Maximum idle connection time.
   */
  public long getIdleConnectionTimeout() {
    return idleConnectionTimeout;
  }

  /**
   * Sets maximum idle connection timeout upon which a connection
   * to client will be closed.
   *
   * @param idleConnectionTimeout Maximum idle connection time.
   * @return reference to this, for fluency
   */
  public IgniteOptions setIdleConnectionTimeout(long idleConnectionTimeout) {
    this.idleConnectionTimeout = idleConnectionTimeout;
    return this;
  }

  /**
   * See {@link #setConnectTimeout(long)}.
   *
   * @return Connect timeout.
   */
  public long getMaxConnectTimeout() {
    return maxConnectTimeout;
  }

  /**
   * Sets maximum connect timeout. If handshake is not established within connect timeout,
   * then SPI tries to repeat handshake procedure with increased connect timeout.
   * Connect timeout can grow till maximum timeout value,
   * if maximum timeout value is reached then the handshake is considered as failed.
   *
   * @param maxConnectTimeout Maximum connect timeout.
   * @return reference to this, for fluency
   */
  public IgniteOptions setMaxConnectTimeout(long maxConnectTimeout) {
    this.maxConnectTimeout = maxConnectTimeout;
    return this;
  }

  /**
   * Gets maximum number of reconnect attempts used when establishing connection
   * with remote nodes.
   *
   * @return Reconnects count.
   */
  public int getReconnectCount() {
    return reconnectCount;
  }

  /**
   * Sets maximum number of reconnect attempts used when establishing connection
   * with remote nodes.
   *
   * @param reconnectCount Maximum number of reconnection attempts.
   * @return reference to this, for fluency
   */
  public IgniteOptions setReconnectCount(int reconnectCount) {
    this.reconnectCount = reconnectCount;
    return this;
  }

  /**
   * Gets frequency of metrics log print out.
   *
   * @return Frequency of metrics log print out.
   */
  public long getMetricsLogFrequency() {
    return metricsLogFrequency;
  }

  /**
   * Sets frequency of metrics log print out.
   *
   * @param metricsLogFrequency Frequency of metrics log print out.
   * @return reference to this, for fluency
   */
  public IgniteOptions setMetricsLogFrequency(long metricsLogFrequency) {
    this.metricsLogFrequency = metricsLogFrequency;
    return this;
  }

  /**
   * Should return fully configured discovery options. If not provided,
   * TcpDiscovery will be used by default.
   *
   * @return Grid discovery options {@link IgniteDiscoveryOptions}.
   */
  public IgniteDiscoveryOptions getDiscoverySpi() {
    return discoveryOptions;
  }

  /**
   * Sets fully configured instance of {@link IgniteDiscoveryOptions}.
   *
   * @param discoveryOptions {@link IgniteDiscoveryOptions}.
   * @return reference to this, for fluency
   */
  public IgniteOptions setDiscoverySpi(IgniteDiscoveryOptions discoveryOptions) {
    this.discoveryOptions = discoveryOptions;
    return this;
  }

  /**
   * Gets configuration (descriptors) for all caches.
   *
   * @return List of cache configurations.
   */
  public List<IgniteCacheOptions> getCacheConfiguration() {
    return cacheConfiguration;
  }

  /**
   * Sets cache configurations.
   *
   * @param cacheConfiguration Cache configurations.
   * @return reference to this, for fluency
   */
  public IgniteOptions setCacheConfiguration(List<IgniteCacheOptions> cacheConfiguration) {
    this.cacheConfiguration = cacheConfiguration;
    return this;
  }

  public IgniteSslOptions getSslContextFactory() {
    return sslOptions;
  }

  /**
   * Sets SSL options that will be used for creating a secure socket layer.
   *
   * @param sslOptions Ssl options.
   * @return reference to this, for fluency
   */
  public IgniteOptions setSslContextFactory(IgniteSslOptions sslOptions) {
    this.sslOptions = sslOptions;
    return this;
  }

  public boolean isShutdownOnSegmentation() {
    return shutdownOnSegmentation;
  }

  /**
   * Sets that vertx will be shutdown when the cache goes into segmented state.
   * Defaults to true
   *
   * @param shutdownOnSegmentation boolean flag.
   * @return reference to this, for fluency
   */
  public IgniteOptions setShutdownOnSegmentation(boolean shutdownOnSegmentation) {
    this.shutdownOnSegmentation = shutdownOnSegmentation;
    return this;
  }

  public int getPageSize() {
    return pageSize;
  }

  /**
   * Sets page size for all data regions.
   * Defaults to 4096 bytes
   *
   * @param pageSize size in bytes.
   * @return reference to this, for fluency
   */
  public IgniteOptions setPageSize(int pageSize) {
    this.pageSize = pageSize;
    return this;
  }

  /**
   * Get default data region start size.
   * Default to 256 MB
   *
   * @return size in bytes.
   */
  public long getDefaultRegionInitialSize() {
    return defaultRegionInitialSize;
  }

  /**
   * Sets default data region start size.
   *
   * @param defaultRegionInitialSize size in bytes.
   * @return reference to this, for fluency
   */
  public IgniteOptions setDefaultRegionInitialSize(long defaultRegionInitialSize) {
    this.defaultRegionInitialSize = defaultRegionInitialSize;
    return this;
  }

  /**
   * Get default data region maximum size.
   * Default to 20% of physical memory available
   *
   * @return size in bytes.
   */
  public long getDefaultRegionMaxSize() {
    return defaultRegionMaxSize;
  }

  /**
   * Sets default data region maximum size.
   *
   * @param defaultRegionMaxSize size in bytes.
   * @return reference to this, for fluency
   */
  public IgniteOptions setDefaultRegionMaxSize(long defaultRegionMaxSize) {
    this.defaultRegionMaxSize = defaultRegionMaxSize;
    return this;
  }

  public boolean isDefaultRegionMetricsEnabled() {
    return defaultRegionMetricsEnabled;
  }

  /**
   * Sets default data region metrics enabled/disabled.
   * Defaults to false
   *
   * @param defaultRegionMetricsEnabled to set.
   * @return reference to this, for fluency
   */
  public IgniteOptions setDefaultRegionMetricsEnabled(boolean defaultRegionMetricsEnabled) {
    this.defaultRegionMetricsEnabled = defaultRegionMetricsEnabled;
    return this;
  }

  public boolean isShutdownOnNodeStop() {
    return shutdownOnNodeStop;
  }

  /**
   * Sets that vertx will be shutdown when the node stops.
   * Defaults to false
   *
   * @param shutdownOnNodeStop to set.
   * @return reference to this, for fluency
   */
  public IgniteOptions setShutdownOnNodeStop(boolean shutdownOnNodeStop) {
    this.shutdownOnNodeStop = shutdownOnNodeStop;
    return this;
  }

  public long getMetricsUpdateFrequency() {
    return metricsUpdateFrequency;
  }

  /**
   * Sets update frequency of metrics.
   * Defaults to 2 seconds
   *
   * @param metricsUpdateFrequency in milliseconds.
   * @return reference to this, for fluency
   */
  public IgniteOptions setMetricsUpdateFrequency(long metricsUpdateFrequency) {
    this.metricsUpdateFrequency = metricsUpdateFrequency;
    return this;
  }

  public long getClientFailureDetectionTimeout() {
    return clientFailureDetectionTimeout;
  }

  /**
   * Sets client failure detection timeout.
   * Defaults to 30 seconds
   *
   * @param clientFailureDetectionTimeout in milliseconds.
   * @return reference to this, for fluency
   */
  public IgniteOptions setClientFailureDetectionTimeout(long clientFailureDetectionTimeout) {
    this.clientFailureDetectionTimeout = clientFailureDetectionTimeout;
    return this;
  }

  public int getMetricsHistorySize() {
    return metricsHistorySize;
  }

  /**
   * Sets metrics history size.
   * Defaults to 10000
   *
   * @param metricsHistorySize to set.
   * @return reference to this, for fluency
   */
  public IgniteOptions setMetricsHistorySize(int metricsHistorySize) {
    this.metricsHistorySize = metricsHistorySize;
    return this;
  }

  public long getMetricsExpireTime() {
    return metricsExpireTime;
  }

  /**
   * Sets metrics expire time.
   * Defaults to never expire
   *
   * @param metricsExpireTime in milliseconds.
   * @return reference to this, for fluency
   */
  public IgniteOptions setMetricsExpireTime(long metricsExpireTime) {
    this.metricsExpireTime = metricsExpireTime;
    return this;
  }

  public IgniteMetricExporterOptions getMetricExporterSpi() {
    return metricExporterOptions;
  }

  /**
   * Sets fully configured instance of {@link IgniteMetricExporterOptions}.
   *
   * @param metricExporterOptions {@link IgniteMetricExporterOptions}.
   * @return reference to this, for fluency
   */
  public IgniteOptions setMetricExporterSpi(IgniteMetricExporterOptions metricExporterOptions) {
    this.metricExporterOptions = metricExporterOptions;
    return this;
  }

  public long getDelayAfterStart() {
    return delayAfterStart;
  }

  /**
   * Sets delay in millisenconds after Ignite start.
   * Defaults to 100ms
   *
   * @param delayAfterStart in milliseconds.
   * @return reference to this, for fluency
   */
  public IgniteOptions setDelayAfterStart(long delayAfterStart) {
    this.delayAfterStart = delayAfterStart;
    return this;
  }

  /**
   * Convert to JSON
   *
   * @return the JSON
   */
  public JsonObject toJson() {
    JsonObject json = new JsonObject();
    IgniteOptionsConverter.toJson(this, json);
    return json;
  }
}
