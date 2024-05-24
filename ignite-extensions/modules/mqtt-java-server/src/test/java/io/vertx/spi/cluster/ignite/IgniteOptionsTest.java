package io.vertx.spi.cluster.ignite;

import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.json.JsonObject;
import io.vertx.spi.cluster.ignite.util.ConfigHelper;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.metric.MetricExporterSpi;
import org.apache.ignite.spi.metric.noop.NoopMetricExporterSpi;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.ignite.configuration.DataStorageConfiguration.*;
import static org.apache.ignite.configuration.IgniteConfiguration.*;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.*;
import static org.junit.Assert.*;

public class IgniteOptionsTest {

  @Test
  public void defaults() {
    IgniteOptions options = new IgniteOptions();
    assertNull(options.getLocalHost());
    assertEquals(DFLT_PORT, options.getLocalPort());
    assertEquals(DFLT_CONN_PER_NODE, options.getConnectionsPerNode());
    assertEquals(DFLT_CONN_TIMEOUT, options.getConnectTimeout());
    assertEquals(DFLT_IDLE_CONN_TIMEOUT, options.getIdleConnectionTimeout());
    assertEquals(DFLT_RECONNECT_CNT, options.getReconnectCount());
    assertEquals(DFLT_MAX_CONN_TIMEOUT, options.getMaxConnectTimeout());
    assertEquals(DFLT_METRICS_LOG_FREQ, options.getMetricsLogFrequency());
    assertEquals("TcpDiscoveryMulticastIpFinder", options.getDiscoverySpi().getType());
    assertEquals(0, options.getDiscoverySpi().getProperties().size());
    assertNull(options.getDiscoverySpi().getCustomSpi());
    assertEquals(0, options.getCacheConfiguration().size());
    assertNull(options.getSslContextFactory());
    assertTrue(options.isShutdownOnSegmentation());
    assertEquals(DFLT_PAGE_SIZE, options.getPageSize());
    assertEquals(DFLT_DATA_REGION_INITIAL_SIZE, options.getDefaultRegionInitialSize());
    assertEquals(DFLT_DATA_REGION_MAX_SIZE, options.getDefaultRegionMaxSize());
    assertFalse(options.isDefaultRegionMetricsEnabled());
    assertFalse(options.isShutdownOnNodeStop());
    assertEquals(100L, options.getDelayAfterStart());
    assertEquals(DFLT_METRICS_UPDATE_FREQ, options.getMetricsUpdateFrequency());
    assertEquals(DFLT_CLIENT_FAILURE_DETECTION_TIMEOUT.longValue(), options.getClientFailureDetectionTimeout());
    assertEquals(DFLT_METRICS_HISTORY_SIZE, options.getMetricsHistorySize());
    assertEquals(DFLT_METRICS_EXPIRE_TIME, options.getMetricsExpireTime());
    assertNotNull(options.getMetricExporterSpi());
  }

  @Test
  public void fromEmptyJson() {
    IgniteOptions options = new IgniteOptions(new JsonObject());
    assertNull(options.getLocalHost());
    assertEquals(DFLT_PORT, options.getLocalPort());
    assertEquals(DFLT_CONN_PER_NODE, options.getConnectionsPerNode());
    assertEquals(DFLT_CONN_TIMEOUT, options.getConnectTimeout());
    assertEquals(DFLT_IDLE_CONN_TIMEOUT, options.getIdleConnectionTimeout());
    assertEquals(DFLT_RECONNECT_CNT, options.getReconnectCount());
    assertEquals(DFLT_MAX_CONN_TIMEOUT, options.getMaxConnectTimeout());
    assertEquals(DFLT_METRICS_LOG_FREQ, options.getMetricsLogFrequency());
    assertEquals("TcpDiscoveryMulticastIpFinder", options.getDiscoverySpi().getType());
    assertEquals(0, options.getDiscoverySpi().getProperties().size());
    assertNull(options.getDiscoverySpi().getCustomSpi());
    assertEquals(0, options.getCacheConfiguration().size());
    assertNull(options.getSslContextFactory());
    assertTrue(options.isShutdownOnSegmentation());
    assertEquals(DFLT_PAGE_SIZE, options.getPageSize());
    assertEquals(DFLT_DATA_REGION_INITIAL_SIZE, options.getDefaultRegionInitialSize());
    assertEquals(DFLT_DATA_REGION_MAX_SIZE, options.getDefaultRegionMaxSize());
    assertFalse(options.isDefaultRegionMetricsEnabled());
    assertFalse(options.isShutdownOnNodeStop());
    assertEquals(100L, options.getDelayAfterStart());
    assertEquals(DFLT_METRICS_UPDATE_FREQ, options.getMetricsUpdateFrequency());
    assertEquals(DFLT_CLIENT_FAILURE_DETECTION_TIMEOUT.longValue(), options.getClientFailureDetectionTimeout());
    assertEquals(DFLT_METRICS_HISTORY_SIZE, options.getMetricsHistorySize());
    assertEquals(DFLT_METRICS_EXPIRE_TIME, options.getMetricsExpireTime());
    assertNotNull(options.getMetricExporterSpi());
  }

  private void checkConfig(IgniteOptions options, IgniteConfiguration config) {
    assertEquals(options.getLocalHost(), config.getLocalHost());
    assertEquals("TcpCommunicationSpi", config.getCommunicationSpi().getClass().getSimpleName());
    assertEquals(options.getLocalPort(), ((TcpCommunicationSpi) config.getCommunicationSpi()).getLocalPort());
    assertEquals(options.getConnectionsPerNode(), ((TcpCommunicationSpi) config.getCommunicationSpi()).getConnectionsPerNode());
    assertEquals(options.getConnectTimeout(), ((TcpCommunicationSpi) config.getCommunicationSpi()).getConnectTimeout());
    assertEquals(options.getIdleConnectionTimeout(), ((TcpCommunicationSpi) config.getCommunicationSpi()).getIdleConnectionTimeout());
    assertEquals(options.getMaxConnectTimeout(), ((TcpCommunicationSpi) config.getCommunicationSpi()).getMaxConnectTimeout());
    assertEquals(options.getReconnectCount(), ((TcpCommunicationSpi) config.getCommunicationSpi()).getReconnectCount());
    assertEquals(options.getMetricsLogFrequency(), config.getMetricsLogFrequency());
    assertEquals("TcpDiscoverySpi", config.getDiscoverySpi().getName());
    assertEquals(options.getDiscoverySpi().getProperties().getLong("joinTimeout").longValue(), ((TcpDiscoverySpi) config.getDiscoverySpi()).getJoinTimeout());
    assertEquals(options.getDiscoverySpi().getType(), ((TcpDiscoverySpi) config.getDiscoverySpi()).getIpFinder().getClass().getSimpleName());
    assertEquals(options.getSslContextFactory().getProtocol(), config.getSslContextFactory().create().getProtocol());
    assertEquals(1, config.getCacheConfiguration().length);
    assertEquals(options.getCacheConfiguration().get(0).getName(), config.getCacheConfiguration()[0].getName());
    assertEquals(options.getCacheConfiguration().get(0).getAtomicityMode(), config.getCacheConfiguration()[0].getAtomicityMode().name());
    assertEquals(options.getCacheConfiguration().get(0).getCacheMode(), config.getCacheConfiguration()[0].getCacheMode().name());
    assertEquals(options.getCacheConfiguration().get(0).getGroupName(), config.getCacheConfiguration()[0].getGroupName());
    assertEquals(options.getCacheConfiguration().get(0).getPartitionLossPolicy(), config.getCacheConfiguration()[0].getPartitionLossPolicy().name());
    assertEquals(options.getCacheConfiguration().get(0).getRebalanceMode(), config.getCacheConfiguration()[0].getRebalanceMode().name());
    assertEquals(options.getCacheConfiguration().get(0).getRebalanceDelay(), config.getCacheConfiguration()[0].getRebalanceDelay());
    assertEquals(options.getCacheConfiguration().get(0).getRebalanceOrder(), config.getCacheConfiguration()[0].getRebalanceOrder());
    assertEquals(options.getCacheConfiguration().get(0).getWriteSynchronizationMode(), config.getCacheConfiguration()[0].getWriteSynchronizationMode().name());
    assertEquals(options.getCacheConfiguration().get(0).getBackups(), config.getCacheConfiguration()[0].getBackups());
    assertEquals(options.getCacheConfiguration().get(0).getMaxConcurrentAsyncOperations(), config.getCacheConfiguration()[0].getMaxConcurrentAsyncOperations());
    assertEquals(options.getCacheConfiguration().get(0).getMaxQueryInteratorsCount(), config.getCacheConfiguration()[0].getMaxQueryIteratorsCount());
    assertEquals(options.getCacheConfiguration().get(0).isEagerTtl(), config.getCacheConfiguration()[0].isEagerTtl());
    assertEquals(options.getCacheConfiguration().get(0).isCopyOnRead(), config.getCacheConfiguration()[0].isCopyOnRead());
    assertEquals(options.getCacheConfiguration().get(0).isEventsDisabled(), config.getCacheConfiguration()[0].isEventsDisabled());
    assertEquals(options.getCacheConfiguration().get(0).isInvalidate(), config.getCacheConfiguration()[0].isInvalidate());
    assertEquals(options.getCacheConfiguration().get(0).isOnheapCacheEnabled(), config.getCacheConfiguration()[0].isOnheapCacheEnabled());
    assertEquals(options.getCacheConfiguration().get(0).isReadFromBackup(), config.getCacheConfiguration()[0].isReadFromBackup());
    assertEquals(options.getCacheConfiguration().get(0).isMetricsEnabled(), config.getCacheConfiguration()[0].isStatisticsEnabled());
    assertNotNull(config.getCacheConfiguration()[0].getExpiryPolicyFactory());
    assertEquals(options.getPageSize(), config.getDataStorageConfiguration().getPageSize());
    assertEquals(options.getDefaultRegionInitialSize(), config.getDataStorageConfiguration().getDefaultDataRegionConfiguration().getInitialSize());
    assertEquals(options.getDefaultRegionMaxSize(), config.getDataStorageConfiguration().getDefaultDataRegionConfiguration().getMaxSize());
    assertEquals(options.isDefaultRegionMetricsEnabled(), config.getDataStorageConfiguration().getDefaultDataRegionConfiguration().isMetricsEnabled());
    assertEquals(options.getMetricsUpdateFrequency(), config.getMetricsUpdateFrequency());
    assertEquals(options.getClientFailureDetectionTimeout(), config.getClientFailureDetectionTimeout().longValue());
    assertEquals(options.getMetricsHistorySize(), config.getMetricsHistorySize());
    assertEquals(options.getMetricsExpireTime(), config.getMetricsExpireTime());
  }

  private IgniteOptions createIgniteOptions() {
    return new IgniteOptions()
      .setLocalHost("localHost")
      .setLocalPort(12345)
      .setConnectionsPerNode(2)
      .setConnectTimeout(2000L)
      .setIdleConnectionTimeout(300_000L)
      .setMaxConnectTimeout(200_000L)
      .setReconnectCount(20)
      .setMetricsLogFrequency(10L)
      .setDiscoverySpi(new IgniteDiscoveryOptions()
        .setType("TcpDiscoveryVmIpFinder")
        .setProperties(new JsonObject().put("joinTimeout", 10_000L)))
      .setSslContextFactory(new IgniteSslOptions()
        .setProtocol("TLSv1.2")
        .setKeyAlgorithm("SunX509")
        .setKeyStoreType("JKS")
        .setKeyStoreFilePath("src/test/resources/server.jks")
        .setKeyStorePassword("123456")
        .setTrustStoreType("JKS")
        .setTrustStoreFilePath("src/test/resources/server.jks")
        .setTrustStorePassword("123456")
        .setTrustAll(true))
      .setCacheConfiguration(Collections.singletonList(new IgniteCacheOptions()
        .setName("*")
        .setAtomicityMode("TRANSACTIONAL")
        .setBackups(1)
        .setCacheMode("PARTITIONED")
        .setCopyOnRead(false)
        .setEagerTtl(false)
        .setEventsDisabled(true)
        .setGroupName("testGroup")
        .setInvalidate(true)
        .setMaxConcurrentAsyncOperations(100)
        .setMaxQueryInteratorsCount(512)
        .setOnheapCacheEnabled(true)
        .setPartitionLossPolicy("READ_WRITE_ALL")
        .setReadFromBackup(false)
        .setRebalanceDelay(100L)
        .setRebalanceMode("SYNC")
        .setRebalanceOrder(1)
        .setWriteSynchronizationMode("FULL_SYNC")
        .setExpiryPolicy(new JsonObject()
          .put("type", "created")
          .put("duration", 60000L)
        )
        .setMetricsEnabled(true)
      ))
      .setPageSize(1024)
      .setDefaultRegionInitialSize(40L * 1024 * 1024)
      .setDefaultRegionMaxSize(100L * 1024 * 1024)
      .setDefaultRegionMetricsEnabled(true)
      .setShutdownOnSegmentation(false)
      .setShutdownOnNodeStop(true)
      .setDelayAfterStart(200L)
      .setMetricsUpdateFrequency(10_000L)
      .setClientFailureDetectionTimeout(15_000L)
      .setMetricsHistorySize(1)
      .setMetricsExpireTime(2)
      .setMetricExporterSpi(new IgniteMetricExporterOptions());
  }

  @Test
  public void toConfig() {
    IgniteOptions options = createIgniteOptions();
    IgniteConfiguration config = ConfigHelper.toIgniteConfig(Vertx.vertx(), options);
    checkConfig(options, config);
  }

  @Test(expected = VertxException.class)
  public void noDiscoverySpiFound() {
    IgniteOptions options = new IgniteOptions()
      .setDiscoverySpi(new IgniteDiscoveryOptions()
        .setType("NotExistingSpi"));
    ConfigHelper.toIgniteConfig(Vertx.vertx(), options);
  }

  private void checkJson(IgniteOptions options, JsonObject json) {
    assertEquals(options.getLocalHost(), json.getString("localHost"));
    assertEquals(options.getLocalPort(), json.getInteger("localPort").intValue());
    assertEquals(options.getConnectionsPerNode(), json.getInteger("connectionsPerNode").intValue());
    assertEquals(options.getConnectTimeout(), json.getLong("connectTimeout").longValue());
    assertEquals(options.getIdleConnectionTimeout(), json.getLong("idleConnectionTimeout").longValue());
    assertEquals(options.getMaxConnectTimeout(), json.getLong("maxConnectTimeout").longValue());
    assertEquals(options.getReconnectCount(), json.getInteger("reconnectCount").intValue());
    assertEquals(options.getMetricsLogFrequency(), json.getLong("metricsLogFrequency").longValue());
    assertEquals(options.isShutdownOnSegmentation(), json.getBoolean("shutdownOnSegmentation"));
    assertEquals(options.isShutdownOnNodeStop(), json.getBoolean("shutdownOnNodeStop"));
    assertEquals(options.getDelayAfterStart(), json.getLong("delayAfterStart").longValue());
    assertEquals(options.getDiscoverySpi().getType(), json.getJsonObject("discoverySpi").getString("type"));
    assertEquals(options.getDiscoverySpi().getProperties().getLong("joinTimeout"), json.getJsonObject("discoverySpi").getJsonObject("properties").getLong("joinTimeout"));
    assertEquals(options.getSslContextFactory().getProtocol(), json.getJsonObject("sslContextFactory").getString("protocol"));
    assertEquals(options.getSslContextFactory().getKeyAlgorithm(), json.getJsonObject("sslContextFactory").getString("keyAlgorithm"));
    assertEquals(options.getSslContextFactory().getKeyStoreType(), json.getJsonObject("sslContextFactory").getString("keyStoreType"));
    assertEquals(options.getSslContextFactory().getKeyStoreFilePath(), json.getJsonObject("sslContextFactory").getString("keyStoreFilePath"));
    assertEquals(options.getSslContextFactory().getKeyStorePassword(), json.getJsonObject("sslContextFactory").getString("keyStorePassword"));
    assertEquals(options.getSslContextFactory().getTrustStoreType(), json.getJsonObject("sslContextFactory").getString("trustStoreType"));
    assertEquals(options.getSslContextFactory().getTrustStoreFilePath(), json.getJsonObject("sslContextFactory").getString("trustStoreFilePath"));
    assertEquals(options.getSslContextFactory().getTrustStorePassword(), json.getJsonObject("sslContextFactory").getString("trustStorePassword"));
    assertEquals(options.getSslContextFactory().isTrustAll(), json.getJsonObject("sslContextFactory").getBoolean("trustAll"));
    assertEquals(1, json.getJsonArray("cacheConfiguration").size());
    assertEquals(options.getCacheConfiguration().get(0).getName(), json.getJsonArray("cacheConfiguration").getJsonObject(0).getString("name"));
    assertEquals(options.getCacheConfiguration().get(0).getAtomicityMode(), json.getJsonArray("cacheConfiguration").getJsonObject(0).getString("atomicityMode"));
    assertEquals(options.getCacheConfiguration().get(0).getCacheMode(), json.getJsonArray("cacheConfiguration").getJsonObject(0).getString("cacheMode"));
    assertEquals(options.getCacheConfiguration().get(0).getGroupName(), json.getJsonArray("cacheConfiguration").getJsonObject(0).getString("groupName"));
    assertEquals(options.getCacheConfiguration().get(0).getPartitionLossPolicy(), json.getJsonArray("cacheConfiguration").getJsonObject(0).getString("partitionLossPolicy"));
    assertEquals(options.getCacheConfiguration().get(0).getRebalanceMode(), json.getJsonArray("cacheConfiguration").getJsonObject(0).getString("rebalanceMode"));
    assertEquals(options.getCacheConfiguration().get(0).getRebalanceDelay(), json.getJsonArray("cacheConfiguration").getJsonObject(0).getLong("rebalanceDelay").longValue());
    assertEquals(options.getCacheConfiguration().get(0).getRebalanceOrder(), json.getJsonArray("cacheConfiguration").getJsonObject(0).getInteger("rebalanceOrder").intValue());
    assertEquals(options.getCacheConfiguration().get(0).getWriteSynchronizationMode(), json.getJsonArray("cacheConfiguration").getJsonObject(0).getString("writeSynchronizationMode"));
    assertEquals(options.getCacheConfiguration().get(0).getBackups(), json.getJsonArray("cacheConfiguration").getJsonObject(0).getInteger("backups").intValue());
    assertEquals(options.getCacheConfiguration().get(0).getMaxConcurrentAsyncOperations(), json.getJsonArray("cacheConfiguration").getJsonObject(0).getInteger("maxConcurrentAsyncOperations").intValue());
    assertEquals(options.getCacheConfiguration().get(0).getMaxQueryInteratorsCount(), json.getJsonArray("cacheConfiguration").getJsonObject(0).getInteger("maxQueryInteratorsCount").intValue());
    assertEquals(options.getCacheConfiguration().get(0).isEagerTtl(), json.getJsonArray("cacheConfiguration").getJsonObject(0).getBoolean("eagerTtl"));
    assertEquals(options.getCacheConfiguration().get(0).isCopyOnRead(), json.getJsonArray("cacheConfiguration").getJsonObject(0).getBoolean("copyOnRead"));
    assertEquals(options.getCacheConfiguration().get(0).isEventsDisabled(), json.getJsonArray("cacheConfiguration").getJsonObject(0).getBoolean("eventsDisabled"));
    assertEquals(options.getCacheConfiguration().get(0).isInvalidate(), json.getJsonArray("cacheConfiguration").getJsonObject(0).getBoolean("invalidate"));
    assertEquals(options.getCacheConfiguration().get(0).isOnheapCacheEnabled(), json.getJsonArray("cacheConfiguration").getJsonObject(0).getBoolean("onheapCacheEnabled"));
    assertEquals(options.getCacheConfiguration().get(0).isReadFromBackup(), json.getJsonArray("cacheConfiguration").getJsonObject(0).getBoolean("readFromBackup"));
    assertEquals(options.getCacheConfiguration().get(0).getExpiryPolicy().getString("type"), json.getJsonArray("cacheConfiguration").getJsonObject(0).getJsonObject("expiryPolicy").getString("type"));
    assertEquals(options.getCacheConfiguration().get(0).getExpiryPolicy().getString("duration"), json.getJsonArray("cacheConfiguration").getJsonObject(0).getJsonObject("expiryPolicy").getString("duration"));
    assertEquals(options.getCacheConfiguration().get(0).isMetricsEnabled(), json.getJsonArray("cacheConfiguration").getJsonObject(0).getBoolean("metricsEnabled"));
    assertEquals(options.getPageSize(), json.getInteger("pageSize").intValue());
    assertEquals(options.getDefaultRegionInitialSize(), json.getLong("defaultRegionInitialSize").longValue());
    assertEquals(options.getDefaultRegionMaxSize(), json.getLong("defaultRegionMaxSize").longValue());
    assertEquals(options.isDefaultRegionMetricsEnabled(), json.getBoolean("defaultRegionMetricsEnabled"));
    assertEquals(options.getMetricsUpdateFrequency(), json.getLong("metricsUpdateFrequency").longValue());
    assertEquals(options.getClientFailureDetectionTimeout(), json.getLong("clientFailureDetectionTimeout").longValue());
    assertEquals(options.getMetricsHistorySize(), json.getInteger("metricsHistorySize").intValue());
    assertEquals(options.getMetricsExpireTime(), json.getLong("metricsExpireTime").longValue());
  }

  @Test
  public void toJson() {
    IgniteOptions options = createIgniteOptions();
    JsonObject json = options.toJson();
    checkJson(options, json);
  }

  private static final String IGNITE_JSON = "{\n" +
    "  \"connectTimeout\": 2000,\n" +
    "  \"connectionsPerNode\": 2,\n" +
    "  \"idleConnectionTimeout\": 300000,\n" +
    "  \"localHost\": \"localHost\",\n" +
    "  \"localPort\": 12345,\n" +
    "  \"maxConnectTimeout\": 200000,\n" +
    "  \"metricsLogFrequency\": 10,\n" +
    "  \"reconnectCount\": 20,\n" +
    "  \"shutdownOnSegmentation\": true,\n" +
    "  \"shutdownOnNodeStop\": false, \n" +
    "  \"delayAfterStart\": 100, \n" +
    "  \"discoverySpi\": {\n" +
    "    \"type\": \"TcpDiscoveryVmIpFinder\",\n" +
    "    \"properties\": {\n" +
    "      \"joinTimeout\": 10000\n" +
    "    }\n" +
    "  },\n" +
    "  \"cacheConfiguration\": [{\n" +
    "    \"name\": \"*\",\n" +
    "    \"atomicityMode\": \"TRANSACTIONAL\",\n" +
    "    \"backups\": 1,\n" +
    "    \"cacheMode\": \"PARTITIONED\",\n" +
    "    \"copyOnRead\": false,\n" +
    "    \"defaultLockTimeout\": 1000,\n" +
    "    \"eagerTtl\": false,\n" +
    "    \"encryptionEnabled\": false,\n" +
    "    \"eventsDisabled\": true,\n" +
    "    \"groupName\": \"testGroup\",\n" +
    "    \"invalidate\": true,\n" +
    "    \"maxConcurrentAsyncOperations\": 100,\n" +
    "    \"maxQueryInteratorsCount\": 512,\n" +
    "    \"onheapCacheEnabled\": true,\n" +
    "    \"partitionLossPolicy\": \"READ_WRITE_ALL\",\n" +
    "    \"readFromBackup\": false,\n" +
    "    \"rebalanceDelay\": 100,\n" +
    "    \"rebalanceMode\": \"SYNC\",\n" +
    "    \"rebalanceOrder\": 1,\n" +
    "    \"writeSynchronizationMode\": \"FULL_SYNC\",\n" +
    "    \"metricsEnabled\": true,\n" +
    "    \"expiryPolicy\": {\n" +
    "      \"type\": \"created\",\n" +
    "      \"duration\": 60000\n" +
    "    }\n" +
    "  }],\n" +
    "  \"sslContextFactory\": {\n" +
    "    \"keyAlgorithm\": \"SunX509\",\n" +
    "    \"keyStoreFilePath\": \"src/test/resources/server.jks\",\n" +
    "    \"keyStorePassword\": \"123456\",\n" +
    "    \"keyStoreType\": \"JKS\",\n" +
    "    \"protocol\": \"TLSv1.2\",\n" +
    "    \"trustAll\": true,\n" +
    "    \"trustStoreFilePath\": \"src/test/resources/server.jks\",\n" +
    "    \"trustStorePassword\": \"123456\",\n" +
    "    \"trustStoreType\": \"JKS\"\n" +
    "  },\n" +
    "  \"pageSize\": 1024,\n" +
    "  \"defaultRegionInitialSize\": 41943040,\n" +
    "  \"defaultRegionMaxSize\": 104857600,\n" +
    "  \"defaultRegionMetricsEnabled\": true,\n" +
    "  \"metricsUpdateFrequency\": 100000,\n" +
    "  \"clientFailureDetectionTimeout\": 200000,\n" +
    "  \"metricsHistorySize\": 1,\n" +
    "  \"metricsExpireTime\": 2,\n" +
    "  \"systemViewExporterSpiDisabled\": true\n" +
    "}";

  @Test
  public void fromJson() {
    JsonObject json = new JsonObject(IGNITE_JSON);
    IgniteOptions options = new IgniteOptions(json);
    checkJson(options, json);
  }

  @Test
  public void copy() {
    IgniteOptions options = createIgniteOptions();
    IgniteOptions copy = new IgniteOptions(options);
    assertEquals(options.getLocalHost(), copy.getLocalHost());
  }

  private static final String IGNITE_JSON_PEM_CERT = "{\n" +
    "  \"sslContextFactory\": {\n" +
    "    \"protocol\": \"TLSv1.2\",\n" +
    "    \"pemKeyCertOptions\": {\n" +
    "      \"keyPath\": \"src/test/resources/server-key.pem\",\n" +
    "      \"certPath\": \"src/test/resources/server-cert.pem\"\n" +
    "    },\n" +
    "    \"pemTrustOptions\": {\n" +
    "      \"certPaths\": [\"src/test/resources/ca.pem\"]\n" +
    "    }\n" +
    "  }\n" +
    "}";

  @Test
  public void testPemKeyCert() {
    JsonObject json = new JsonObject(IGNITE_JSON_PEM_CERT);
    IgniteSslOptions options = new IgniteOptions(json).getSslContextFactory();
    assertEquals(options.getPemKeyCertOptions().getKeyPath(), "src/test/resources/server-key.pem");
    assertEquals(options.getPemKeyCertOptions().getCertPath(), "src/test/resources/server-cert.pem");
    assertEquals(options.getPemTrustOptions().getCertPaths().get(0), "src/test/resources/ca.pem");
    assertEquals(ConfigHelper.toSslContextFactoryConfig(Vertx.vertx(), options).create().getProtocol(), "TLSv1.2");
  }

  private static final String IGNITE_JSON_PFX_CERT = "{\n" +
    "  \"sslContextFactory\": {\n" +
    "    \"protocol\": \"TLSv1.2\",\n" +
    "    \"pfxKeyCertOptions\": {\n" +
    "      \"path\": \"src/test/resources/server-keystore.p12\",\n" +
    "      \"password\": \"wibble\"\n" +
    "    },\n" +
    "    \"pfxTrustOptions\": {\n" +
    "      \"path\": \"src/test/resources/ca.p12\",\n" +
    "      \"password\": \"wibble\"\n" +
    "    }\n" +
    "  }\n" +
    "}";

  @Test
  public void testPfxKeyCert() {
    JsonObject json = new JsonObject(IGNITE_JSON_PFX_CERT);
    IgniteSslOptions options = new IgniteOptions(json).getSslContextFactory();
    assertEquals(options.getPfxKeyCertOptions().getPath(), "src/test/resources/server-keystore.p12");
    assertEquals(options.getPfxKeyCertOptions().getPassword(), "wibble");
    assertEquals(options.getPfxTrustOptions().getPath(), "src/test/resources/ca.p12");
    assertEquals(options.getPfxTrustOptions().getPassword(), "wibble");
    assertEquals(ConfigHelper.toSslContextFactoryConfig(Vertx.vertx(), options).create().getProtocol(), "TLSv1.2");
  }

  private static final String IGNITE_JSON_JKS_CERT = "{\n" +
    "  \"sslContextFactory\": {\n" +
    "    \"protocol\": \"TLSv1.2\",\n" +
    "    \"jksKeyCertOptions\": {\n" +
    "      \"path\": \"src/test/resources/server.jks\",\n" +
    "      \"password\": \"123456\"\n" +
    "    },\n" +
    "    \"jksTrustOptions\": {\n" +
    "      \"path\": \"src/test/resources/server.jks\",\n" +
    "      \"password\": \"123456\"\n" +
    "    }\n" +
    "  }\n" +
    "}";

  @Test
  public void testJksKeyCert() {
    JsonObject json = new JsonObject(IGNITE_JSON_JKS_CERT);
    IgniteSslOptions options = new IgniteOptions(json).getSslContextFactory();
    assertEquals(options.getJksKeyCertOptions().getPath(), "src/test/resources/server.jks");
    assertEquals(options.getJksKeyCertOptions().getPassword(), "123456");
    assertEquals(options.getJksTrustOptions().getPath(), "src/test/resources/server.jks");
    assertEquals(options.getJksTrustOptions().getPassword(), "123456");
    assertEquals(ConfigHelper.toSslContextFactoryConfig(Vertx.vertx(), options).create().getProtocol(), "TLSv1.2");
  }

  @Test
  public void testCustomDiscoverySpi() {
    DiscoverySpi customSpi = new TcpDiscoverySpi();
    IgniteOptions options = new IgniteOptions();
    options.getDiscoverySpi().setCustomSpi(customSpi);
    assertEquals(options.getDiscoverySpi().getProperties(), new JsonObject());
    assertEquals(options.getDiscoverySpi().getType(), "TcpDiscoveryMulticastIpFinder");
    assertEquals(options.getDiscoverySpi().getCustomSpi(), customSpi);
    IgniteConfiguration cfg = ConfigHelper.toIgniteConfig(Vertx.vertx(), options);
    assertEquals(cfg.getDiscoverySpi(), customSpi);
  }

  @Test
  public void testCustomMetricExporterSpi() {
    MetricExporterSpi customSpi = new NoopMetricExporterSpi();
    IgniteOptions options = new IgniteOptions();
    options.getMetricExporterSpi().setCustomSpi(customSpi);
    IgniteConfiguration cfg = ConfigHelper.toIgniteConfig(Vertx.vertx(), options);
    assertEquals(customSpi, cfg.getMetricExporterSpi()[0]);
  }
}
