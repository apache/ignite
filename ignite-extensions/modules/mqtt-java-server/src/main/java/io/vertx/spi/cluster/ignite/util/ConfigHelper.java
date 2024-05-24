package io.vertx.spi.cluster.ignite.util;

import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.KeyCertOptions;
import io.vertx.core.net.TrustOptions;
import io.vertx.spi.cluster.ignite.*;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.metric.MetricExporterSpi;
import org.apache.ignite.ssl.SslContextFactory;

import javax.cache.configuration.Factory;
import javax.cache.expiry.*;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_DATA_REG_DEFAULT_NAME;
import static org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi.*;
import static org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder.*;
import static org.apache.ignite.ssl.SslContextFactory.getDisabledTrustManager;

public class ConfigHelper {

  private ConfigHelper() {
    //private
  }

  public static IgniteConfiguration loadConfiguration(URL config) {
    try {
      return F.first(IgnitionEx.loadConfigurations(config).get1());
    } catch (IgniteCheckedException e) {
      throw new VertxException(e);
    }
  }

  public static IgniteConfiguration lookupXmlConfiguration(Class<?> clazz, String... files) {
    InputStream is = lookupFiles(clazz, files);
    try {
      return F.first(IgnitionEx.loadConfigurations(is).get1());
    } catch (IgniteCheckedException | IllegalArgumentException e) {
      throw new VertxException(e);
    }
  }

  public static JsonObject lookupJsonConfiguration(Class<?> clazz, String... files) {
    InputStream is = lookupFiles(clazz, files);
    try {
      return new JsonObject(readFromInputStream(is));
    } catch (NullPointerException | DecodeException | IOException e) {
      throw new VertxException(e);
    }
  }

  public static InputStream lookupFiles(Class<?> clazz, String... files) {
    ClassLoader ctxClsLoader = Thread.currentThread().getContextClassLoader();
    InputStream is = null;
    for (String file : files) {
      if (ctxClsLoader != null) {
        is = ctxClsLoader.getResourceAsStream(file);
      }
      if (is == null) {
        is = clazz.getClassLoader().getResourceAsStream(file);
      }
      if (is != null) {
        break;
      }
    }
    return is;
  }

  private static String readFromInputStream(InputStream inputStream) throws IOException {
    StringBuilder resultStringBuilder = new StringBuilder();
    try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
      String line;
      while ((line = br.readLine()) != null) {
        resultStringBuilder.append(line).append("\n");
      }
    }
    return resultStringBuilder.toString();
  }

  public static IgniteConfiguration toIgniteConfig(Vertx vertx, IgniteOptions options) {
    IgniteConfiguration configuration = new IgniteConfiguration();

    configuration
      .setLocalHost(options.getLocalHost())
      .setCommunicationSpi(
        new TcpCommunicationSpi()
          .setLocalPort(options.getLocalPort())
          .setConnectionsPerNode(options.getConnectionsPerNode())
          .setConnectTimeout(options.getConnectTimeout())
          .setIdleConnectionTimeout(options.getIdleConnectionTimeout())
          .setMaxConnectTimeout(options.getMaxConnectTimeout())
          .setReconnectCount(options.getReconnectCount())
      )
      .setDiscoverySpi(toDiscoverySpiConfig(options.getDiscoverySpi()))
      .setMetricExporterSpi(toMetricExporterSpi(options.getMetricExporterSpi()))
      .setCacheConfiguration(
        options.getCacheConfiguration().stream()
          .map(ConfigHelper::toCacheConfiguration)
          .toArray(CacheConfiguration[]::new)
      )
      .setMetricsLogFrequency(options.getMetricsLogFrequency())
      .setMetricsUpdateFrequency(options.getMetricsUpdateFrequency())
      .setClientFailureDetectionTimeout(options.getClientFailureDetectionTimeout())
      .setMetricsHistorySize(options.getMetricsHistorySize())
      .setMetricsExpireTime(options.getMetricsExpireTime());

    if (options.getSslContextFactory() != null) {
      configuration.setSslContextFactory(toSslContextFactoryConfig(vertx, options.getSslContextFactory()));
    }

    configuration.setDataStorageConfiguration(
      new DataStorageConfiguration()
        .setPageSize(options.getPageSize())
        .setDefaultDataRegionConfiguration(
          new DataRegionConfiguration()
            .setName(DFLT_DATA_REG_DEFAULT_NAME)
            .setInitialSize(options.getDefaultRegionInitialSize())
            .setMaxSize(Math.max(options.getDefaultRegionMaxSize(), options.getDefaultRegionInitialSize()))
            .setMetricsEnabled(options.isDefaultRegionMetricsEnabled())
        )
    );

    return configuration;
  }

  public static Factory<SSLContext> toSslContextFactoryConfig(Vertx vertx, IgniteSslOptions options) {
    if (options.getKeyStoreFilePath() == null || options.getKeyStoreFilePath().isEmpty()) {
      return createSslFactory(vertx, options);
    }
    return createIgniteSslFactory(options);
  }

  @Deprecated
  private static Factory<SSLContext> createIgniteSslFactory(IgniteSslOptions options) {
    SslContextFactory sslContextFactory = new SslContextFactory();
    sslContextFactory.setProtocol(options.getProtocol());
    sslContextFactory.setKeyAlgorithm(options.getKeyAlgorithm());
    sslContextFactory.setKeyStoreType(options.getKeyStoreType());
    sslContextFactory.setKeyStoreFilePath(options.getKeyStoreFilePath());
    if (options.getKeyStorePassword() != null) {
      sslContextFactory.setKeyStorePassword(options.getKeyStorePassword().toCharArray());
    }
    sslContextFactory.setTrustStoreType(options.getTrustStoreType());
    sslContextFactory.setTrustStoreFilePath(options.getTrustStoreFilePath());
    if (options.getTrustStorePassword() != null) {
      sslContextFactory.setTrustStorePassword(options.getTrustStorePassword().toCharArray());
    }
    if (options.isTrustAll()) {
      sslContextFactory.setTrustManagers(getDisabledTrustManager());
    }
    return sslContextFactory;
  }

  private static Factory<SSLContext> createSslFactory(Vertx vertx, IgniteSslOptions options) {
    final SecureRandom random = new SecureRandom();
    final List<KeyManager> keyManagers = new ArrayList<>();
    final List<TrustManager> trustManagers = new ArrayList<>();
    try {
      keyManagers.addAll(toKeyManagers(vertx, options.getPemKeyCertOptions()));
      trustManagers.addAll(toTrustManagers(vertx, options.getPemTrustOptions()));
      keyManagers.addAll(toKeyManagers(vertx, options.getPfxKeyCertOptions()));
      trustManagers.addAll(toTrustManagers(vertx, options.getPfxTrustOptions()));
      keyManagers.addAll(toKeyManagers(vertx, options.getJksKeyCertOptions()));
      trustManagers.addAll(toTrustManagers(vertx, options.getPfxTrustOptions()));
    } catch (Exception e) {
      throw new IgniteException(e);
    }
    if (keyManagers.isEmpty()) {
      return null;
    }
    if (options.isTrustAll()) {
      trustManagers.clear();
      trustManagers.add(getDisabledTrustManager());
    }
    final KeyManager[] keyManagerArray = keyManagers.toArray(new KeyManager[0]);
    final TrustManager[] trustManagerArray = trustManagers.toArray(new TrustManager[0]);
    return () -> {
      try {
        SSLContext sslContext = SSLContext.getInstance(options.getProtocol());
        sslContext.init(keyManagerArray, trustManagerArray, random);
        return sslContext;
      } catch (NoSuchAlgorithmException | KeyManagementException e) {
        throw new IgniteException(e);
      }
    };
  }

  private static List<KeyManager> toKeyManagers(Vertx vertx, KeyCertOptions keyCertOptions) throws Exception {
    if (keyCertOptions != null) {
      return Arrays.asList(keyCertOptions.getKeyManagerFactory(vertx).getKeyManagers());
    }
    return new ArrayList<>();
  }

  private static List<TrustManager> toTrustManagers(Vertx vertx, TrustOptions trustOptions) throws Exception {
    if (trustOptions != null) {
      return Arrays.asList(trustOptions.getTrustManagerFactory(vertx).getTrustManagers());
    }
    return new ArrayList<>();
  }

  private static DiscoverySpi toDiscoverySpiConfig(IgniteDiscoveryOptions options) {
    if (options.getCustomSpi() != null) {
      return options.getCustomSpi();
    }
    JsonObject properties = options.getProperties();
    switch (options.getType()) {
      case "TcpDiscoveryVmIpFinder":
        return new TcpDiscoverySpi()
          .setJoinTimeout(properties.getLong("joinTimeout", DFLT_JOIN_TIMEOUT))
          .setLocalAddress(properties.getString("localAddress", null))
          .setLocalPort(properties.getInteger("localPort", DFLT_PORT))
          .setLocalPortRange(properties.getInteger("localPortRange", DFLT_PORT_RANGE))
          .setIpFinder(new TcpDiscoveryVmIpFinder()
            .setAddresses(properties.getJsonArray("addresses", new JsonArray()).stream()
              .map(Object::toString)
              .collect(Collectors.toList())
            )
          );
      case "TcpDiscoveryMulticastIpFinder":
        return new TcpDiscoverySpi()
          .setJoinTimeout(properties.getLong("joinTimeout", DFLT_JOIN_TIMEOUT))
          .setLocalAddress(properties.getString("localAddress", null))
          .setLocalPort(properties.getInteger("localPort", DFLT_PORT))
          .setLocalPortRange(properties.getInteger("localPortRange", DFLT_PORT_RANGE))
          .setIpFinder(new TcpDiscoveryMulticastIpFinder()
            .setAddressRequestAttempts(properties.getInteger("addressRequestAttempts", DFLT_ADDR_REQ_ATTEMPTS))
            .setLocalAddress(properties.getString("localAddress", null))
            .setMulticastGroup(properties.getString("multicastGroup", DFLT_MCAST_GROUP))
            .setMulticastPort(properties.getInteger("multicastPort", DFLT_MCAST_PORT))
            .setResponseWaitTime(properties.getInteger("responseWaitTime", DFLT_RES_WAIT_TIME))
            .setTimeToLive(properties.getInteger("timeToLive", -1))
            .setAddresses(properties.getJsonArray("addresses", new JsonArray()).stream()
              .map(Object::toString)
              .collect(Collectors.toList())
            )
          );
      default:
        throw new VertxException("not discovery spi found");
    }
  }

  private static MetricExporterSpi[] toMetricExporterSpi(IgniteMetricExporterOptions options) {
    return Optional.ofNullable(options)
      .map(IgniteMetricExporterOptions::getCustomSpi)
      .map(spi -> new MetricExporterSpi[]{spi})
      .orElse(new MetricExporterSpi[0]);
  }

  private static CacheConfiguration toCacheConfiguration(IgniteCacheOptions options) {
    CacheConfiguration cfg = new CacheConfiguration<>()
      .setName(options.getName())
      .setCacheMode(CacheMode.valueOf(options.getCacheMode()))
      .setBackups(options.getBackups())
      .setReadFromBackup(options.isReadFromBackup())
      .setAtomicityMode(CacheAtomicityMode.valueOf(options.getAtomicityMode()))
      .setWriteSynchronizationMode(CacheWriteSynchronizationMode.valueOf(options.getWriteSynchronizationMode()))
      .setCopyOnRead(options.isCopyOnRead())
      .setEagerTtl(options.isEagerTtl())
      .setEncryptionEnabled(options.isEncryptionEnabled())
      .setGroupName(options.getGroupName())
      .setInvalidate(options.isInvalidate())
      .setMaxConcurrentAsyncOperations(options.getMaxConcurrentAsyncOperations())
      .setOnheapCacheEnabled(options.isOnheapCacheEnabled())
      .setPartitionLossPolicy(PartitionLossPolicy.valueOf(options.getPartitionLossPolicy()))
      .setRebalanceMode(CacheRebalanceMode.valueOf(options.getRebalanceMode()))
      .setRebalanceOrder(options.getRebalanceOrder())
      .setRebalanceDelay(options.getRebalanceDelay())
      .setMaxQueryIteratorsCount(options.getMaxQueryInteratorsCount())
      .setEventsDisabled(options.isEventsDisabled())
      .setStatisticsEnabled(options.isMetricsEnabled());
    if (options.getExpiryPolicy() != null) {
      Duration duration = new Duration(TimeUnit.MILLISECONDS, options.getExpiryPolicy().getLong("duration"));
      Factory<ExpiryPolicy> factory;
      switch (options.getExpiryPolicy().getString("type", "created")) {
        case "accessed":
          factory = AccessedExpiryPolicy.factoryOf(duration);
          break;
        case "modified":
          factory = ModifiedExpiryPolicy.factoryOf(duration);
          break;
        case "touched":
          factory = TouchedExpiryPolicy.factoryOf(duration);
          break;
        case "created":
        default:
          factory = CreatedExpiryPolicy.factoryOf(duration);
      }
      cfg.setExpiryPolicyFactory(factory);
    }
    return cfg;
  }
}
