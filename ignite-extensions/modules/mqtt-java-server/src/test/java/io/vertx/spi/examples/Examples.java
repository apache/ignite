package io.vertx.spi.examples;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.EventBusOptions;
import io.vertx.core.http.ClientAuth;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.ignite.IgniteClusterManager;
import io.vertx.spi.cluster.ignite.IgniteOptions;
import io.vertx.spi.cluster.ignite.IgniteSslOptions;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;

/**
 * @author <a href="http://escoffier.me">Clement Escoffier</a>
 */
public class Examples {

  public void example1() {
    ClusterManager clusterManager = new IgniteClusterManager();

    VertxOptions options = new VertxOptions().setClusterManager(clusterManager);
    Vertx.clusteredVertx(options).onComplete(res -> {
      if (res.succeeded()) {
        Vertx vertx = res.result();
      } else {
        // failed!
      }
    });
  }

  public void example2() {
    IgniteConfiguration cfg = new IgniteConfiguration();
    // Configuration code (omitted)

    ClusterManager clusterManager = new IgniteClusterManager(cfg);

    VertxOptions options = new VertxOptions().setClusterManager(clusterManager);
    Vertx.clusteredVertx(options).onComplete(res -> {
      if (res.succeeded()) {
        Vertx vertx = res.result();
      } else {
        // failed!
      }
    });
  }

  public void example3(Ignite ignite) {
    // Configuration code (omitted)

    ClusterManager clusterManager = new IgniteClusterManager(ignite);

    VertxOptions options = new VertxOptions().setClusterManager(clusterManager);
    Vertx.clusteredVertx(options).onComplete(res -> {
      if (res.succeeded()) {
        Vertx vertx = res.result();
      } else {
        // failed!
      }
    });
  }

  public void example4() {
    PemKeyCertOptions pemKeyCert = new PemKeyCertOptions()
      .addCertPath("cert.pem")
      .addKeyPath("key.pem");
    PemTrustOptions pemTrust = new PemTrustOptions()
      .addCertPath("ca.pem");

    IgniteOptions igniteOptions = new IgniteOptions()
      // Configuration code (omitted)
      .setSslContextFactory(new IgniteSslOptions()
        .setProtocol("TLSv1.2")
        .setPemKeyCertOptions(pemKeyCert)
        .setPemTrustOptions(pemTrust));

    EventBusOptions eventBusOptions = new EventBusOptions()
      .setSsl(true)
      .setClientAuth(ClientAuth.REQUIRED)
      .setPemKeyCertOptions(pemKeyCert)
      .setPemTrustOptions(pemTrust);

    VertxOptions options = new VertxOptions()
      .setClusterManager(new IgniteClusterManager(igniteOptions))
      .setEventBusOptions(eventBusOptions);

    Vertx.clusteredVertx(options).onComplete(res -> {
      if (res.succeeded()) {
        Vertx vertx = res.result();
      } else {
        // failed!
      }
    });
  }
}
