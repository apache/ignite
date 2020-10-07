package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.junit.jupiter.api.Test;

public class NetworkConfiguration {

    @Test
    void discoveryConfigExample() {
        //tag::discovery[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi().setLocalPort(8300);

        cfg.setDiscoverySpi(discoverySpi);
        Ignite ignite = Ignition.start(cfg);
        //end::discovery[]
        ignite.close();
    }

    @Test
    void failureDetectionTimeout() {
        //tag::failure-detection-timeout[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setFailureDetectionTimeout(5_000);

        cfg.setClientFailureDetectionTimeout(10_000);
        //end::failure-detection-timeout[]
        Ignition.start(cfg).close();
    }

}
