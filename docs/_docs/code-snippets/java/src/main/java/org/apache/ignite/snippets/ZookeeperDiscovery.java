package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpi;

public class ZookeeperDiscovery {

    void ZookeeperDiscoveryConfigurationExample() {
        //tag::cfg[]
        ZookeeperDiscoverySpi zkDiscoverySpi = new ZookeeperDiscoverySpi();

        zkDiscoverySpi.setZkConnectionString("127.0.0.1:34076,127.0.0.1:43310,127.0.0.1:36745");
        zkDiscoverySpi.setSessionTimeout(30_000);

        zkDiscoverySpi.setZkRootPath("/ignite");
        zkDiscoverySpi.setJoinTimeout(10_000);

        IgniteConfiguration cfg = new IgniteConfiguration();

        //Override default discovery SPI.
        cfg.setDiscoverySpi(zkDiscoverySpi);

        // Start the node.
        Ignite ignite = Ignition.start(cfg);
        //end::cfg[]
        ignite.close();
    }
}
