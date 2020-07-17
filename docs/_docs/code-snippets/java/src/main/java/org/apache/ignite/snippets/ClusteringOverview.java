package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.junit.jupiter.api.Test;

public class ClusteringOverview {

    @Test
     void clientModeCfg() {
        Ignite serverNode = Ignition.start(new IgniteConfiguration().setIgniteInstanceName("server-node"));
        // tag::clientCfg[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        // Enable client mode.
        cfg.setClientMode(true);

        // Start the node in client mode.
        Ignite ignite = Ignition.start(cfg);
        // end::clientCfg[]

        ignite.close();
        serverNode.close();
    }

    @Test
     void setClientModeEnabledByIgnition() {

        Ignite serverNode = Ignition.start(new IgniteConfiguration().setIgniteInstanceName("server-node"));
        // tag::clientModeIgnition[]
        Ignition.setClientMode(true);

        // Start the node in the client mode.
        Ignite ignite = Ignition.start();
        // end::clientModeIgnition[]
        

        ignite.close();
        serverNode.close();
    }

    @Test
    void communicationSpiDemo() {

        Ignite serverNode = Ignition.start(new IgniteConfiguration().setIgniteInstanceName("server-node"));
        // tag::commSpi[]
        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        // Override local port.
        commSpi.setLocalPort(4321);

        IgniteConfiguration cfg = new IgniteConfiguration();
        // end::commSpi[]
        // tag::commSpi[]

        // Override default communication SPI.
        cfg.setCommunicationSpi(commSpi);

        // Start grid.
        Ignition.start(cfg);
        // end::commSpi[]
        serverNode.close();
        Ignition.ignite().close();
    }
}
