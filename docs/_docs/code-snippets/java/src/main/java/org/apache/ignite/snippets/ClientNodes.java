package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteClientDisconnectedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.junit.jupiter.api.Test;

public class ClientNodes {

    @Test
    void disableReconnection() {

        //tag::disable-reconnection[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
        discoverySpi.setClientReconnectDisabled(true);

        cfg.setDiscoverySpi(discoverySpi);
        //end::disable-reconnection[]

        try (Ignite ignite = Ignition.start(cfg)) {

        }
    }

    void slowClient() {
        //tag::slow-clients[]
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setClientMode(true);

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();
        commSpi.setSlowClientQueueLimit(1000);

        cfg.setCommunicationSpi(commSpi);
        //end::slow-clients[]
    }

    void reconnect() {
        Ignite ignite = Ignition.start();

        //tag::reconnect[]

        IgniteCache cache = ignite.getOrCreateCache(new CacheConfiguration<>("myCache"));

        try {
            cache.put(1, "value");
        } catch (IgniteClientDisconnectedException e) {
            if (e.getCause() instanceof IgniteClientDisconnectedException) {
                IgniteClientDisconnectedException cause = (IgniteClientDisconnectedException) e.getCause();

                cause.reconnectFuture().get(); // Wait until the client is reconnected. 
                // proceed
            }
        }
        //end::reconnect[]

        ignite.close();
    }
}
