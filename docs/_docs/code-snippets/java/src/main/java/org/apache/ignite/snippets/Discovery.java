package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.junit.jupiter.api.Test;

public class Discovery {

    @Test
    void clientsBehindNat() {

        //tag::client-behind-nat[]
        IgniteConfiguration cfg = new IgniteConfiguration();
        
        cfg.setClientMode(true);

        cfg.setCommunicationSpi(new TcpCommunicationSpi().setForceClientToServerConnections(true));

        //end::client-behind-nat[]
        try(Ignite ignite = Ignition.start(cfg)) {
            
        } 
    }
}
