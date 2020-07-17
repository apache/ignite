package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.jupiter.api.Test;

public class IgniteLifecycle {
    
    void test() {
    }

    @Test
    void startNode() {
        //tag::start[]
        IgniteConfiguration cfg = new IgniteConfiguration();
        Ignite ignite = Ignition.start(cfg);
        //end::start[]
    }

    @Test
    void startAndClose() {

        //tag::autoclose[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        try (Ignite ignite = Ignition.start(cfg)) {
            //
        }

        //end::autoclose[]
    }

    @Test
    void startClientNode() {
        //tag::client-node[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        // Enable client mode.
        cfg.setClientMode(true);

        // Start a client 
        Ignite ignite = Ignition.start(cfg);
        //end::client-node[]

        ignite.close();
    }

}
