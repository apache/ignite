package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.junit.jupiter.api.Test;

public class IgniteLifecycle {

    @Test
    void startNode() {
        //tag::start[]
        IgniteConfiguration cfg = new IgniteConfiguration();
        Ignite ignite = Ignition.start(cfg);
        //end::start[]
        ignite.close();
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

    @Test
    void lifecycleEvents() {
        //tag::lifecycle[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        // Specify a lifecycle bean in the node configuration.
        cfg.setLifecycleBeans(new MyLifecycleBean());

        // Start the node.
        Ignite ignite = Ignition.start(cfg);
        //end::lifecycle[]

        ignite.close();
    }
}
