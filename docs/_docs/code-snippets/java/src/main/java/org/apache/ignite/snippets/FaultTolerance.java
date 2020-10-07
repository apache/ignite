package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.failover.always.AlwaysFailoverSpi;
import org.apache.ignite.spi.failover.never.NeverFailoverSpi;

public class FaultTolerance {
    void always() {
        // tag::always[]
        AlwaysFailoverSpi failSpi = new AlwaysFailoverSpi();

        // Override maximum failover attempts.
        failSpi.setMaximumFailoverAttempts(5);

        // Override the default failover SPI.
        IgniteConfiguration cfg = new IgniteConfiguration().setFailoverSpi(failSpi);

        // Start a node.
        Ignite ignite = Ignition.start(cfg);
        // end::always[]

        ignite.close();
    }

    void never() {
        // tag::never[]
        NeverFailoverSpi failSpi = new NeverFailoverSpi();

        IgniteConfiguration cfg = new IgniteConfiguration();

        // Override the default failover SPI.
        cfg.setFailoverSpi(failSpi);

        // Start a node.
        Ignite ignite = Ignition.start(cfg);
        // end::never[]

        ignite.close();
    }

    public static void main(String[] args) {
        FaultTolerance ft = new FaultTolerance();

        ft.always();
        ft.never();
    }
}
