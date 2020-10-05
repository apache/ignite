package org.apache.ignite.snippets;

import java.util.Collections;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;

public class FailureHandler {

    void configure() {
        // tag::configure-handler[]
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setFailureHandler(new StopNodeFailureHandler());
        Ignite ignite = Ignition.start(cfg);
        // end::configure-handler[]
        ignite.close();
    }

    void failureTypes() {
        // tag::failure-types[]
        StopNodeFailureHandler failureHandler = new StopNodeFailureHandler();
        failureHandler.setIgnoredFailureTypes(Collections.EMPTY_SET);

        IgniteConfiguration cfg = new IgniteConfiguration().setFailureHandler(failureHandler);

        Ignite ignite = Ignition.start(cfg);
        // end::failure-types[]

        ignite.close();
    }

    public static void main(String[] args) {
        FailureHandler fh = new FailureHandler();
        fh.configure();
        fh.failureTypes();
    }
}
