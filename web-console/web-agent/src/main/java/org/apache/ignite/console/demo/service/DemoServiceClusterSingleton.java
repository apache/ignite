

package org.apache.ignite.console.demo.service;

import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;

/**
 * Demo service to provide on one node in cluster.
 */
public class DemoServiceClusterSingleton implements Service {
    /** {@inheritDoc} */
    @Override public void cancel(ServiceContext ctx) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void init(ServiceContext ctx) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void execute(ServiceContext ctx) {
        // No-op.
    }
}
