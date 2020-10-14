package org.apache.ignite.internal.ducktest.tests.runnable;

import org.apache.ignite.Ignite;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 *
 */
public class DeleteDataRunnable implements IgniteRunnable {
    /** Ignite. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** Size. */
    private int size;

    public DeleteDataRunnable() {
    }

    @Override
    public void run() {

    }
}
