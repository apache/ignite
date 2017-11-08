package org.apache.ignite.tests.p2p.compute;

import org.apache.ignite.Ignite;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 */
public class ExternalCallable2 implements IgniteCallable {
    /** */
    @IgniteInstanceResource
    Ignite ignite;

    /** */
    private int param;

    /**
     *
     */
    public ExternalCallable2() {
        // No-op.
    }

    /**
     * @param param Param.
     */
    public ExternalCallable2(int param) {
        this.param = param;
    }

    /** {@inheritDoc} */
    @Override public Object call() {
        System.err.println("!!!!! I am job_2 " + param + " on " + ignite.name());

        return 42;
    }
}


