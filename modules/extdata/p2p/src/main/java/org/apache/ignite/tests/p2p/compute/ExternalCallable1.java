package org.apache.ignite.tests.p2p.compute;

import org.apache.ignite.Ignite;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 */
public class ExternalCallable1 implements IgniteCallable {

    @IgniteInstanceResource
    Ignite ignite;

    private int param = 0;

    public ExternalCallable1() {
    }

    public ExternalCallable1(int param) {
        this.param = param;
    }

    @Override public Object call() {
        System.err.println("!!!!! I am job_1 " + param + " on " + ignite.name());

        return 42;
    }
}


