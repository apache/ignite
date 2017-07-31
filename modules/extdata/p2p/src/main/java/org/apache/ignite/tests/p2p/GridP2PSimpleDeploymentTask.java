package org.apache.ignite.tests.p2p;

import org.apache.ignite.lang.IgniteCallable;

/**
 * Test task for {@code GridP2PDetectSelfTest}
 */
public class GridP2PSimpleDeploymentTask implements IgniteCallable<Integer> {

    /**
     * @return always 1
     */
    @Override public Integer call() {
        return 1;
    }
}
