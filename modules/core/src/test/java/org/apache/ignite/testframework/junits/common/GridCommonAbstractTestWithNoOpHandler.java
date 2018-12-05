package org.apache.ignite.testframework.junits.common;

import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.NoOpFailureHandler;

/**
 * NoOpFailureHandler marker interface.
 */
public class GridCommonAbstractTestWithNoOpHandler extends GridCommonAbstractTest {
    @Override
    protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new NoOpFailureHandler();
    }
}
