package org.gridgain.examples;

import org.gridgain.examples.compute.failover.*;
import org.gridgain.testframework.junits.common.*;

/**
 * Checkpoint examples self test.
 */
public class GridCheckpointExamplesSelfTest extends GridAbstractExamplesTest {
    /**
     * Starts remote nodes before each test.
     *
     * Note: using beforeTestsStarted() to start nodes only once won't work.
     *
     * @throws Exception If remote nodes start failed.
     */
    @Override protected void beforeTest() throws Exception {
        for (int i = 0; i < RMT_NODES_CNT; i++)
            startGrid("node-" + i, ComputeFailoverNodeStartup.configuration());
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridCheckpointExample() throws Exception {
        ComputeFailoverExample.main(EMPTY_ARGS);
    }
}
