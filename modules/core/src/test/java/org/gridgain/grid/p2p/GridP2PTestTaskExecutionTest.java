package org.gridgain.grid.p2p;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;
/**
 * Test executes GridP2PTestTask on the remote node.
 * Before running of test you MUST start at least one remote node.
 */
public final class GridP2PTestTaskExecutionTest extends GridCommonAbstractTest {
    /**
     * Method executes GridP2PTestTask.
     * @throws GridException If failed.
     */
    public void testGridP2PTestTask() throws GridException {
        try (Grid g  = G.start()) {
            assert g != null;

            assert !g.cluster().forRemotes().nodes().isEmpty() : "Test requires at least 1 remote node.";

            /* Execute GridP2PTestTask. */
            GridComputeTaskFuture<Integer> fut = executeAsync(g.compute(), GridP2PTestTask.class, 1);

            /* Wait for task completion. */
            Integer res = fut.get();

            X.println("Result of execution is: " + res);

            assert res > 0 : "Result of execution is: " + res + " for more information see GridP2PTestJob";
        }
    }
}
