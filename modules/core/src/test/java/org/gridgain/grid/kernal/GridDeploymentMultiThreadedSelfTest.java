/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;

/**
 * Task deployment tests.
 */
public class GridDeploymentMultiThreadedSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int THREAD_CNT = 20;

    /** */
    private static final int EXEC_CNT = 30000;

    /**
     * @throws Exception If failed.
     */
    public void testDeploy() throws Exception {
        try {
            final Ignite ignite = startGrid(0);

            ignite.compute().localDeployTask(GridDeploymentTestTask.class, GridDeploymentTestTask.class.getClassLoader());

            assert ignite.compute().localTasks().get(GridDeploymentTestTask.class.getName()) != null;

            ignite.compute().undeployTask(GridDeploymentTestTask.class.getName());

            final CyclicBarrier barrier = new CyclicBarrier(THREAD_CNT, new Runnable() {
                private int iterCnt;

                @Override public void run() {
                    try {
                        ignite.compute().undeployTask(GridDeploymentTestTask.class.getName());

                        assert ignite.compute().localTasks().get(GridDeploymentTestTask.class.getName()) == null;

                        if (++iterCnt % 100 == 0)
                            info("Iterations count: " + iterCnt);
                    }
                    catch (GridException e) {
                        U.error(log, "Failed to undeploy task message.", e);

                        fail("See logs for details.");
                    }
                }
            });

            GridTestUtils.runMultiThreaded(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    try {
                        for (int i = 0; i < EXEC_CNT; i++) {
                            barrier.await(2000, MILLISECONDS);

                            ignite.compute().localDeployTask(GridDeploymentTestTask.class,
                                GridDeploymentTestTask.class.getClassLoader());

                            assert ignite.compute().localTasks().get(GridDeploymentTestTask.class.getName()) != null;
                        }
                    }
                    catch (Exception e) {
                        U.error(log, "Test failed.", e);

                        throw e;
                    }
                    finally {
                        info("Thread finished.");
                    }

                    return null;
                }
            }, THREAD_CNT, "grid-load-test-thread");
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Test task.
     */
    private static class GridDeploymentTestTask extends GridComputeTaskAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> subgrid, Object arg) throws GridException {
            assert false;

            return Collections.emptyMap();
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
            return null;
        }
    }
}
