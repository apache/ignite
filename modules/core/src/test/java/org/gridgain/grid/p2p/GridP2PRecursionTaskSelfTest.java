/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.p2p;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.resources.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

/**
 *
 */
@SuppressWarnings({"ProhibitedExceptionDeclared"})
@GridCommonTest(group = "P2P")
public class GridP2PRecursionTaskSelfTest extends GridCommonAbstractTest {
    /**
     * Current deployment mode. Used in {@link #getConfiguration(String)}.
     */
    private GridDeploymentMode depMode;

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDeploymentMode(depMode);

        return cfg;
    }

    /**
     * Process one test.
     *
     * @param depMode deployment mode.
     * @throws Exception if error occur.
     */
    private void processTest(GridDeploymentMode depMode) throws Exception {
        this.depMode = depMode;

        try {
            Ignite ignite1 = startGrid(1);
            startGrid(2);

            long res = ignite1.compute().execute(FactorialTask.class, 3L);

            assert res == factorial(3);

            res = ignite1.compute().execute(FactorialTask.class, 3L);

            assert res == factorial(3);
        }
        finally {
            stopGrid(2);
            stopGrid(1);
        }
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testPrivateMode() throws Exception {
        processTest(GridDeploymentMode.PRIVATE);
    }

    /**
     * Test GridDeploymentMode.ISOLATED mode.
     *
     * @throws Exception if error occur.
     */
    public void testIsolatedMode() throws Exception {
        processTest(GridDeploymentMode.ISOLATED);
    }

    /**
     * Test GridDeploymentMode.CONTINOUS mode.
     *
     * @throws Exception if error occur.
     */
    public void testContinuousMode() throws Exception {
        processTest(GridDeploymentMode.CONTINUOUS);
    }

    /**
     * Test GridDeploymentMode.SHARED mode.
     *
     * @throws Exception if error occur.
     */
    public void testSharedMode() throws Exception {
        processTest(GridDeploymentMode.SHARED);
    }

    /**
     * Calculates factorial.
     *
     * @param num Factorial to calculate.
     * @return Factorial for the number passed in.
     */
    private long factorial(long num) {
        assert num > 0;

        if (num == 1)
            return num;

        return num * factorial(num - 1);
    }

    /**
     * Task that will always fail due to non-transient resource injection.
     */
    public static class FactorialTask extends GridComputeTaskAdapter<Long, Long> {
        /** {@inheritDoc} */
        @Override public Map<? extends GridComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Long arg) throws GridException {
            assert arg > 1;

            Map<FactorialJob, ClusterNode> map = new HashMap<>();

            Iterator<ClusterNode> iter = subgrid.iterator();

            for (int i = 0; i < arg; i++) {
                // Recycle iterator.
                if (iter.hasNext() == false)
                    iter = subgrid.iterator();

                ClusterNode node = iter.next();

                map.put(new FactorialJob(arg - 1), node);
            }

            return map;
        }

        /** {@inheritDoc} */
        @Override public Long reduce(List<GridComputeJobResult> results) throws GridException {
            long retVal = 0;

            for (GridComputeJobResult res : results) {
                retVal += (Long)res.getData();
            }

            return retVal;
        }
    }

    /** */
    private static class FactorialJob extends GridComputeJobAdapter {
        /** */
        @GridInstanceResource
        private Ignite ignite;

        /**
         * Constructor.
         * @param arg Argument.
         */
        FactorialJob(Long arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override public Long execute() throws GridException {
            Long arg = argument(0);

            assert arg != null;
            assert arg > 0;

            if (arg == 1)
                return 1L;

            ignite.compute().localDeployTask(FactorialTask.class, FactorialTask.class.getClassLoader());

            return ignite.compute().execute(FactorialTask.class, arg);
        }
    }
}
