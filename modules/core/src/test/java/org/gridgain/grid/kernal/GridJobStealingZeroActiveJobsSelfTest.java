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
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.collision.jobstealing.*;
import org.gridgain.grid.spi.failover.jobstealing.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Job stealing test.
 */
@GridCommonTest(group = "Kernal Self")
public class GridJobStealingZeroActiveJobsSelfTest extends GridCommonAbstractTest {
    /** */
    private static Grid grid1;

    /** */
    private static Grid grid2;

    /** */
    public GridJobStealingZeroActiveJobsSelfTest() {
        super(false /* don't start grid*/);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        grid1 = startGrid(1);
        grid2 = startGrid(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid1 = null;

        stopGrid(1);
        stopGrid(2);
    }

    /**
     * Test 2 jobs on 2 nodes.
     *
     * @throws GridException If test failed.
     */
    public void testTwoJobs() throws GridException {
        grid1.compute().execute(JobStealingTask.class, null).get();
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        GridJobStealingCollisionSpi colSpi = new GridJobStealingCollisionSpi();

        // One job at a time.
        colSpi.setActiveJobsThreshold(gridName.endsWith("1") ? 0 : 2);
        colSpi.setWaitJobsThreshold(0);

        GridJobStealingFailoverSpi failSpi = new GridJobStealingFailoverSpi();

        // Verify defaults.
        assert failSpi.getMaximumFailoverAttempts() == GridJobStealingFailoverSpi.DFLT_MAX_FAILOVER_ATTEMPTS;

        cfg.setCollisionSpi(colSpi);
        cfg.setFailoverSpi(failSpi);

        return cfg;
    }

    /** */
    @SuppressWarnings({"PublicInnerClass"})
    public static class JobStealingTask extends GridComputeTaskAdapter<Object, Object> {
        /** Grid. */
        @GridInstanceResource
        private Grid grid;

        /** Logger. */
        @GridLoggerResource
        private GridLogger log;

        /** {@inheritDoc} */
        @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> subgrid, @Nullable Object arg) throws GridException {
            assert subgrid.size() == 2 : "Invalid subgrid size: " + subgrid.size();

            Map<GridComputeJobAdapter, GridNode> map = new HashMap<>(subgrid.size());

            // Put all jobs onto local node.
            for (Iterator iter = subgrid.iterator(); iter.hasNext(); iter.next())
                map.put(new GridJobStealingJob(5000L), grid.localNode());

            return map;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
            assert results.size() == 2;

            for (GridComputeJobResult res : results) {
                log.info("Job result: " + res.getData());
            }

            String name1 = results.get(0).getData();
            String name2 = results.get(1).getData();

            assert name1.equals(name2);

            assert !name1.equals(grid1.name());
            assert name1.equals(grid2.name());

            return null;
        }
    }

    /**
     *
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static final class GridJobStealingJob extends GridComputeJobAdapter {
        /** Injected grid. */
        @GridInstanceResource
        private Grid grid;

        /**
         * @param arg Job argument.
         */
        GridJobStealingJob(Long arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override public Serializable execute() throws GridException {
            try {
                Long sleep = argument(0);

                assert sleep != null;

                Thread.sleep(sleep);
            }
            catch (InterruptedException e) {
                throw new GridException("Job got interrupted.", e);
            }

            return grid.name();
        }
    }
}
