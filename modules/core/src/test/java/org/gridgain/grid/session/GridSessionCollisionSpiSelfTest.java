/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.session;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.collision.*;
import org.gridgain.grid.spi.collision.fifoqueue.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;

/**
 * Grid session collision SPI self test.
 */
public class GridSessionCollisionSpiSelfTest extends GridCommonAbstractTest {
    /**
     * Constructs a test.
     */
    public GridSessionCollisionSpiSelfTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        cfg.setCollisionSpi(new GridSessionCollisionSpi());

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testCollisionSessionAttribute() throws Exception {
        Ignite ignite = G.grid(getTestGridName());

        ignite.compute().execute(GridSessionTestTask.class, null);

        info("Executed session collision test task.");
    }

    /**
     * Test task.
     */
    @GridComputeTaskSessionFullSupport
    private static class GridSessionTestTask extends GridComputeTaskSplitAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override protected Collection<GridComputeJobAdapter> split(int gridSize, Object arg) throws GridException {
            Collection<GridComputeJobAdapter> jobs = new ArrayList<>(gridSize);

            for (int i = 0; i < gridSize; i++) {
                jobs.add(new GridComputeJobAdapter() {
                    /** */
                    @GridTaskSessionResource
                    private GridComputeTaskSession taskSes;

                    /** */
                    @GridJobContextResource
                    private GridComputeJobContext jobCtx;

                    /** */
                    @GridLoggerResource
                    private GridLogger log;

                    /** {@inheritDoc} */
                    @Override public Serializable execute() {
                        GridUuid jobId = jobCtx.getJobId();

                        String attr = (String)taskSes.getAttribute(jobId);

                        assert attr != null : "Attribute is null.";
                        assert attr.equals("test-" + jobId) : "Attribute has incorrect value: " + attr;

                        if (log.isInfoEnabled())
                            log.info("Executing job: " + jobId);

                        return null;
                    }
                });
            }

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
            // Nothing to reduce.
            return null;
        }
    }

    /**
     * Test collision spi.
     */
    private static class GridSessionCollisionSpi extends GridFifoQueueCollisionSpi {
        /** */
        @GridLoggerResource
        private GridLogger log;

        /** {@inheritDoc} */
        @Override public void onCollision(GridCollisionContext ctx) {
            Collection<GridCollisionJobContext> waitJobs = ctx.waitingJobs();

            for (GridCollisionJobContext job : waitJobs) {
                GridUuid jobId = job.getJobContext().getJobId();

                try {
                    job.getTaskSession().setAttribute(jobId, "test-" + jobId);

                    if (log.isInfoEnabled())
                        log.info("Set session attribute for job: " + jobId);
                }
                catch (GridException e) {
                    log.error("Failed to set session attribute: " + job, e);
                }

                job.activate();
            }
        }
    }
}
