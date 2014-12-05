/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.session;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
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
    @ComputeTaskSessionFullSupport
    private static class GridSessionTestTask extends ComputeTaskSplitAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override protected Collection<ComputeJobAdapter> split(int gridSize, Object arg) throws GridException {
            Collection<ComputeJobAdapter> jobs = new ArrayList<>(gridSize);

            for (int i = 0; i < gridSize; i++) {
                jobs.add(new ComputeJobAdapter() {
                    /** */
                    @IgniteTaskSessionResource
                    private ComputeTaskSession taskSes;

                    /** */
                    @IgniteJobContextResource
                    private ComputeJobContext jobCtx;

                    /** */
                    @IgniteLoggerResource
                    private IgniteLogger log;

                    /** {@inheritDoc} */
                    @Override public Serializable execute() {
                        IgniteUuid jobId = jobCtx.getJobId();

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
        @Override public Object reduce(List<ComputeJobResult> results) throws GridException {
            // Nothing to reduce.
            return null;
        }
    }

    /**
     * Test collision spi.
     */
    private static class GridSessionCollisionSpi extends FifoQueueCollisionSpi {
        /** */
        @IgniteLoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public void onCollision(CollisionContext ctx) {
            Collection<CollisionJobContext> waitJobs = ctx.waitingJobs();

            for (CollisionJobContext job : waitJobs) {
                IgniteUuid jobId = job.getJobContext().getJobId();

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
