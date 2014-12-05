/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.*;
import org.gridgain.grid.*;
import org.apache.ignite.spi.collision.*;
import org.gridgain.grid.spi.discovery.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

/**
 * Cancelled jobs metrics self test.
 */
@GridCommonTest(group = "Kernal Self")
public class GridCancelledJobsMetricsSelfTest extends GridCommonAbstractTest {

    /** */
    private static GridCancelCollisionSpi colSpi = new GridCancelCollisionSpi();

    /** */
    public GridCancelledJobsMetricsSelfTest() {
        super(true);
    }


    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        cfg.setCollisionSpi(colSpi);

        DiscoverySpi discoSpi = cfg.getDiscoverySpi();

        assert discoSpi instanceof GridTcpDiscoverySpi;

        ((GridTcpDiscoverySpi)discoSpi).setHeartbeatFrequency(500);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testCancelledJobs() throws Exception {
        final Ignite ignite = G.grid(getTestGridName());

        Collection<ComputeTaskFuture<?>> futs = new ArrayList<>();

        IgniteCompute comp = ignite.compute().enableAsync();

        for (int i = 1; i <= 10; i++) {
            comp.execute(CancelledTask.class, null);

            futs.add(comp.future());
        }

        // Wait to be sure that metrics were updated.
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return ignite.cluster().localNode().metrics().getTotalCancelledJobs() > 0;
            }
        }, 5000);

        colSpi.externalCollision();

        for (ComputeTaskFuture<?> fut : futs) {
            try {
                fut.get();

                assert false : "Job was not interrupted.";
            }
            catch (GridException e) {
                if (e.hasCause(InterruptedException.class))
                    throw new GridException("Test run has been interrupted.", e);

                info("Caught expected exception: " + e.getMessage());
            }
        }

        // Job was cancelled and now we need to calculate metrics.
        int totalCancelledJobs = ignite.cluster().localNode().metrics().getTotalCancelledJobs();

        assert totalCancelledJobs == 10 : "Metrics were not updated. Expected 10 got " + totalCancelledJobs;
    }

    /**
     *
     */
    private static final class CancelledTask extends ComputeTaskSplitAdapter<String, Object> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, String arg) {
            return Arrays.asList(new GridCancelledJob());
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) {
            assert results.get(0).isCancelled() : "Wrong job result status.";

            return null;
        }
    }

    /**
     *
     */
    private static final class GridCancelledJob extends ComputeJobAdapter {
        /** {@inheritDoc} */
        @Override public String execute() throws GridException {
            X.println("Executing job.");

            try {
                Thread.sleep(Long.MAX_VALUE);
            }
            catch (InterruptedException ignored) {
                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e1) {
                    throw new GridException("Unexpected exception: ", e1);
                }

                throw new GridException("Job got interrupted while waiting for cancellation.");
            }
            finally {
                X.println("Finished job.");
            }

            return null;
        }
    }

    /**
     *
     */
    @IgniteSpiMultipleInstancesSupport(true)
    private static class GridCancelCollisionSpi extends IgniteSpiAdapter
        implements CollisionSpi {
        /** */
        @IgniteLoggerResource
        private IgniteLogger log;

        /** */
        private CollisionExternalListener lsnr;

        /** {@inheritDoc} */
        @Override public void onCollision(CollisionContext ctx) {
            Collection<CollisionJobContext> activeJobs = ctx.activeJobs();
            Collection<CollisionJobContext> waitJobs = ctx.waitingJobs();

            for (CollisionJobContext job : waitJobs)
                job.activate();

            for (CollisionJobContext job : activeJobs) {
                log.info("Cancelling job : " + job.getJob());

                job.cancel();
            }
        }

        /** {@inheritDoc} */
        @Override public void spiStart(String gridName) throws IgniteSpiException {
            // Start SPI start stopwatch.
            startStopwatch();

            // Ack start.
            if (log.isInfoEnabled())
                log.info(startInfo());
        }

        /** {@inheritDoc} */
        @Override public void spiStop() throws IgniteSpiException {
            // Ack stop.
            if (log.isInfoEnabled())
                log.info(stopInfo());
        }

        /** {@inheritDoc} */
        @Override public void setExternalCollisionListener(CollisionExternalListener lsnr) {
            this.lsnr = lsnr;
        }

        /** */
        public void externalCollision() {
            CollisionExternalListener tmp = lsnr;

            if (tmp != null)
                tmp.onExternalCollision();
        }
    }
}
