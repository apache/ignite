/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.*;
import org.gridgain.grid.*;
import org.gridgain.grid.spi.collision.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;
import java.util.*;

/**
 * Collision job context test.
 */
@GridCommonTest(group = "Kernal Self")
public class GridCollisionJobsContextSelfTest extends GridCommonAbstractTest {
    /** */
    public GridCollisionJobsContextSelfTest() {
        super(/*start grid*/true);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        Ignite ignite = G.grid(getTestGridName());

        assert ignite != null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCollisionSpi(new TestCollisionSpi());

        return cfg;
    }

    /**
     * @throws Exception If test failed.
     */
    public void testCollisionJobContext() throws Exception {
        G.grid(getTestGridName()).compute().execute(new GridTestTask(), "some-arg");
    }

    /** */
    @SuppressWarnings( {"PublicInnerClass"})
    @IgniteSpiMultipleInstancesSupport(true)
    public static class TestCollisionSpi extends IgniteSpiAdapter implements GridCollisionSpi {
        /** Grid logger. */
        @IgniteLoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public void onCollision(GridCollisionContext ctx) {
            Collection<GridCollisionJobContext> activeJobs = ctx.activeJobs();
            Collection<GridCollisionJobContext> waitJobs = ctx.waitingJobs();

            assert waitJobs != null;
            assert activeJobs != null;


            for (GridCollisionJobContext job : waitJobs) {
                assert job.getJob() != null;
                assert job.getJobContext() != null;
                assert job.getTaskSession() != null;

                assert job.getJob() instanceof GridTestJob : job.getJob();

                job.activate();
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
        @Override public void setExternalCollisionListener(GridCollisionExternalListener lsnr) {
            // No-op.
        }
    }
}
