/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.*;
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
        Grid grid = G.grid(getTestGridName());

        assert grid != null;
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCollisionSpi(new TestCollisionSpi());

        return cfg;
    }

    /**
     * @throws Exception If test failed.
     */
    public void testCollisionJobContext() throws Exception {
        G.grid(getTestGridName()).compute().execute(new GridTestTask(), "some-arg").get();
    }

    /** */
    @SuppressWarnings( {"PublicInnerClass"})
    @GridSpiMultipleInstancesSupport(true)
    public static class TestCollisionSpi extends GridSpiAdapter implements GridCollisionSpi {
        /** Grid logger. */
        @GridLoggerResource private GridLogger log;

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
        @Override public void spiStart(String gridName) throws GridSpiException {
            // Start SPI start stopwatch.
            startStopwatch();

            // Ack start.
            if (log.isInfoEnabled()) {
                log.info(startInfo());
            }
        }

        /** {@inheritDoc} */
        @Override public void spiStop() throws GridSpiException {
            // Ack stop.
            if (log.isInfoEnabled()) {
                log.info(stopInfo());
            }
        }

        /** {@inheritDoc} */
        @Override public void setExternalCollisionListener(GridCollisionExternalListener lsnr) {
            // No-op.
        }
    }
}
