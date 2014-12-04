/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.spi.collision.fifoqueue.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static java.util.concurrent.TimeUnit.*;
import static org.gridgain.grid.events.GridEventType.*;

/**
 * Test that rejected job is not failed over.
 */
public class GridTaskJobRejectSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(1);
        startGrid(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopGrid(1);
        stopGrid(2);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridFifoQueueCollisionSpi collision = new GridFifoQueueCollisionSpi();

        collision.setParallelJobsNumber(1);

        cfg.setCollisionSpi(collision);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testReject() throws Exception {
        grid(1).events().localListen(new IgnitePredicate<GridEvent>() {
            @Override public boolean apply(GridEvent evt) {
                X.println("Task event: " + evt);

                return true;
            }
        }, EVTS_TASK_EXECUTION);

        grid(1).events().localListen(new IgnitePredicate<GridEvent>() {
            @Override public boolean apply(GridEvent evt) {
                X.println("Job event: " + evt);

                return true;
            }
        }, EVTS_JOB_EXECUTION);

        final CountDownLatch startedLatch = new CountDownLatch(1);

        grid(1).events().localListen(new IgnitePredicate<GridEvent>() {
            @Override public boolean apply(GridEvent evt) {
                startedLatch.countDown();

                return true;
            }
        }, EVT_JOB_STARTED);

        final AtomicInteger failedOver = new AtomicInteger(0);

        grid(1).events().localListen(new IgnitePredicate<GridEvent>() {
            @Override public boolean apply(GridEvent evt) {
                failedOver.incrementAndGet();

                return true;
            }
        }, EVT_JOB_FAILED_OVER);

        final CountDownLatch finishedLatch = new CountDownLatch(1);

        grid(1).events().localListen(new IgnitePredicate<GridEvent>() {
            @Override public boolean apply(GridEvent evt) {
                finishedLatch.countDown();

                return true;
            }
        }, EVT_TASK_FINISHED, EVT_TASK_FAILED);

        final ClusterNode node = grid(1).localNode();

        GridCompute comp = grid(1).compute().enableAsync();

        comp.execute(new GridComputeTaskAdapter<Void, Void>() {
            @Override public Map<? extends GridComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
                @Nullable Void arg) {
                return F.asMap(new SleepJob(), node, new SleepJob(), node);
            }

            /** {@inheritDoc} */
            @Nullable @Override public Void reduce(List<GridComputeJobResult> results) throws GridException {
                return null;
            }
        }, null);

        GridComputeTaskFuture<?> fut = comp.future();

        assert startedLatch.await(2, SECONDS);

        fut.cancel();

        assert finishedLatch.await(2, SECONDS);

        assert failedOver.get() == 0;
    }

    /**
     * Sleeping job.
     */
    private static final class SleepJob extends GridComputeJobAdapter {
        /** {@inheritDoc} */
        @Override public Object execute() {
            try {
                Thread.sleep(10000);
            }
            catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }

            return null;
        }
    }
}
