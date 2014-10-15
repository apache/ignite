/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Test that joining node is able to take items from queue.
 * See GG-2311 for more information.
 */
public abstract class GridCacheQueueJoinedNodeSelfAbstractTest extends GridCommonAbstractTest {
    /** */
    protected static final int GRID_CNT = 3;

    /** */
    protected static GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    protected static final int ITEMS_CNT = 300;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        GridTcpDiscoverySpi spi = new GridTcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTakeFromJoined() throws Exception {
        String queueName = UUID.randomUUID().toString();

        GridCacheQueue<Integer> queue = grid(0).cache(null).dataStructures()
            .queue(queueName, 0, true, true);

        assertNotNull(queue);

        assertTrue(queue.isEmpty());

        PutJob putJob = new PutJob(queueName);

        GridFuture<?> fut = grid(0).forLocal().compute().run(putJob);

        Collection<GridFuture<?>> futs = new ArrayList<>(GRID_CNT - 1);

        Collection<TakeJob> jobs = new ArrayList<>(GRID_CNT - 1);

        int itemsLeft = ITEMS_CNT;

        for (int i = 1; i < GRID_CNT; i++) {
            int cnt = ITEMS_CNT / (GRID_CNT - 1);

            TakeJob job = new TakeJob(queueName, cnt, 10);

            jobs.add(job);

            futs.add(grid(i).forLocal().compute().call(job));

            itemsLeft -= cnt;
        }

        assertEquals("Not all items will be polled", 0, itemsLeft);

        // Wait for half of items to be polled.
        for (TakeJob job : jobs)
            job.awaitItems();

        log.info("Start one more grid.");

        Grid joined = startGrid(GRID_CNT);

        // We expect at least one item to be taken.
        TakeJob joinedJob = new TakeJob(queueName, 1, 1);

        jobs.add(joinedJob);

        Integer polled = joined.forLocal().compute().call(joinedJob).get();

        assertNotNull("Joined node should poll item", polled);

        info(">>> Joined node polled " + polled);

        for (GridFuture<?> f : futs)
            f.cancel();

        putJob.stop(true);

        fut.get();

        for (TakeJob job : jobs)
            job.awaitDone();
    }

    /**
     * Test job putting data to queue.
     */
    protected class PutJob implements GridRunnable {
        /** */
        @GridToStringExclude
        @GridInstanceResource
        private Grid grid;

        /** Queue name. */
        private final String queueName;

        /** */
        private volatile boolean stop;

        /**
         * @param queueName Queue name.
         */
        PutJob(String queueName) {
            this.queueName = queueName;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            assertNotNull(grid);

            log.info("Running job [node=" + grid.localNode().id() + ", job=" + getClass().getSimpleName() + "]");

            try {
                GridCacheQueue<Integer> queue = grid.cache(null).dataStructures().queue(queueName, 0, true, false);

                assertNotNull(queue);

                int i = 0;

                while (!stop)
                    queue.add(i++);
            }
            catch (Exception e) {
                error("Failed to put value to the queue", e);

                fail("Unexpected exception: " + e);
            }

            log.info("PutJob finished");
        }

        /**
         * @param stop Stop flag.
         */
        void stop(boolean stop) {
            this.stop = stop;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(PutJob.class, this);
        }
    }

    /**
     * Test job putting data to queue.
     */
    protected class TakeJob implements GridCallable<Integer> {
        /** */
        @GridToStringExclude
        @GridInstanceResource
        private Grid grid;

        /** Queue name. */
        private final String queueName;

        /** Maximum count of items to be taken from queue. */
        private final int maxTakeCnt;

        /** Latch for waiting for items. */
        private final CountDownLatch takeLatch;

        /** Latch for waiting for job completion. */
        private final CountDownLatch doneLatch;

        /**
         * @param queueName Queue name.
         * @param maxTakeCnt Maximum count of items to be taken from queue.
         * @param waitCnt Count of items to
         */
        TakeJob(String queueName, int maxTakeCnt, int waitCnt) {
            this.queueName = queueName;
            this.maxTakeCnt = maxTakeCnt;

            takeLatch = new CountDownLatch(waitCnt);

            doneLatch = new CountDownLatch(1);
        }

        /**
         * Awaits for a given count of items to be taken.
         *
         * @throws GridInterruptedException If interrupted.
         */
        private void awaitItems() throws GridInterruptedException {
            U.await(takeLatch);
        }

        /**
         * Awaits for a given count of items to be taken.
         *
         * @throws GridInterruptedException If interrupted.
         */
        private void awaitDone() throws GridInterruptedException {
            U.await(doneLatch);
        }

        /** {@inheritDoc} */
        @Nullable @Override public Integer call() {
            assertNotNull(grid);

            log.info("Running job [node=" + grid.localNode().id() + ", job=" + getClass().getSimpleName() + "]");

            Integer lastPolled = null;

            try {
                GridCacheQueue<Integer> queue = grid.cache(null).dataStructures().queue(queueName, 0, true, false);

                assertNotNull(queue);

                for (int i = 0; i < maxTakeCnt; i++) {
                    lastPolled = queue.take();

                    takeLatch.countDown();
                }
            }
            catch (GridRuntimeException e) {
                if (e.getCause() instanceof GridInterruptedException || e.getCause() instanceof InterruptedException)
                    log.info("Cancelling job due to interruption: " + e.getMessage());
                else
                    fail("Unexpected error: " + e);
            }
            catch (GridException e) {
                error("Failed to get value from the queue", e);
            }
            finally {
                doneLatch.countDown();
            }

            log.info("TakeJob finished, last polled value: " + lastPolled);

            return lastPolled;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TakeJob.class, this);
        }
    }
}
