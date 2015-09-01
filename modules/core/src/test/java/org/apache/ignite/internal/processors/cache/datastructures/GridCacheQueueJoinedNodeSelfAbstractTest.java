/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.datastructures;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.jetbrains.annotations.Nullable;

/**
 * Test that joining node is able to take items from queue.
 * See GG-2311 for more information.
 */
public abstract class GridCacheQueueJoinedNodeSelfAbstractTest extends IgniteCollectionAbstractTest {
    /** */
    protected static final int GRID_CNT = 3;

    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    protected static final int ITEMS_CNT = 300;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTakeFromJoined() throws Exception {
        String queueName = UUID.randomUUID().toString();

        IgniteQueue<Integer> queue = grid(0).queue(queueName, 0, config(true));

        assertNotNull(queue);

        assertTrue(queue.isEmpty());

        PutJob putJob = new PutJob(queueName);

        IgniteCompute comp = compute(grid(0).cluster().forLocal()).withAsync();

        comp.run(putJob);

        IgniteFuture<?> fut = comp.future();

        Collection<IgniteFuture<?>> futs = new ArrayList<>(GRID_CNT - 1);

        Collection<TakeJob> jobs = new ArrayList<>(GRID_CNT - 1);

        int itemsLeft = ITEMS_CNT;

        for (int i = 1; i < GRID_CNT; i++) {
            int cnt = ITEMS_CNT / (GRID_CNT - 1);

            TakeJob job = new TakeJob(queueName, cnt, 10);

            jobs.add(job);

            comp = compute(grid(i).cluster().forLocal()).withAsync();

            comp.call(job);

            futs.add(comp.future());

            itemsLeft -= cnt;
        }

        assertEquals("Not all items will be polled", 0, itemsLeft);

        // Wait for half of items to be polled.
        for (TakeJob job : jobs)
            job.awaitItems();

        log.info("Start one more grid.");

        Ignite joined = startGrid(GRID_CNT);

        // We expect at least one item to be taken.
        TakeJob joinedJob = new TakeJob(queueName, 1, 1);

        jobs.add(joinedJob);

        Integer polled = forLocal(joined).call(joinedJob);

        assertNotNull("Joined node should poll item", polled);

        info(">>> Joined node polled " + polled);

        for (IgniteFuture<?> f : futs)
            f.cancel();

        putJob.stop(true);

        fut.get();

        for (TakeJob job : jobs)
            job.awaitDone();
    }

    /**
     * Test job putting data to queue.
     */
    protected class PutJob implements IgniteRunnable {
        /** */
        @GridToStringExclude
        @IgniteInstanceResource
        private Ignite ignite;

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
            assertNotNull(ignite);

            log.info("Running job [node=" + ignite.cluster().localNode().id() +
                ", job=" + getClass().getSimpleName() + "]");

            try {
                IgniteQueue<Integer> queue = ignite.queue(queueName, 0, null);

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
    protected class TakeJob implements IgniteCallable<Integer> {
        /** */
        @GridToStringExclude
        @IgniteInstanceResource
        private Ignite ignite;

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
         * @throws org.apache.ignite.internal.IgniteInterruptedCheckedException If interrupted.
         */
        private void awaitItems() throws IgniteInterruptedCheckedException {
            U.await(takeLatch);
        }

        /**
         * Awaits for a given count of items to be taken.
         *
         * @throws org.apache.ignite.internal.IgniteInterruptedCheckedException If interrupted.
         */
        private void awaitDone() throws IgniteInterruptedCheckedException {
            U.await(doneLatch);
        }

        /** {@inheritDoc} */
        @Nullable @Override public Integer call() {
            assertNotNull(ignite);

            log.info("Running job [node=" + ignite.cluster().localNode().id() +
                ", job=" + getClass().getSimpleName() + "]");

            Integer lastPolled = null;

            try {
                IgniteQueue<Integer> queue = ignite.queue(queueName, 0, null);

                assertNotNull(queue);

                for (int i = 0; i < maxTakeCnt; i++) {
                    lastPolled = queue.take();

                    takeLatch.countDown();
                }
            }
            catch (IgniteException e) {
                if (e.getCause() instanceof IgniteInterruptedCheckedException || e.getCause() instanceof InterruptedException)
                    log.info("Cancelling job due to interruption: " + e.getMessage());
                else
                    fail("Unexpected error: " + e);
            }
            catch (Exception e) {
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