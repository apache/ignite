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

import org.apache.ignite.*;
import org.apache.ignite.cache.datastructures.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.testframework.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Queue multi node tests.
 */
public abstract class GridCacheQueueRotativeMultiNodeAbstractTest extends IgniteCollectionAbstractTest {
    /** */
    protected static final int GRID_CNT = 4;

    /** */
    protected static final int RETRIES = 133;

    /** */
    private static final int QUEUE_CAPACITY = 100_000;

    /** */
    private static CountDownLatch lthTake;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        for (int i = 0; i < GRID_CNT; i++)
            startGrid(i);

        assert G.allGrids().size() == GRID_CNT : G.allGrids().size();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        assert G.allGrids().isEmpty();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMarshaller(new IgniteOptimizedMarshaller(false));

        return cfg;
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPutRotativeNodes() throws Exception {
        String queueName = UUID.randomUUID().toString();

        IgniteQueue<Integer> queue =
            grid(0).queue(queueName, collocatedCollectionConfiguration(), QUEUE_CAPACITY, true);

        assertTrue(queue.isEmpty());

        // Start and stop GRID_CNT*2 new nodes.
        for (int i = GRID_CNT; i < GRID_CNT * 3; i++) {
            startGrid(i);

            forLocal(grid(i)).call(new PutJob(queueName, collocatedCollectionConfiguration(), RETRIES));

            // last node must be alive.
            if (i < (GRID_CNT * 3) - 1)
                stopGrid(i);
        }

        queue = grid((GRID_CNT * 3) - 1).queue(queueName, null, 0, false);

        assertEquals(RETRIES * GRID_CNT * 2, queue.size());
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPutTakeRotativeNodes() throws Exception {
        String queueName = UUID.randomUUID().toString();

        IgniteQueue<Integer> queue =
                grid(0).queue(queueName, collocatedCollectionConfiguration(), QUEUE_CAPACITY, true);

        assertTrue(queue.isEmpty());

        // Start and stop GRID_CNT*2 new nodes.
        for (int i = GRID_CNT; i < GRID_CNT * 3; i++) {
            startGrid(i);

            forLocal(grid(i)).call(new PutTakeJob(queueName, collocatedCollectionConfiguration(), RETRIES));

            // last node must be alive.
            if (i < (GRID_CNT * 3) - 1)
                stopGrid(i);
        }

        queue = grid((GRID_CNT * 3) - 1).queue(queueName, collocatedCollectionConfiguration(), QUEUE_CAPACITY, true);

        assertEquals(0, queue.size());
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testTakeRemoveRotativeNodes() throws Exception {
        lthTake = new CountDownLatch(1);

        final String queueName = UUID.randomUUID().toString();

        final IgniteQueue<Integer> queue =
                grid(0).queue(queueName, collocatedCollectionConfiguration(), QUEUE_CAPACITY, true);

        assertTrue(queue.isEmpty());

        Thread th = new Thread(new Runnable() {
            @Override public void run() {
                try {
                    assert grid(1).compute().call(new TakeJob(queueName, collocatedCollectionConfiguration()));
                }
                catch (IgniteException e) {
                    error(e.getMessage(), e);
                }
            }
        });

        th.start();

        assert lthTake.await(1, TimeUnit.MINUTES) : "Timeout happened.";

        assertTrue(grid(2).compute().call(new RemoveQueueJob(queueName)));

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                queue.poll();

                return null;
            }
        }, DataStructureRemovedException.class, null);

        info("Queue was removed: " + queue);

        th.join();
    }

    /**
     * Test job putting data to queue.
     */
    protected static class PutJob implements IgniteCallable<Integer> {
        /** */
        @GridToStringExclude
        @IgniteInstanceResource
        private Ignite ignite;

        /** Queue name. */
        private final String queueName;

        /** */
        private final int retries;

        /** */
        private final IgniteCollectionConfiguration colCfg;

        /**
         * @param queueName Queue name.
         * @param colCfg Collection configuration.
         * @param retries  Number of operations.
         */
        PutJob(String queueName, IgniteCollectionConfiguration colCfg, int retries) {
            this.queueName = queueName;
            this.colCfg = colCfg;
            this.retries = retries;
        }

        /** {@inheritDoc} */
        @Override public Integer call() throws IgniteCheckedException {
            assertNotNull(ignite);

            ignite.log().info("Running job [node=" + ignite.cluster().localNode().id() + ", job=" + this + "]");

            IgniteQueue<Integer> queue = ignite.queue(queueName, colCfg, QUEUE_CAPACITY, true);

            assertNotNull(queue);

            for (int i = 0; i < retries; i++)
                queue.put(i);

            return queue.size();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(PutJob.class, this);
        }
    }

    /**
     * Test job putting and taking data to/from queue.
     */
    protected static class PutTakeJob implements IgniteCallable<Integer> {
        /** */
        @GridToStringExclude
        @IgniteInstanceResource
        private Ignite ignite;

        /** Queue name. */
        private final String queueName;

        /** */
        private final int retries;

        /** */
        private final IgniteCollectionConfiguration colCfg;

        /**
         * @param queueName Queue name.
         * @param retries  Number of operations.
         */
        PutTakeJob(String queueName, IgniteCollectionConfiguration colCfg, int retries) {
            this.queueName = queueName;
            this.colCfg = colCfg;
            this.retries = retries;
        }

        /** {@inheritDoc} */
        @Override public Integer call() throws IgniteCheckedException {
            assertNotNull(ignite);

            ignite.log().info("Running job [node=" + ignite.cluster().localNode().id() + ", job=" + this + ']');

            IgniteQueue<Integer> queue = ignite.queue(queueName, colCfg, QUEUE_CAPACITY, true);

            assertNotNull(queue);

            for (int i = 0; i < retries; i++) {
                queue.put(i);

                assertNotNull(queue.peek());

                assertNotNull(queue.element());

                assertNotNull(queue.take());
            }

            return queue.size();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(PutTakeJob.class, this);
        }
    }

    /**
     * Test job taking data from queue.
     */
    protected static class TakeJob implements IgniteCallable<Boolean> {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** Queue name. */
        private final String queueName;

        /** */
        private final IgniteCollectionConfiguration colCfg;

        /**
         * @param queueName Queue name.
         * @param colCfg Collection configuration.
         */
        TakeJob(String queueName, IgniteCollectionConfiguration colCfg) {
            this.queueName = queueName;
            this.colCfg = colCfg;
        }

        /** {@inheritDoc} */
        @Override public Boolean call() throws IgniteCheckedException {
            assertNotNull(ignite);

            ignite.log().info("Running job [node=" + ignite.cluster().localNode().id() + ", job=" + this + ']');

            IgniteQueue<Integer> queue = ignite.queue(queueName, colCfg, QUEUE_CAPACITY, true);

            assertNotNull(queue);

            try {
                // Queue can be removed.
                lthTake.countDown();

                queue.take();
            }
            catch (IgniteException e) {
                ignite.log().info("Caught expected exception: " + e.getMessage());
            }

            return queue.removed();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TakeJob.class, this);
        }
    }

    /**
     * Job removing queue.
     */
    protected static class RemoveQueueJob implements IgniteCallable<Boolean> {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** Queue name. */
        private final String queueName;

        /**
         * @param queueName Queue name.
         */
        RemoveQueueJob(String queueName) {
            this.queueName = queueName;
        }

        /** {@inheritDoc} */
        @Override public Boolean call() throws IgniteCheckedException {
            assertNotNull(ignite);

            ignite.log().info("Running job [node=" + ignite.cluster().localNode().id() + ", job=" + this + "]");

            IgniteQueue<Integer> queue = ignite.queue(queueName, null, 0, false);

            assert queue != null;

            assert queue.capacity() == QUEUE_CAPACITY;

            queue.close();

            return true;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RemoveQueueJob.class, this);
        }
    }
}
