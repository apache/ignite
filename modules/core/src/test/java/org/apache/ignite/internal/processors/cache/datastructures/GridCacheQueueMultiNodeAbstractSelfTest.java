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
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Queue multi node tests.
 */
public abstract class GridCacheQueueMultiNodeAbstractSelfTest extends IgniteCollectionAbstractTest {
    /** */
    private static final int GRID_CNT = 4;

    /** */
    private static final int RETRIES = 20;

    /** */
    protected static final int QUEUE_CAPACITY = 100_000;

    /** */
    private static AtomicInteger cntr = new AtomicInteger();

    /** */
    private static final int ITEMS_CNT = 50;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("BusyWait")
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        boolean success = false;

        for (int i = 0; i < 5; i++) {
            success = true;

            for (Ignite g : G.allGrids()) {
                if (g.cluster().nodes().size() != GRID_CNT) {
                    info("Grid has incorrect nodes count [gridName=" + g.name() +
                        ", nodesCnt=" + g.cluster().nodes().size() + ']');

                    success = false;

                    break;
                }
            }

            if (!success && i != 4)
                Thread.sleep(1000);
        }

        assert success;

        info("Topology is consistent.");
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPublicThreadPoolSize(RETRIES * 2);

        cfg.setSystemThreadPoolSize(RETRIES * 2);

        cfg.setConnectorConfiguration(null);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPut() throws Exception {
        String queueName = UUID.randomUUID().toString();

        IgniteQueue<Integer> queue = grid(0).queue(queueName, QUEUE_CAPACITY, config(false));

        assertTrue(queue.isEmpty());

        grid(0).compute().broadcast(new PutJob(queueName, RETRIES));

        assertEquals(GRID_CNT * RETRIES, queue.size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutPollCollocated() throws Exception {
        try {
            final String queueName = UUID.randomUUID().toString();

            info("Queue name: " + queueName);

            grid(0).queue(queueName, 5, config(true));

            final CountDownLatch latch = new CountDownLatch(1);

            final Ignite g = startGrid(GRID_CNT + 1);

            IgniteInternalFuture<Object> fut1 = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    info(">>> Executing put callable [node=" + g.cluster().localNode().id() +
                        ", thread=" + Thread.currentThread().getName() + ']');

                    IgniteQueue<Integer> q = g.queue(queueName, 5, config(true));

                    assertTrue(q.isEmpty());

                    for (int i = 0; i < ITEMS_CNT; i++) {
                        if (i == q.capacity()) {
                            info(">>> Opening latch...");

                            latch.countDown();
                        }

                        put(q, i);
                    }

                    info(">>> Finished put callable on node: " + g.cluster().localNode().id());

                    return null;
                }
            });

            latch.await();

            final Ignite g1 = startGrid(GRID_CNT + 2);

            IgniteInternalFuture<Object> fut2 = GridTestUtils.runAsync(new Callable<Object>() {
                @SuppressWarnings("BusyWait")
                @Override public Object call() throws Exception {
                    try {
                        info(">>> Executing poll callable [node=" + g1.cluster().localNode().id() +
                            ", thread=" + Thread.currentThread().getName() + ']');

                        IgniteQueue<Integer> q = g1.queue(queueName, 5, config(true));

                        int cnt = 0;
                        int nullCnt = 0;

                        do {
                            Integer i = q.poll();

                            info("Polled value: " + i);

                            if (i != null) {
                                cnt++;

                                nullCnt = 0;
                            }
                            else {
                                if (nullCnt == 3)
                                    throw new Exception("Failed to poll non-null value within 3 attempts.");

                                nullCnt++;

                                Thread.sleep(1000);
                            }
                        }
                        while (cnt < ITEMS_CNT);

                        info("Finished poll callable on node: " + g1.cluster().localNode().id());

                        return null;
                    }
                    finally {
                        info("Poll callable finished.");
                    }
                }
            });

            fut1.get();
            fut2.get();

            grid(0).queue(queueName, 0, null).close();
        }
        finally {
            stopGrid(GRID_CNT + 1);
            stopGrid(GRID_CNT + 2);

            checkTopology(GRID_CNT);

            awaitPartitionMapExchange();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void testAddAll() throws Exception {
        try {
            String queueName = UUID.randomUUID().toString();

            info("Queue name: " + queueName);

            IgniteQueue<Integer> queue = grid(0).queue(queueName, QUEUE_CAPACITY, config(false));

            assertTrue(queue.isEmpty());

            grid(0).compute().call(new AddAllJob(queueName, RETRIES));

            assertEquals(GRID_CNT * RETRIES, queue.size());

            queue.clear(5);

            assertEquals(0, queue.size());

            queue.close();
        }
        catch (Throwable t) {
            error("Failure in test: " + t);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGet() throws Exception {
        String queueName = UUID.randomUUID().toString();

        info("Queue name: " + queueName);

        IgniteQueue<String> queue = grid(0).queue(queueName, QUEUE_CAPACITY, config(false));

        assertTrue(queue.isEmpty());

        String val = UUID.randomUUID().toString();

        queue.put(val);

        grid(0).compute().call(new GetJob(queueName, config(false), RETRIES, val));

        assertEquals(1, queue.size());

        queue.close();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTake() throws Exception {
        String queueName = UUID.randomUUID().toString();

        info("Queue name: " + queueName);

        IgniteQueue<Integer> queue = grid(0).queue(queueName, QUEUE_CAPACITY, config(false));

        assertTrue(queue.isEmpty());

        grid(0).compute().broadcast(new PutTakeJob(queueName, RETRIES));

        assertEquals(0, queue.size());

        queue.close();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAddMultinode() throws Exception {
        testAddMultinode(true);

        testAddMultinode(false);
    }

    /**
     * @param collocated Collocation flag.
     * @throws Exception If failed.
     */
    private void testAddMultinode(final boolean collocated) throws Exception {
        final String queueName = UUID.randomUUID().toString();

        info("Queue name: " + queueName + ", collocated: " + collocated);

        try {
            Collection<IgniteInternalFuture> futs = new ArrayList<>();

            final int THREADS_PER_NODE = 3;
            final int ITEMS_PER_THREAD = 1000;

            for (int i = 0; i < GRID_CNT; i++) {
                final int idx = i;

                futs.add(GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        CollectionConfiguration colCfg = config(collocated);

                        IgniteQueue<Integer> queue = grid(idx).queue(queueName, 0, colCfg);

                        for (int i = 0; i < ITEMS_PER_THREAD; i++)
                            assertTrue(queue.add(i));

                        return null;
                    }
                }, THREADS_PER_NODE, "testPutMultiNode"));
            }

            for (IgniteInternalFuture fut : futs)
                fut.get();

            IgniteQueue<Integer> queue = grid(0).queue(queueName, 0, null);

            assertEquals(THREADS_PER_NODE * ITEMS_PER_THREAD * GRID_CNT, queue.size());

            int[] items = new int[ITEMS_PER_THREAD];

            Integer item;

            while ((item = queue.poll()) != null)
                items[item]++;

            for (int i = 0; i < ITEMS_PER_THREAD; i++)
                assertEquals(THREADS_PER_NODE * GRID_CNT, items[i]);
        }
        finally {
            grid(0).queue(queueName, 0, null).close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAddPollMultinode() throws Exception {
        testAddPollMultinode(true);

        testAddPollMultinode(false);
    }

    /**
     * @param collocated Collocation flag.
     * @throws Exception If failed.
     */
    private void testAddPollMultinode(final boolean collocated) throws Exception {
        final String queueName = UUID.randomUUID().toString();

        info("Queue name: " + queueName + ", collocated: " + collocated);

        try {
            Collection<IgniteInternalFuture> putFuts = new ArrayList<>();
            Collection<IgniteInternalFuture> pollFuts = new ArrayList<>();

            final int PUT_THREADS_PER_NODE = 3;
            final int POLL_THREADS_PER_NODE = 2;
            final int ITEMS_PER_THREAD = 1000;

            final AtomicBoolean stopPoll = new AtomicBoolean();

            Collection<int[]> pollData = new ArrayList<>();

            for (int i = 0; i < GRID_CNT; i++) {
                final int idx = i;

                putFuts.add(GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        CollectionConfiguration colCfg = config(collocated);

                        IgniteQueue<Integer> queue = grid(idx).queue(queueName, 0, colCfg);

                        for (int i = 0; i < ITEMS_PER_THREAD; i++)
                            assertTrue(queue.add(i));

                        return null;
                    }
                }, PUT_THREADS_PER_NODE, "testAddPollMultinode"));

                for (int j = 0; j < POLL_THREADS_PER_NODE; j++) {
                    final int[] items = new int[ITEMS_PER_THREAD];

                    pollData.add(items);

                    pollFuts.add(GridTestUtils.runAsync(new Callable<Void>() {
                        @Override public Void call() throws Exception {
                            CollectionConfiguration colCfg = config(collocated);

                            IgniteQueue<Integer> queue = grid(idx).queue(queueName, 0, colCfg);

                            while (!stopPoll.get()) {
                                Integer val = queue.poll();

                                if (val != null)
                                    items[val]++;
                            }

                            return null;
                        }
                    }));
                }
            }

            for (IgniteInternalFuture fut : putFuts)
                fut.get();

            stopPoll.set(true);

            for (IgniteInternalFuture fut : pollFuts)
                fut.get();

            CollectionConfiguration colCfg = config(collocated);

            IgniteQueue<Integer> queue = grid(0).queue(queueName, 0, colCfg);

            int[] resItems = new int[ITEMS_PER_THREAD];

            Integer item;

            while ((item = queue.poll()) != null)
                resItems[item]++;

            for (int[] items : pollData) {
                for (int i = 0; i < ITEMS_PER_THREAD; i++)
                    resItems[i] += items[i];
            }

            for (int i = 0; i < ITEMS_PER_THREAD; i++)
                assertEquals(PUT_THREADS_PER_NODE * GRID_CNT, resItems[i]);

            assertTrue(queue.isEmpty());
        }
        finally {
            grid(0).queue(queueName, 0, null).close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testIterator() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-583");

        final String queueName = UUID.randomUUID().toString();

        info("Queue name: " + queueName);

        try (IgniteQueue<Integer> queue = grid(0).queue(queueName, QUEUE_CAPACITY, config(false))) {
            assertTrue(queue.isEmpty());

            grid(0).compute().call(new AddAllJob(queueName, RETRIES));

            assertEquals(GRID_CNT * RETRIES, queue.size());

            Collection<ClusterNode> nodes = grid(0).cluster().nodes();

            for (ClusterNode node : nodes) {
                Collection<Integer> queueElements = compute(grid(0).cluster().forNode(node)).call(new IgniteCallable<Collection<Integer>>() {
                    @IgniteInstanceResource
                    private Ignite grid;

                    /** {@inheritDoc} */
                    @Override public Collection<Integer> call() throws Exception {
                        Collection<Integer> values = new ArrayList<>();

                        grid.log().info("Running job [node=" + grid.cluster().localNode().id() + ", job=" + this + "]");

                        IgniteQueue<Integer> locQueue = grid.queue(queueName, 0, null);

                        grid.log().info("Queue size " + locQueue.size());

                        for (Integer element : locQueue)
                            values.add(element);

                        grid.log().info("Returning: " + values);

                        return values;
                    }
                });

                assertTrue(F.eqOrdered(queue, queueElements));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSerialization() throws Exception {
        // Random queue name.
        String queueName = UUID.randomUUID().toString();

        final IgniteQueue<Integer> queue = grid(0).queue(queueName, 0, config(false));

        assertNotNull(queue);

        try {
            for (int i = 0; i < 10; i++)
                queue.add(i);

            Collection<Integer> c = grid(0).compute().broadcast(new QueueJob(queue));

            assertEquals(GRID_CNT, c.size());

            for (Integer size : c)
                assertEquals((Integer)10, size);
        }
        finally {
            queue.close();
        }
    }

    /**
     * @param q Queue.
     * @param v Value.
     */
    private static <T> void put(IgniteQueue<T> q, T v) {
        X.println("Putting value: " + v);

        q.put(v);

        X.println("Done putting value: " + v);
    }

    /**
     * Tests queue serialization.
     */
    private static class QueueJob implements IgniteCallable<Integer> {
        /** */
        private IgniteQueue<Integer> queue;

        /**
         * @param queue Queue.
         */
        private QueueJob(IgniteQueue<Integer> queue) {
            this.queue = queue;
        }

        /** {@inheritDoc} */
        @Override public Integer call() throws Exception {
            assertNotNull(queue);

            return queue.size();
        }
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

        /**
         * @param queueName Queue name.
         * @param retries  Number of operations.
         */
        PutJob(String queueName, int retries) {
            this.queueName = queueName;
            this.retries = retries;
        }

        /** {@inheritDoc} */
        @Override public Integer call() throws IgniteCheckedException {
            assertNotNull(ignite);

            ignite.log().info("Running job [node=" + ignite.cluster().localNode().id() + ", job=" + this + "]");

            IgniteQueue<Integer> queue = ignite.queue(queueName, 0, null);

            assertNotNull(queue);

            for (int i = 0; i < retries; i++)
                queue.put(cntr.getAndIncrement());

            return queue.size();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(PutJob.class, this);
        }
    }

    /**
     * Test job putting data to queue.
     */
    protected static class AddAllJob implements IgniteCallable<Integer> {
        /** */
        @GridToStringExclude
        @IgniteInstanceResource
        private Ignite ignite;

        /** Queue name. */
        private final String queueName;

        /** */
        private final int size;

        /**
         * @param queueName Queue name.
         * @param size Number of add items.
         */
        AddAllJob(String queueName, int size) {
            this.queueName = queueName;
            this.size = size;
        }

        /** {@inheritDoc} */
        @Override public Integer call() throws IgniteCheckedException {
            assertNotNull(ignite);

            ignite.log().info("Running job [node=" + ignite.cluster().localNode().id() + ", job=" + this + "]");

            IgniteQueue<Integer> queue = ignite.queue(queueName, 0, null);

            assertNotNull(queue);

            Collection<Integer> items = new ArrayList<>();

            for (int i = 0; i < size; i++)
                items.add(cntr.getAndIncrement());

            queue.addAll(items);

            return queue.size();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(AddAllJob.class, this);
        }
    }

    /**
     * Test job putting data to queue.
     */
    protected static class GetJob implements IgniteCallable<Integer> {
        /** */
        @GridToStringExclude
        @IgniteInstanceResource
        private Ignite ignite;

        /** Queue name. */
        private final String queueName;

        /** */
        private final int retries;

        /** */
        private final String expVal;

        /** */
        private final CollectionConfiguration colCfg;

        /**
         * @param queueName Queue name.
         * @param colCfg Collection configuration.
         * @param retries  Number of operations.
         * @param expVal Expected value.
         */
        GetJob(String queueName, CollectionConfiguration colCfg, int retries, String expVal) {
            this.queueName = queueName;
            this.colCfg = colCfg;
            this.retries = retries;
            this.expVal = expVal;
        }

        /** {@inheritDoc} */
        @Override public Integer call() throws IgniteCheckedException {
            assertNotNull(ignite);

            ignite.log().info("Running job [node=" + ignite.cluster().localNode().id() + ", job=" + this + "]");

            IgniteQueue<String> queue = ignite.queue(queueName, QUEUE_CAPACITY, colCfg);

            assertNotNull(queue);

            assertEquals(1, queue.size());

            for (int i = 0; i < retries; i++) {
                assertEquals(expVal, queue.peek());

                assertEquals(expVal, queue.element());
            }

            return queue.size();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(GetJob.class, this);
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

        /**
         * @param queueName Queue name.
         * @param retries  Number of operations.
         */
        PutTakeJob(String queueName, int retries) {
            this.queueName = queueName;
            this.retries = retries;
        }

        /** {@inheritDoc} */
        @Override public Integer call() throws IgniteCheckedException {
            assertNotNull(ignite);

            ignite.log().info("Running job [node=" + ignite.cluster().localNode().id() + ", job=" + this + ']');

            IgniteQueue<Integer> queue = ignite.queue(queueName, 0, null);

            assertNotNull(queue);

            for (int i = 0; i < retries; i++) {
                queue.put(i);

                assertNotNull(queue.take());
            }

            return queue.size();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(PutTakeJob.class, this);
        }
    }
}