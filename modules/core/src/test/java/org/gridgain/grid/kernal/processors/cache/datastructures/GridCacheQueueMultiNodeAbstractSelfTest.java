/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Queue multi node tests.
 */
public abstract class GridCacheQueueMultiNodeAbstractSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int GRID_CNT = 4;

    /** */
    private static GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    private static final int RETRIES = 20;

    /** */
    protected static final int QUEUE_CAPACITY = 100000;

    /** */
    private static AtomicInteger cntr = new AtomicInteger();

    /** */
    private static final int ITEMS_CNT = 50;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
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
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        GridTcpDiscoverySpi spi = new GridTcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);
        cfg.setRestEnabled(false);

        cfg.setExecutorService(
            new ThreadPoolExecutor(
                RETRIES * 2,
                RETRIES * 2,
                0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>()));

        cfg.setExecutorServiceShutdown(true);

        cfg.setSystemExecutorService(
            new ThreadPoolExecutor(
                RETRIES * 2,
                RETRIES * 2,
                0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>()));

        cfg.setSystemExecutorServiceShutdown(true);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPut() throws Exception {
        String queueName = UUID.randomUUID().toString();

        GridCacheQueue<Integer> queue = grid(0).cache(null).dataStructures().queue(queueName, QUEUE_CAPACITY,
            false, true);

        assertTrue(queue.isEmpty());

        grid(0).compute().broadcast(new PutJob(queueName, RETRIES));

        assertEquals(GRID_CNT * RETRIES, queue.size());

        for (int i = 0; i < GRID_CNT; i++) {
            assertTrue(grid(i).cache(null).isEmpty());
            assertEquals(0, grid(i).cache(null).size());
            assertEquals(0, grid(i).cache(null).primarySize());
            assertEquals(0, grid(i).cache(null).values().size());
            assertEquals(0, grid(i).cache(null).primaryValues().size());
            assertEquals(0, grid(i).cache(null).keySet().size());
            assertEquals(0, grid(i).cache(null).primaryKeySet().size());
            assertEquals(0, grid(i).cache(null).entrySet().size());
            assertEquals(0, grid(i).cache(null).primaryEntrySet().size());
            assertEquals(0, grid(i).cache(null).globalSize());
            assertEquals(0, grid(i).cache(null).globalPrimarySize());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutPollCollocated() throws Exception {
        try {
            final String queueName = UUID.randomUUID().toString();

            info("Queue name: " + queueName);

            grid(0).cache(null).dataStructures().queue(queueName, 5, true, true);

            final CountDownLatch latch = new CountDownLatch(1);

            final Ignite g = startGrid(GRID_CNT + 1);

            GridFuture<Object> fut1 = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    info(">>> Executing put callable [node=" + g.cluster().localNode().id() +
                        ", thread=" + Thread.currentThread().getName() + ", aff=" +
                        F.nodeId8s(g.cache(null).affinity().mapKeyToPrimaryAndBackups(
                            new GridCacheInternalKeyImpl(queueName))) + ']');

                    GridCacheQueue<Integer> q = g.cache(null).dataStructures().queue(queueName, 5, true, true);

                    assert q.isEmpty();

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

            GridFuture<Object> fut2 = GridTestUtils.runAsync(new Callable<Object>() {
                @SuppressWarnings("BusyWait")
                @Override public Object call() throws Exception {
                    try {
                        info(">>> Executing poll callable [node=" + g1.cluster().localNode().id() +
                            ", thread=" + Thread.currentThread().getName() + ", aff=" +
                            F.nodeId8s(g1.cache(null).affinity().mapKeyToPrimaryAndBackups(
                                new GridCacheInternalKeyImpl(queueName))) + ']');

                        GridCacheQueue<Integer> q = g1.cache(null).dataStructures().queue(queueName, 5, true, true);

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

            grid(0).cache(null).dataStructures().removeQueue(queueName);
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

            GridCache c = grid(0).cache(null);

            GridCacheQueue<Integer> queue = c.dataStructures().queue(queueName, QUEUE_CAPACITY, false, true);

            assertTrue(queue.isEmpty());

            grid(0).compute().call(new AddAllJob(queueName, RETRIES));

            assertEquals(GRID_CNT * RETRIES, queue.size());

            queue.clear(5);

            assertEquals(0, queue.size());

            c.dataStructures().removeQueue(queueName);
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

        GridCacheQueue<String> queue = grid(0).cache(null).dataStructures().queue(queueName, QUEUE_CAPACITY,
            false, true);

        assertTrue(queue.isEmpty());

        String val = UUID.randomUUID().toString();

        queue.put(val);

        grid(0).compute().call(new GetJob(queueName, RETRIES, val));

        assertEquals(1, queue.size());

        grid(0).cache(null).dataStructures().removeQueue(queueName);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutTake() throws Exception {
        String queueName = UUID.randomUUID().toString();

        info("Queue name: " + queueName);

        GridCacheQueue<Integer> queue = grid(0).cache(null).dataStructures().queue(queueName, QUEUE_CAPACITY,
            false, true);

        assertTrue(queue.isEmpty());

        grid(0).compute().broadcast(new PutTakeJob(queueName, RETRIES));

        assertEquals(0, queue.size());

        grid(0).cache(null).dataStructures().removeQueue(queueName);
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
            Collection<GridFuture> futs = new ArrayList<>();

            final int THREADS_PER_NODE = 3;
            final int ITEMS_PER_THREAD = 1000;

            for (int i = 0; i < GRID_CNT; i++) {
                final int idx = i;

                futs.add(GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        GridCache cache = grid(idx).cache(null);

                        GridCacheQueue<Integer> queue = cache.dataStructures().queue(queueName, 0, collocated, true);

                        for (int i = 0; i < ITEMS_PER_THREAD; i++)
                            assertTrue(queue.add(i));

                        return null;
                    }
                }, THREADS_PER_NODE, "testPutMultiNode"));
            }

            for (GridFuture fut : futs)
                fut.get();

            GridCache cache = grid(0).cache(null);

            GridCacheQueue<Integer> queue = cache.dataStructures().queue(queueName, 0, collocated, true);

            assertEquals(THREADS_PER_NODE * ITEMS_PER_THREAD * GRID_CNT, queue.size());

            int[] items = new int[ITEMS_PER_THREAD];

            Integer item;

            while ((item = queue.poll()) != null)
                items[item]++;

            for (int i = 0; i < ITEMS_PER_THREAD; i++)
                assertEquals(THREADS_PER_NODE * GRID_CNT, items[i]);
        }
        finally {
            grid(0).cache(null).dataStructures().removeQueue(queueName);
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
            Collection<GridFuture> putFuts = new ArrayList<>();
            Collection<GridFuture> pollFuts = new ArrayList<>();

            final int PUT_THREADS_PER_NODE = 3;
            final int POLL_THREADS_PER_NODE = 2;
            final int ITEMS_PER_THREAD = 1000;

            final AtomicBoolean stopPoll = new AtomicBoolean();

            Collection<int[]> pollData = new ArrayList<>();

            for (int i = 0; i < GRID_CNT; i++) {
                final int idx = i;

                putFuts.add(GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        GridCache cache = grid(idx).cache(null);

                        GridCacheQueue<Integer> queue = cache.dataStructures().queue(queueName, 0, collocated, true);

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
                            GridCache cache = grid(idx).cache(null);

                            GridCacheQueue<Integer> queue = cache.dataStructures().queue(queueName, 0,
                                collocated, true);

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

            for (GridFuture fut : putFuts)
                fut.get();

            stopPoll.set(true);

            for (GridFuture fut : pollFuts)
                fut.get();

            GridCache cache = grid(0).cache(null);

            GridCacheQueue<Integer> queue = cache.dataStructures().queue(queueName, 0, collocated, true);

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
            grid(0).cache(null).dataStructures().removeQueue(queueName);
        }
    }

    /**
     * @throws Exception If failed.
     */
    // TODO: GG-4807 Uncomment when fix
    public void _testIterator() throws Exception {
        final String queueName = UUID.randomUUID().toString();

        info("Queue name: " + queueName);

        GridCacheQueue<Integer> queue = grid(0).cache(null).dataStructures().queue(queueName, QUEUE_CAPACITY, false, true);

        assertTrue(queue.isEmpty());

        grid(0).compute().call(new AddAllJob(queueName, RETRIES));

        assertEquals(GRID_CNT * RETRIES, queue.size());

        Collection<GridNode> nodes = grid(0).nodes();

        for (GridNode node : nodes) {
            Collection<Integer> queueElements = compute(grid(0).forNode(node)).call(new GridCallable<Collection<Integer>>() {
                @GridInstanceResource
                private Ignite grid;

                /** {@inheritDoc} */
                @Override public Collection<Integer> call() throws Exception {
                    Collection<Integer> values = new ArrayList<>();

                    grid.log().info("Running job [node=" + grid.cluster().localNode().id() + ", job=" + this + "]");

                    GridCacheQueue<Integer> locQueue = grid.cache(null).dataStructures().queue(queueName,
                        QUEUE_CAPACITY, false, true);

                    grid.log().info("Queue size " + locQueue.size());

                    for (Integer element : locQueue)
                        values.add(element);

                    grid.log().info("Returning: " + values);

                    return values;
                }
            });

            assertTrue(F.eqOrdered(queue, queueElements));
        }

        grid(0).cache(null).dataStructures().removeQueue(queueName);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSerialization() throws Exception {
        // Random queue name.
        String queueName = UUID.randomUUID().toString();

        final GridCacheQueue<Integer> queue = grid(0).cache(null).dataStructures().queue(queueName, 0, false, true);

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
            grid(0).cache(null).dataStructures().removeQueue(queueName);
        }
    }

    /**
     * @param q Queue.
     * @param v Value.
     */
    private static <T> void put(GridCacheQueue<T> q, T v) {
        X.println("Putting value: " + v);

        q.put(v);

        X.println("Done putting value: " + v);
    }

    /**
     * Tests queue serialization.
     */
    private static class QueueJob implements Callable<Integer>, Serializable {
        /** */
        private GridCacheQueue<Integer> queue;

        /**
         * @param queue Queue.
         */
        private QueueJob(GridCacheQueue<Integer> queue) {
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
    protected static class PutJob implements GridCallable<Integer> {
        /** */
        @GridToStringExclude
        @GridInstanceResource
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
        @Override public Integer call() throws GridException {
            assertNotNull(ignite);

            ignite.log().info("Running job [node=" + ignite.cluster().localNode().id() + ", job=" + this + "]");

            GridCacheQueue<Integer> queue = ignite.cache(null).dataStructures().queue(queueName, QUEUE_CAPACITY,
                false, true);

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
    protected static class AddAllJob implements GridCallable<Integer> {
        /** */
        @GridToStringExclude
        @GridInstanceResource
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
        @Override public Integer call() throws GridException {
            assertNotNull(ignite);

            ignite.log().info("Running job [node=" + ignite.cluster().localNode().id() + ", job=" + this + "]");

            GridCacheQueue<Integer> queue = ignite.cache(null).dataStructures().queue(queueName, QUEUE_CAPACITY,
                false, true);

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
    protected static class GetJob implements GridCallable<Integer> {
        /** */
        @GridToStringExclude
        @GridInstanceResource
        private Ignite ignite;

        /** Queue name. */
        private final String queueName;

        /** */
        private final int retries;

        /** */
        private final String expVal;

        /**
         * @param queueName Queue name.
         * @param retries  Number of operations.
         * @param expVal Expected value.
         */
        GetJob(String queueName, int retries, String expVal) {
            this.queueName = queueName;
            this.retries = retries;
            this.expVal = expVal;
        }

        /** {@inheritDoc} */
        @Override public Integer call() throws GridException {
            assertNotNull(ignite);

            ignite.log().info("Running job [node=" + ignite.cluster().localNode().id() + ", job=" + this + "]");

            GridCacheQueue<String> queue = ignite.cache(null).dataStructures().queue(queueName, QUEUE_CAPACITY,
                false, true);

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
    protected static class PutTakeJob implements GridCallable<Integer> {
        /** */
        @GridToStringExclude
        @GridInstanceResource
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
        @Override public Integer call() throws GridException {
            assertNotNull(ignite);

            ignite.log().info("Running job [node=" + ignite.cluster().localNode().id() + ", job=" + this + ']');

            GridCacheQueue<Integer> queue = ignite.cache(null).dataStructures().queue(queueName, QUEUE_CAPACITY,
                false, true);

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
