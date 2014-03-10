/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.gridgain.examples.datagrid.store.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Queue multi node tests.
 */
public abstract class GridCacheQueueMultiNodeAbstractSelfTest extends GridCommonAbstractTest implements Externalizable {
    /** */
    private static final int GRID_CNT = 4;

    /** */
    private static GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    private static final int RETRIES = 20;

    /** */
    protected static final int QUEUE_CAPACITY = 100000;

    /** */
    private static CountDownLatch lthTake = new CountDownLatch(1);

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

            for (Grid g : G.allGrids()) {
                if (g.nodes().size() != GRID_CNT) {
                    info("Grid has incorrect nodes count [gridName=" + g.name() +
                        ", nodesCnt=" + g.nodes().size() + ']');

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

        grid(0).compute().broadcast(new PutJob(queueName, RETRIES)).get();

        assertEquals(GRID_CNT * RETRIES, queue.size());
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

            GridFuture<Object> fut1 = startGrid(GRID_CNT + 1).scheduler().callLocal(new Callable<Object>() {
                @GridInstanceResource
                private Grid g;

                @Override public Object call() throws Exception {
                    info(">>> Executing put callable [node=" + g.localNode().id() +
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

                    info(">>> Finished put callable on node: " + g.localNode().id());

                    return null;
                }
            });

            latch.await();

            GridFuture<Object> fut2 = startGrid(GRID_CNT + 2).scheduler().callLocal(new Callable<Object>() {
                @GridInstanceResource
                private Grid g;

                @SuppressWarnings("BusyWait")
                @Override public Object call() throws Exception {
                    try {
                        info(">>> Executing poll callable [node=" + g.localNode().id() +
                            ", thread=" + Thread.currentThread().getName() + ", aff=" +
                            F.nodeId8s(g.cache(null).affinity().mapKeyToPrimaryAndBackups(
                                new GridCacheInternalKeyImpl(queueName))) + ']');

                        GridCacheQueue<Integer> q = g.cache(null).dataStructures().queue(queueName, QUEUE_CAPACITY,
                            false, true);

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

                        info("Finished poll callable on node: " + g.localNode().id());

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

            grid(0).compute().call(new AddAllJob(queueName, RETRIES)).get();

            assertEquals(GRID_CNT * RETRIES, queue.size());

            // Create query which joins on 2 types to select people for a specific organization.
            GridCacheQuery<Map.Entry<GridCacheAffinityKey<UUID>, Person>> qry =
                c.queries().createSqlQuery(GridCacheQueueItemImpl.class,
                    "select * from GridCacheQueueItemImpl where qid=?");

            assertEquals(GRID_CNT * RETRIES, REPLICATED == c.configuration().getCacheMode() ?
                qry.projection(grid(0).forLocal()).execute(queue.name()).get().size() :
                qry.execute(queue.name()).get().size());

            queue.clear(5);

            assertEquals(0, queue.size());

            assertEquals(0, REPLICATED == c.configuration().getCacheMode() ?
                qry.projection(grid(0).forLocal()).execute(queue.name()).get().size() :
                qry.execute(queue.name()).get().size());

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

        grid(0).compute().call(new GetJob(queueName, RETRIES, val)).get();

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

        grid(0).compute().call(new PutTakeJob(queueName, RETRIES)).get();

        assertEquals(0, queue.size());

        grid(0).cache(null).dataStructures().removeQueue(queueName);
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
            Collection<Integer> queueElements = grid(0).forNode(node).compute().call(new GridCallable<Collection<Integer>>() {
                @GridInstanceResource
                private Grid grid;

                /** {@inheritDoc} */
                @Override public Collection<Integer> call() throws Exception {
                    Collection<Integer> values = new ArrayList<>();

                    grid.log().info("Running job [node=" + grid.localNode().id() + ", job=" + this + "]");

                    GridCacheQueue<Integer> locQueue = grid.cache(null).dataStructures().queue(queueName,
                        QUEUE_CAPACITY, false, true);

                    grid.log().info("Queue size " + locQueue.size());

                    for (Integer element : locQueue)
                        values.add(element);

                    grid.log().info("Returning: " + values);

                    return values;
                }
            }).get();

            assertTrue(F.eqOrdered(queue, queueElements));
        }

        grid(0).cache(null).dataStructures().removeQueue(queueName);
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

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // No-op.
    }

    /**
     * Test job putting data to queue.
     */
    protected static class PutJob implements GridCallable<Integer> {
        /** */
        @GridToStringExclude
        @GridInstanceResource
        private Grid grid;

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
            assertNotNull(grid);

            grid.log().info("Running job [node=" + grid.localNode().id() + ", job=" + this + "]");

            GridCacheQueue<Integer> queue = grid.cache(null).dataStructures().queue(queueName, QUEUE_CAPACITY,
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
        private Grid grid;

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
            assertNotNull(grid);

            grid.log().info("Running job [node=" + grid.localNode().id() + ", job=" + this + "]");

            GridCacheQueue<Integer> queue = grid.cache(null).dataStructures().queue(queueName, QUEUE_CAPACITY,
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
        private Grid grid;

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
            assertNotNull(grid);

            grid.log().info("Running job [node=" + grid.localNode().id() + ", job=" + this + "]");

            GridCacheQueue<String> queue = grid.cache(null).dataStructures().queue(queueName, QUEUE_CAPACITY,
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
        private Grid grid;

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
            assertNotNull(grid);

            grid.log().info("Running job [node=" + grid.localNode().id() + ", job=" + this + ']');

            GridCacheQueue<Integer> queue = grid.cache(null).dataStructures().queue(queueName, QUEUE_CAPACITY,
                false, true);

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
    protected static class TakeJob implements GridCallable<Boolean> {
        /** */
        @GridInstanceResource
        private Grid grid;

        /** Queue name. */
        private final String queueName;

        /**
         * @param queueName Queue name.
         */
        TakeJob(String queueName) {
            this.queueName = queueName;
        }

        /** {@inheritDoc} */
        @Override public Boolean call() throws GridException {
            assertNotNull(grid);

            grid.log().info("Running job [node=" + grid.localNode().id() + ", job=" + this + ']');

            GridCacheQueue<Integer> queue = grid.cache(null).dataStructures().queue(queueName, QUEUE_CAPACITY,
                false, true);

            assertNotNull(queue);

            try {
                // Queue can be removed.
                lthTake.countDown();

                queue.take();
            }
            catch (GridRuntimeException e) {
                grid.log().info("Caught expected exception: " + e.getMessage());
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
    protected static class RemoveQueueJob implements GridCallable<Boolean> {
        /** */
        @GridInstanceResource
        private Grid grid;

        /** Queue name. */
        private final String queueName;

        /**
         * @param queueName Queue name.
         */
        RemoveQueueJob(String queueName) {
            this.queueName = queueName;
        }

        /** {@inheritDoc} */
        @Override public Boolean call() throws GridException {
            assertNotNull(grid);

            grid.log().info("Running job [node=" + grid.localNode().id() + ", job=" + this + "]");

            GridCacheQueue<Integer> queue = grid.cache(null).dataStructures().queue(queueName, QUEUE_CAPACITY,
                false, true);

            assert queue.capacity() == QUEUE_CAPACITY;

            assert grid.cache(null).dataStructures().removeQueue(queueName, 10);

            return true;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RemoveQueueJob.class, this);
        }
    }
}
