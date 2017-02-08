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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Cache count down latch self test.
 */
public abstract class IgniteCountDownLatchAbstractSelfTest extends IgniteAtomicsAbstractTest
    implements Externalizable {
    /** */
    private static final int NODES_CNT = 4;

    /** */
    protected static final int THREADS_CNT = 5;

    /** */
    private static final Random RND = new Random();

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return NODES_CNT;
    }

    /**
     * @throws Exception If failed.
     */
    public void testLatch() throws Exception {
        checkLatch();
    }

    /**
     * Implementation of ignite data structures internally uses special system caches, need make sure
     * that transaction on these system caches do not intersect with transactions started by user.
     *
     * @throws Exception If failed.
     */
    public void testIsolation() throws Exception {
        Ignite ignite = grid(0);

        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setName("myCache");
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setWriteSynchronizationMode(FULL_SYNC);

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(cfg);

        try {
            IgniteCountDownLatch latch = ignite.countDownLatch("latch1", 10, false, true);

            assertNotNull(latch);

            try (Transaction tx = ignite.transactions().txStart()) {
                cache.put(1, 1);

                assertEquals(8, latch.countDown(2));

                tx.rollback();
            }

            assertEquals(0, cache.size());

            assertEquals(7, latch.countDown(1));
        }
        finally {
            ignite.destroyCache(cfg.getName());
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void checkLatch() throws Exception {
        // Test API.
        checkAutoDelete();

        checkAwait();

        checkCountDown();

        // Test main functionality.
        IgniteCountDownLatch latch1 = grid(0).countDownLatch("latch", 2, false, true);

        assertEquals(2, latch1.count());

        IgniteCompute comp = grid(0).compute().withAsync();

        comp.call(new IgniteCallable<Object>() {
            @IgniteInstanceResource
            private Ignite ignite;

            @LoggerResource
            private IgniteLogger log;

            @Nullable @Override public Object call() throws Exception {
                // Test latch in multiple threads on each node.
                IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(
                    new Callable<Object>() {
                        @Nullable @Override public Object call() throws Exception {
                            IgniteCountDownLatch latch = ignite.countDownLatch("latch", 2, false, true);

                            assert latch != null && latch.count() == 2;

                            log.info("Thread is going to wait on latch: " + Thread.currentThread().getName());

                            assert latch.await(1, MINUTES);

                            log.info("Thread is again runnable: " + Thread.currentThread().getName());

                            return null;
                        }
                    },
                    5,
                    "test-thread"
                );

                fut.get();

                return null;
            }
        });

        IgniteFuture<Object> fut = comp.future();

        Thread.sleep(3000);

        assert latch1.countDown() == 1;

        assert latch1.countDown() == 0;

        // Ensure there are no hangs.
        fut.get();

        // Test operations on removed latch.
        latch1.close();

        checkRemovedLatch(latch1);
    }

    /**
     * @param latch Latch.
     * @throws Exception If failed.
     */
    protected void checkRemovedLatch(final IgniteCountDownLatch latch) throws Exception {
        assert GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return latch.removed();
            }
        }, 5000);

        assert latch.removed();

        assert latch.count() == 0;

        // Test await on removed future.
        latch.await();
        assert latch.await(10);
        assert latch.await(10, SECONDS);

        latch.await();

        // Test countdown.
        assert latch.countDown() == 0;
        assert latch.countDown(5) == 0;
        latch.countDownAll();
    }

    /**
     * @throws Exception Exception.
     */
    private void checkAutoDelete() throws Exception {
        IgniteCountDownLatch latch = createLatch("rmv", 5, true);

        latch.countDownAll();

        // Latch should be removed since autoDelete = true
        checkRemovedLatch(latch);

        IgniteCountDownLatch latch1 = createLatch("rmv1", 5, false);

        latch1.countDownAll();

        // Latch should NOT be removed since autoDelete = false
        assert !latch1.removed();

        removeLatch("rmv1");
    }

    /**
     * @throws Exception Exception.
     */
    private void checkAwait() throws Exception {
        // Check only 'false' cases here. Successful await is tested over the grid.
        IgniteCountDownLatch latch = createLatch("await", 5, false);

        assert !latch.await(10);
        assert !latch.await(10, MILLISECONDS);

        removeLatch("await");
    }

    /**
     * @throws Exception Exception.
     */
    private void checkCountDown() throws Exception {
        IgniteCountDownLatch latch = createLatch("cnt", 10, true);

        assert latch.countDown() == 9;
        assert latch.countDown(2) == 7;

        latch.countDownAll();

        assert latch.count() == 0;

        checkRemovedLatch(latch);

        IgniteCountDownLatch latch1 = createLatch("cnt1", 10, true);

        assert latch1.countDown() == 9;
        assert latch1.countDown(2) == 7;

        latch1.countDownAll();

        assert latch1.count() == 0;

        checkRemovedLatch(latch1);
    }

    /**
     * @param latchName Latch name.
     * @param cnt Count.
     * @param autoDel Auto delete flag.
     * @return New latch.
     * @throws Exception If failed.
     */
    private IgniteCountDownLatch createLatch(String latchName, int cnt, boolean autoDel)
        throws Exception {
        IgniteCountDownLatch latch = grid(RND.nextInt(NODES_CNT)).countDownLatch(latchName, cnt, autoDel, true);

        // Test initialization.
        assert latchName.equals(latch.name());
        assert latch.count() == cnt;
        assert latch.initialCount() == cnt;
        assert latch.autoDelete() == autoDel;

        return latch;
    }

    /**
     * @param latchName Latch name.
     * @throws Exception If failed.
     */
    private void removeLatch(String latchName)
        throws Exception {
        IgniteCountDownLatch latch = grid(RND.nextInt(NODES_CNT)).countDownLatch(latchName, 10, false, true);

        assert latch != null;

        if (latch.count() > 0)
            latch.countDownAll();

        // Remove latch on random node.
        IgniteCountDownLatch latch0 = grid(RND.nextInt(NODES_CNT)).countDownLatch(latchName, 0, false, false);

        assertNotNull(latch0);

        latch0.close();

        // Ensure latch is removed on all nodes.
        for (Ignite g : G.allGrids())
            assertNull(((IgniteKernal)g).context().dataStructures().countDownLatch(latchName, 10, true, false));

        checkRemovedLatch(latch);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLatchMultinode1() throws Exception {
        if (gridCount() == 1)
            return;

        IgniteCountDownLatch latch = grid(0).countDownLatch("l1", 10,
            true,
            true);

        List<IgniteInternalFuture<?>> futs = new ArrayList<>();

        final AtomicBoolean countedDown = new AtomicBoolean();

        for (int i = 0; i < gridCount(); i++) {
            final Ignite ignite = grid(i);

            futs.add(GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    IgniteCountDownLatch latch = ignite.countDownLatch("l1", 10,
                        true,
                        false);

                    assertNotNull(latch);

                    boolean wait = latch.await(30_000);

                    assertTrue(countedDown.get());

                    assertEquals(0, latch.count());

                    assertTrue(wait);

                    return null;
                }
            }));
        }

        for (int i = 0; i < 10; i++) {
            if (i == 9)
                countedDown.set(true);

            latch.countDown();
        }

        for (IgniteInternalFuture<?> fut : futs)
            fut.get(30_000);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLatchBroadcast() throws Exception {
        Ignite ignite = grid(0);
        ClusterGroup srvsGrp = ignite.cluster().forServers();

        int numOfSrvs = srvsGrp.nodes().size();

        ignite.destroyCache("testCache");
        IgniteCache<Object, Object> cache = ignite.createCache("testCache");

        for (ClusterNode node : srvsGrp.nodes())
            cache.put(String.valueOf(node.id()), 0);

        for (int i = 0; i < 500; i++) {
            IgniteCountDownLatch latch1 = createLatch1(ignite, numOfSrvs);
            IgniteCountDownLatch latch2 = createLatch2(ignite, numOfSrvs);

            ignite.compute(srvsGrp).broadcast(new IgniteRunnableJob(latch1, latch2, i));
            assertTrue(latch2.await(10000));
        }
    }

    /**
     * @param client Ignite client.
     * @param numOfSrvs Number of server nodes.
     * @return Ignite latch.
     */
    private IgniteCountDownLatch createLatch1(Ignite client, int numOfSrvs) {
        return client.countDownLatch(
            "testName1", // Latch name.
            numOfSrvs,          // Initial count.
            true,        // Auto remove, when counter has reached zero.
            true         // Create if it does not exist.
        );
    }

    /**
     * @param client Ignite client.
     * @param numOfSrvs Number of server nodes.
     * @return Ignite latch.
     */
    private IgniteCountDownLatch createLatch2(Ignite client, int numOfSrvs) {
        return client.countDownLatch(
            "testName2", // Latch name.
            numOfSrvs,          // Initial count.
            true,        // Auto remove, when counter has reached zero.
            true         // Create if it does not exist.
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testLatchMultinode2() throws Exception {
        if (gridCount() == 1)
            return;

        IgniteCountDownLatch latch = grid(0).countDownLatch("l2", gridCount() * 3,
            true,
            true);

        assertNotNull(latch);

        List<IgniteInternalFuture<?>> futs = new ArrayList<>();

        final AtomicInteger cnt = new AtomicInteger();

        for (int i = 0; i < gridCount(); i++) {
            final Ignite ignite = grid(i);

            futs.add(GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    IgniteCountDownLatch latch = ignite.countDownLatch("l2", 10,
                        true,
                        false);

                    assertNotNull(latch);

                    for (int i = 0; i < 3; i++) {
                        cnt.incrementAndGet();

                        latch.countDown();
                    }

                    boolean wait = latch.await(30_000);

                    assertEquals(gridCount() * 3, cnt.get());

                    assertEquals(0, latch.count());

                    assertTrue(wait);

                    return null;
                }
            }));
        }

        for (IgniteInternalFuture<?> fut : futs)
            fut.get(30_000);
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
     * Ignite job
     */
    public class IgniteRunnableJob implements IgniteRunnable {

        /**
         * Ignite.
         */
        @IgniteInstanceResource
        Ignite igniteInstance;

        /**
         * Number of iteration.
         */
        protected final int iteration;

        /**
         * Ignite latch 1.
         */
        private final IgniteCountDownLatch latch1;

        /**
         * Ignite latch 2.
         */
        private final IgniteCountDownLatch latch2;

        /**
         * @param latch1 Ignite latch 1.
         * @param latch2 Ignite latch 2.
         * @param iteration Number of iteration.
         */
        public IgniteRunnableJob(IgniteCountDownLatch latch1, IgniteCountDownLatch latch2, int iteration) {
            this.iteration = iteration;
            this.latch1 = latch1;
            this.latch2 = latch2;
        }

        /**
         * @return Ignite latch.
         */
        IgniteCountDownLatch createLatch1() {
            return latch1;
        }

        /**
         * @return Ignite latch.
         */
        IgniteCountDownLatch createLatch2() {
            return latch2;
        }

        /** {@inheritDoc} */
        @Override
        public void run() {

            IgniteCountDownLatch latch1 = createLatch1();
            IgniteCountDownLatch latch2 = createLatch2();

            IgniteCache<Object, Object> cache = igniteInstance.cache("testCache");

            for (ClusterNode node : igniteInstance.cluster().forServers().nodes()) {
                Integer val = (Integer)cache.get(String.valueOf(node.id()));
                assertEquals(val, (Integer)iteration);
            }

            latch1.countDown();

            assertTrue(latch1.await(10000));

            cache.put(getUID(), (iteration + 1));

            latch2.countDown();

        }

        /**
         * @return Node UUID as string.
         */
        String getUID() {
            String id = "";
            Collection<ClusterNode> nodes = igniteInstance.cluster().forLocal().nodes();
            for (ClusterNode node : nodes) {
                if (node.isLocal())
                    id = String.valueOf(node.id());
            }
            return id;
        }

        /**
         * @return Ignite.
         */
        public Ignite igniteInstance() {
            return igniteInstance;
        }
    }
}
