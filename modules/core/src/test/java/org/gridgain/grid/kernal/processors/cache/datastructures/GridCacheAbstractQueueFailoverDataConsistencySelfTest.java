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

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cache.datastructures.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.GridCacheAtomicWriteOrderMode.*;
import static org.apache.ignite.cache.GridCacheAtomicityMode.*;
import static org.apache.ignite.cache.GridCacheDistributionMode.*;
import static org.apache.ignite.cache.GridCacheMode.*;

/**
 * Queue failover test.
 */
public abstract class GridCacheAbstractQueueFailoverDataConsistencySelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final String QUEUE_NAME = "FailoverQueueTest";

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrids(gridCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 5 * 60_000;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMetricsLogFrequency(0);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheStore<?, ?> cacheStore() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setBackups(1);
        ccfg.setAtomicWriteOrderMode(PRIMARY);
        ccfg.setSwapEnabled(false);
        ccfg.setQueryIndexEnabled(false);
        ccfg.setCacheStoreFactory(null);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheDistributionMode distributionMode() {
        return PARTITIONED_ONLY;
    }

    /**
     * @throws Exception If failed.
     */
    public void testAddFailover() throws Exception {
        testAddFailover(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAddFailoverCollocated() throws Exception {
        testAddFailover(true);
    }

    /**
     * @param collocated Collocation flag.
     * @throws Exception If failed.
     */
    private void testAddFailover(boolean collocated) throws Exception {
        GridCacheQueue<Integer> queue = cache().dataStructures().queue(QUEUE_NAME, 0, collocated, true);

        assertNotNull(queue);
        assertEquals(0, queue.size());

        int primaryNode = primaryQueueNode();

        int testNodeIdx = -1;

        for (int i = 0; i < gridCount(); i++) {
            if (i != primaryNode)
                testNodeIdx = i;
        }

        log.info("Test node: " + testNodeIdx) ;
        log.info("Header primary node: " + primaryNode) ;

        queue = grid(testNodeIdx).cache(null).dataStructures().queue(QUEUE_NAME, 0, collocated, false);

        assertNotNull(queue);

        testAddFailover(queue, Arrays.asList(primaryNode)); // Kill queue header's primary node .

        List<Integer> killIdxs = new ArrayList<>();

        for (int i = 0; i < gridCount(); i++) {
            if (i != testNodeIdx)
                killIdxs.add(i);
        }

        testAddFailover(queue, killIdxs); // Kill random node.
    }

    /**
     * @param queue Queue.
     * @param killIdxs Indexes of nodes to kill.
     * @throws Exception If failed.
     */
    private void testAddFailover(GridCacheQueue<Integer> queue, final List<Integer> killIdxs) throws Exception {
        assert !killIdxs.isEmpty();

        final AtomicBoolean stop = new AtomicBoolean();

        IgniteFuture<?> fut = startNodeKiller(stop, new AtomicInteger(), killIdxs);

        final int ITEMS = (atomicityMode() == ATOMIC) ? 10_000 : 3000;

        try {
            for (int i = 0; i < ITEMS; i++) {
                assertTrue(queue.add(i));

                if ((i + 1) % 500 == 0)
                    log.info("Added " + (i + 1) + " items.");
            }
        }
        finally {
            stop.set(true);
        }

        fut.get();

        log.info("Added all items.");

        for (int i = 0; i < ITEMS; i++) {
            assertEquals((Integer)i, queue.poll());

            if ((i + 1) % 500 == 0)
                log.info("Polled " + (i + 1) + " items.");
        }

        assertNull(queue.poll());
        assertEquals(0, queue.size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testPollFailover() throws Exception {
        testPollFailover(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPollFailoverCollocated() throws Exception {
        testPollFailover(true);
    }

    /**
     * @param collocated Collocation flag.
     * @throws Exception If failed.
     */
    private void testPollFailover(boolean collocated) throws Exception {
        GridCacheQueue<Integer> queue = cache().dataStructures().queue(QUEUE_NAME, 0, collocated, true);

        assertNotNull(queue);
        assertEquals(0, queue.size());

        int primaryNode = primaryQueueNode();

        int testNodeIdx = -1;

        for (int i = 0; i < gridCount(); i++) {
            if (i != primaryNode)
                testNodeIdx = i;
        }

        log.info("Test node: " + testNodeIdx) ;
        log.info("Primary node: " + primaryNode) ;

        queue = grid(testNodeIdx).cache(null).dataStructures().queue(QUEUE_NAME, 0, collocated, false);

        assertNotNull(queue);

        testPollFailover(queue, Arrays.asList(primaryQueueNode())); // Kill queue header's primary node .

        List<Integer> killIdxs = new ArrayList<>();

        for (int i = 0; i < gridCount(); i++) {
            if (i != testNodeIdx)
                killIdxs.add(i);
        }

        testPollFailover(queue, killIdxs); // Kill random node.
    }

    /**
     * @param queue Queue.
     * @param killIdxs Indexes of nodes to kill.
     * @throws Exception If failed.
     */
    private void testPollFailover(GridCacheQueue<Integer> queue, final List<Integer> killIdxs) throws Exception {
        assert !killIdxs.isEmpty();

        final int ITEMS = atomicityMode() == ATOMIC && !queue.collocated() ? 10_000 : 3000;

        for (int i = 0; i < ITEMS; i++) {
            assertTrue(queue.add(i));

            if ((i + 1) % 500 == 0)
                log.info("Added " + (i + 1) + " items.");
        }

        log.info("Added all items.");

        final AtomicBoolean stop = new AtomicBoolean();

        final AtomicInteger stopCnt = new AtomicInteger();

        IgniteFuture<?> fut = startNodeKiller(stop, stopCnt, killIdxs);

        int err = 0;

        try {
            int pollNum = ITEMS;

            int exp = 0;

            for (int i = 0; i < pollNum; i++) {
                Integer e = queue.poll();

                if (atomicityMode() == ATOMIC) {
                    if (e == null || e != exp) {
                        log.info("Unexpected data [expected=" + i + ", actual=" + e + ']');

                        err++;

                        pollNum--;

                        exp = e != null ? (e + 1) : (exp + 1);
                    }
                    else
                        exp++;
                }
                else
                    assertEquals((Integer)i, e);

                if ((i + 1) % 500 == 0)
                    log.info("Polled " + (i + 1) + " items.");
            }
        }
        finally {
            stop.set(true);
        }

        fut.get();

        if (atomicityMode() == ATOMIC)
            assertTrue("Too many errors for atomic cache: " + err, err <= stopCnt.get());

        assertNull(queue.poll());
        assertEquals(0, queue.size());
    }

    /**
     * Starts thread restarting random node (node's index is chosen using given collection).
     *
     * @param stop Stop flag.
     * @param killCnt Counter incremented after node restart.
     * @param killIdxs Indexes of nodes to kill.
     * @return Future completing when thread finishes.
     */
    private IgniteFuture<?> startNodeKiller(final AtomicBoolean stop, final AtomicInteger killCnt,
        final List<Integer> killIdxs) {
        return GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                while (!stop.get()) {
                    int idx = killIdxs.get(rnd.nextInt(0, killIdxs.size()));

                    U.sleep(rnd.nextLong(500, 1000));

                    log.info("Killing node: " + idx);

                    stopGrid(idx);

                    U.sleep(rnd.nextLong(500, 1000));

                    startGrid(idx);

                    killCnt.incrementAndGet();
                }

                return null;
            }
        });
    }

    /**
     * @return Primary node for queue's header.
     */
    private int primaryQueueNode() {
        GridCacheAffinity<Object> aff = grid(0).cache(null).affinity();

        for (int i = 0; i < gridCount(); i++) {
            for (GridCacheEntryEx e : ((GridKernal)grid(i)).context().cache().internalCache().map().allEntries0()) {
                if (aff.isPrimary(grid(i).localNode(), e.key()) && e.key() instanceof GridCacheQueueHeaderKey)
                    return i;
            }
        }

        fail("Failed to find primary node for queue header.");

        return -1;
    }
}
