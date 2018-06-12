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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.datastructures.SetItemKey;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.GridTestUtils;

import javax.cache.CacheException;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Set failover tests.
 */
public abstract class GridCacheSetFailoverAbstractSelfTest extends IgniteCollectionAbstractTest {
    /** */
    private static final String SET_NAME = "testFailoverSet";

    /** */
    private static final long TEST_DURATION = 100_000;

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
    @Override protected CacheMode collectionCacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_DURATION + 60_000;
    }

    /**
     * Exception need to be thrown, while iteration during node fail.
     *
     * @throws Exception
     */
    public void testIterationAfterNodeFail() throws Exception {
        final IgniteSet<Integer> set = grid(0).set(SET_NAME, config(false));

        final int ITEMS = 10_000;

        Collection<Integer> items = new ArrayList<>(ITEMS);

        for (int i = 0; i < ITEMS; i++)
            items.add(i);

        set.addAll(items);

        assertEquals(ITEMS, set.size());

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int i = 0; i < 10; i++) {
            int idx = rnd.nextInt(1, gridCount());

            Iterator<Integer> iter = set.iterator();

            iter.hasNext();

            iter.next();

            stopGrid(idx);

            assertTrue(iter.hasNext());

            try {
                iter.next();
            } catch (CacheException ignored) {
                // No-op.
            }

            startGrid(idx);
        }

        set.close();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("WhileLoopReplaceableByForEach")
    public void testNodeRestart() throws Exception {
        IgniteSet<Integer> set = grid(0).set(SET_NAME, config(false));

        final int ITEMS = 10_000;

        Collection<Integer> items = new ArrayList<>(ITEMS);

        for (int i = 0; i < ITEMS; i++)
            items.add(i);

        set.addAll(items);

        assertEquals(ITEMS, set.size());

        AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> killFut = startNodeKiller(stop, null, null);

        long stopTime = System.currentTimeMillis() + TEST_DURATION;

        try {
            while (System.currentTimeMillis() < stopTime) {
                log.info("Remove set.");

                set.close();

                log.info("Create new set.");

                set = grid(0).set(SET_NAME, config(false));

                set.addAll(items);
            }
        }
        finally {
            stop.set(true);
        }

        killFut.get();

        set.close();

        if (false) { // TODO IGNITE-600: enable check when fixed.
            int cnt = 0;

            Set<IgniteUuid> setIds = new HashSet<>();

            for (int i = 0; i < gridCount(); i++) {
                GridCacheAdapter cache = grid(i).context().cache().internalCache(DEFAULT_CACHE_NAME);

                Iterator<GridCacheMapEntry> entries = cache.map().entries(cache.context().cacheId()).iterator();

                while (entries.hasNext()) {
                    GridCacheEntryEx entry = entries.next();

                    if (entry.hasValue()) {
                        cnt++;

                        if (entry.key() instanceof SetItemKey) {
                            SetItemKey setItem = (SetItemKey)entry.key();

                            if (setIds.add(setItem.setId()))
                                log.info("Unexpected set item [setId=" + setItem.setId() +
                                    ", grid: " + grid(i).name() +
                                    ", entry=" + entry + ']');
                        }
                    }
                }
            }

            assertEquals("Found unexpected cache entries", 0, cnt);
        }
    }

    /**
     * If iterator already initialized and node failed before iteration start no fail expected.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("WhileLoopReplaceableByForEach")
    public void testNodeFailBeforeIteration() throws Exception {
        IgniteSet<Integer> set = grid(0).set(SET_NAME, config(false));

        final int ITEMS = 10_000;

        Collection<Integer> items = new ArrayList<>(ITEMS);

        for (int i = 0; i < ITEMS; i++)
            items.add(i);

        set.addAll(items);

        //assertEquals(ITEMS, set.size());

        final AtomicBoolean stop = new AtomicBoolean();

        CyclicBarrier srvDown = new CyclicBarrier(2);

        CyclicBarrier srvCanDown = new CyclicBarrier(2);

        IgniteInternalFuture<?> killFut = startNodeKiller(stop, srvCanDown, srvDown);

        try {
            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            for (int i = 0; i < 10; i++) {
/*                try {
                    int size = set.size();

                    assertTrue(size == ITEMS);
                }
                catch (IgniteException ignore) {
                    // No-op.
                }*/

                Iterator<Integer> iter = set.iterator();

                int cnt = 0;

                srvCanDown.await();

                srvDown.await();

                Thread.sleep(500);

                while (iter.hasNext()) {
                    assertNotNull(iter.next());

                    cnt++;
                }

                System.err.println("expect: " + ITEMS + " current: " + cnt);

                assertTrue("expect: " + ITEMS + " current: " + cnt, cnt == ITEMS);

                int val = rnd.nextInt(ITEMS);

                assertTrue("Not contains: " + val, set.contains(val));

                val = ITEMS + rnd.nextInt(ITEMS);

                assertFalse("Contains: " + val, set.contains(val));

                srvCanDown.reset();

                log.info("Remove set.");

                set.close();

                log.info("Create new set.");

                set = grid(0).set(SET_NAME, config(false));

                set.addAll(items);
            }
        }
        finally {
            stop.set(true);
        }

        set.close();

        killFut.cancel();
    }

    /**
     * Starts thread restarting random node.
     *
     * @param stop Stop flag.
     * @return Future completing when thread finishes.
     */
    private IgniteInternalFuture<?> startNodeKiller(final AtomicBoolean stop, final CyclicBarrier srvCanDown,
                                                    final CyclicBarrier srvDown) {
        return GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                while (!stop.get()) {
                    int idx = rnd.nextInt(1, gridCount());

                    if (srvCanDown != null)
                        srvCanDown.await();
                    else
                        U.sleep(rnd.nextLong(2000, 3000));

                    log.info("Killing node: " + idx);

                    stopGrid(idx);

                    if (srvCanDown != null) {
                        srvDown.await();

                        srvDown.reset();
                    }

                    U.sleep(rnd.nextLong(500, 1000));

                    startGrid(idx);
                }

                return null;
            }
        });
    }
}
