/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.util.typedef.X;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Concurrent and advanced tests for WAL state change.
 */
@SuppressWarnings("unchecked")
public class WalModeChangeAdvancedSelfTest extends WalModeChangeCommonAbstractSelfTest {
    /**
     * Constructor.
     */
    public WalModeChangeAdvancedSelfTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        deleteWorkFiles();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        deleteWorkFiles();
    }

    /** {@inheritDoc} */
    public void testCacheCleanup() throws Exception {
        Ignite srv = startGrid(config(SRV_1, false, false));

        srv.active(true);

        IgniteCache cache1 = srv.getOrCreateCache(cacheConfig(CACHE_NAME, PARTITIONED, TRANSACTIONAL));
        IgniteCache cache2 = srv.getOrCreateCache(cacheConfig(CACHE_NAME_2, PARTITIONED, TRANSACTIONAL));

        assertForAllNodes(CACHE_NAME, true);
        assertForAllNodes(CACHE_NAME_2, true);

        for (int i = 0; i < 10; i++) {
            cache1.put(i, i);
            cache2.put(i, i);
        }

        srv.cluster().walDisable(CACHE_NAME);

        assertForAllNodes(CACHE_NAME, false);
        assertForAllNodes(CACHE_NAME_2, true);

        for (int i = 10; i < 20; i++) {
            cache1.put(i, i);
            cache2.put(i, i);
        }

        srv.cluster().walDisable(CACHE_NAME_2);

        assertForAllNodes(CACHE_NAME, false);
        assertForAllNodes(CACHE_NAME_2, false);

        for (int i = 20; i < 30; i++) {
            cache1.put(i, i);
            cache2.put(i, i);
        }

        assertEquals(cache1.size(), 30);
        assertEquals(cache2.size(), 30);

        srv.cluster().walEnable(CACHE_NAME);

        assertForAllNodes(CACHE_NAME, true);
        assertForAllNodes(CACHE_NAME_2, false);

        assertEquals(cache1.size(), 30);
        assertEquals(cache2.size(), 30);

        stopAllGrids(true);

        srv = startGrid(config(SRV_1, false, false));

        srv.active(true);

        cache1 = srv.getOrCreateCache(cacheConfig(CACHE_NAME, PARTITIONED, TRANSACTIONAL));
        cache2 = srv.getOrCreateCache(cacheConfig(CACHE_NAME_2, PARTITIONED, TRANSACTIONAL));

        assertForAllNodes(CACHE_NAME, true);
        assertForAllNodes(CACHE_NAME_2, false);

        assertEquals(cache1.size(), 30);
        assertEquals(cache2.size(), 0);
    }

    /**
     * Test that concurrent enable/disable events doesn't leave to hangs.
     *
     * @throws Exception If failed.
     */
    public void testConcurrentOperations() throws Exception {
        final Ignite srv1 = startGrid(config(SRV_1, false, false));
        final Ignite srv2 = startGrid(config(SRV_2, false, false));
        final Ignite srv3 = startGrid(config(SRV_3, false, true));

        final Ignite cli = startGrid(config(CLI, true, false));

        final Ignite cacheCli = startGrid(config(CLI_2, true, false));

        cacheCli.active(true);

        final IgniteCache cache = cacheCli.getOrCreateCache(cacheConfig(PARTITIONED));

        for (int i = 0; i < 3; i++) {
            // Start pushing requests.
            Collection<Ignite> walNodes = new ArrayList<>();

            walNodes.add(srv1);
            walNodes.add(srv2);
            walNodes.add(srv3);
            walNodes.add(cli);

            final AtomicBoolean done = new AtomicBoolean();

            final CountDownLatch latch = new CountDownLatch(5);

            for (Ignite node : walNodes) {
                final Ignite node0 = node;

                Thread t = new Thread(new Runnable() {
                    @Override public void run() {
                        checkConcurrentOperations(done, node0);

                        latch.countDown();
                    }
                });

                t.setName("wal-load-" + node0.name());

                t.start();
            }

            // Do some cache loading in the mean time.
            Thread t = new Thread(new Runnable() {
                @Override public void run() {
                    int i = 0;

                    while (!done.get())
                        cache.put(i, i++);

                    latch.countDown();
                }
            });

            t.setName("cache-load");

            t.start();

            Thread.sleep(20_000);

            done.set(true);

            latch.await();

            X.println(">>> Iteration finished: " + i);
        }
    }

    /**
     * Check concurrent operations.
     *
     * @param done Done flag.
     * @param node Node.
     */
    private static void checkConcurrentOperations(AtomicBoolean done, Ignite node) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        boolean state = rnd.nextBoolean();

        while (!done.get()) {
            if (state)
                node.cluster().walEnable(CACHE_NAME);
            else
                node.cluster().walDisable(CACHE_NAME);

            state = !state;
        }

        try {
            Thread.sleep(rnd.nextLong(200, 1000));
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
