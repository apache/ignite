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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Test for dynamic cache start.
 */
@SuppressWarnings("unchecked")
public class IgniteDynamicCacheStartSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "TestDynamicCache";

    /**
     * @return Number of nodes for this test.
     */
    public int nodeCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(nodeCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartStopCacheMultithreadedSameNode() throws Exception {
        final IgniteKernal kernal = (IgniteKernal)grid(0);

        final Collection<IgniteInternalFuture<?>> futs = new ConcurrentLinkedDeque<>();

        int threadNum = 20;

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                CacheConfiguration ccfg = new CacheConfiguration();

                ccfg.setName(CACHE_NAME);

                futs.add(kernal.context().cache().dynamicStartCache(ccfg, F.<ClusterNode>alwaysTrue()));

                return null;
            }
        }, threadNum, "cache-starter");

        assertEquals(threadNum, futs.size());

        int succeeded = 0;
        int failed = 0;

        for (IgniteInternalFuture<?> fut : futs) {
            try {
                fut.get();

                succeeded++;
            }
            catch (IgniteCheckedException e) {
                info(e.getMessage());

                failed++;
            }
        }

        assertEquals(1, succeeded);
        assertEquals(threadNum - 1, failed);

        futs.clear();

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                futs.add(kernal.context().cache().dynamicStopCache(CACHE_NAME));

                return null;
            }
        }, threadNum, "cache-stopper");

        assertEquals(threadNum, futs.size());

        for (IgniteInternalFuture<?> fut : futs)
            fut.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartCacheMultithreadedDifferentNodes() throws Exception {
        final Collection<IgniteInternalFuture<?>> futs = new ConcurrentLinkedDeque<>();

        int threadNum = 20;

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                CacheConfiguration ccfg = new CacheConfiguration();

                ccfg.setName(CACHE_NAME);

                IgniteKernal kernal = (IgniteKernal)grid(ThreadLocalRandom.current().nextInt(nodeCount()));

                futs.add(kernal.context().cache().dynamicStartCache(ccfg, F.<ClusterNode>alwaysTrue()));

                return null;
            }
        }, threadNum, "cache-starter");

        assertEquals(threadNum, futs.size());

        int succeeded = 0;
        int failed = 0;

        for (IgniteInternalFuture<?> fut : futs) {
            try {
                fut.get();

                succeeded++;
            }
            catch (IgniteCheckedException e) {
                info(e.getMessage());

                failed++;
            }
        }

        assertEquals(1, succeeded);
        assertEquals(threadNum - 1, failed);

        futs.clear();

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                IgniteKernal kernal = (IgniteKernal)grid(ThreadLocalRandom.current().nextInt(nodeCount()));

                futs.add(kernal.context().cache().dynamicStopCache(CACHE_NAME));

                return null;
            }
        }, threadNum, "cache-stopper");

        assertEquals(threadNum, futs.size());

        for (IgniteInternalFuture<?> fut : futs)
            fut.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartStopCacheSimpleTransactional() throws Exception {
        checkStartStopCacheSimple(CacheAtomicityMode.TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartStopCacheSimpleAtomic() throws Exception {
        checkStartStopCacheSimple(CacheAtomicityMode.ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    private void checkStartStopCacheSimple(CacheAtomicityMode mode) throws Exception {
        final IgniteKernal kernal = (IgniteKernal)grid(0);

        CacheConfiguration ccfg = new CacheConfiguration();
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setAtomicityMode(mode);

        ccfg.setName(CACHE_NAME);

        kernal.context().cache().dynamicStartCache(ccfg, F.<ClusterNode>alwaysTrue()).get();

        for (int g = 0; g < nodeCount(); g++) {
            IgniteKernal kernal0 = (IgniteKernal)grid(g);

            for (IgniteInternalFuture f : kernal0.context().cache().context().exchange().exchangeFutures())
                f.get();

            assertNotNull(grid(g).jcache(CACHE_NAME));
        }

        grid(0).jcache(CACHE_NAME).put("1", "1");

        for (int g = 0; g < nodeCount(); g++)
            assertEquals("1", grid(g).jcache(CACHE_NAME).get("1"));

        // Grab caches before stop.
        final IgniteCache[] caches = new IgniteCache[nodeCount()];

        for (int g = 0; g < nodeCount(); g++)
            caches[g] = grid(g).jcache(CACHE_NAME);

        kernal.context().cache().dynamicStopCache(CACHE_NAME).get();

        for (int g = 0; g < nodeCount(); g++) {
            final IgniteKernal kernal0 = (IgniteKernal)grid(g);

            final int idx = g;

            for (IgniteInternalFuture f : kernal0.context().cache().context().exchange().exchangeFutures())
                f.get();

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    return kernal0.jcache(CACHE_NAME);
                }
            }, IllegalArgumentException.class, null);

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    return caches[idx].get("1");
                }
            }, IllegalStateException.class, null);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartStopCacheAddNode() throws Exception {
        final IgniteKernal kernal = (IgniteKernal)grid(0);

        CacheConfiguration ccfg = new CacheConfiguration();
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        ccfg.setName(CACHE_NAME);

        kernal.context().cache().dynamicStartCache(ccfg, F.<ClusterNode>alwaysTrue()).get();

        startGrid(nodeCount());

        try {
            // Check that cache got deployed on new node.
            IgniteCache<Object, Object> cache = ignite(nodeCount()).jcache(CACHE_NAME);

            cache.put("1", "1");

            for (int g = 0; g < nodeCount(); g++)
                assertEquals("1", grid(g).jcache(CACHE_NAME).get("1"));

            // Undeploy cache.
            kernal.context().cache().dynamicStopCache(CACHE_NAME).get(); // TODO debug without get().

            info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

            startGrid(nodeCount() + 1);

            // Check that cache is not deployed on new node after undeploy.
            for (int g = 0; g < nodeCount(); g++) {
                final IgniteKernal kernal0 = (IgniteKernal)grid(g);

                for (IgniteInternalFuture f : kernal0.context().cache().context().exchange().exchangeFutures())
                    f.get();

                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return kernal0.jcache(CACHE_NAME);
                    }
                }, IllegalArgumentException.class, null);
            }
        }
        finally {
            stopGrid(nodeCount() + 1);
            stopGrid(nodeCount());
        }
    }
}
