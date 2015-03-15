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
import org.apache.ignite.lang.*;
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
    private static final String DYNAMIC_CACHE_NAME = "TestDynamicCache";

    /** */
    private static final String STATIC_CACHE_NAME = "TestStaticCache";

    /** */
    private static final String TEST_ATTRIBUTE_NAME = "TEST_ATTRIBUTE_NAME";

    /** */
    public static final IgnitePredicate<ClusterNode> NODE_FILTER = new IgnitePredicate<ClusterNode>() {
        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode n) {
            Boolean val = n.attribute(TEST_ATTRIBUTE_NAME);

            return val != null && val;
        }
    };

    /** */
    private boolean testAttribute = true;

    /**
     * @return Number of nodes for this test.
     */
    public int nodeCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setUserAttributes(F.asMap(TEST_ATTRIBUTE_NAME, testAttribute));

        CacheConfiguration cacheCfg = new CacheConfiguration();

        cacheCfg.setName(STATIC_CACHE_NAME);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
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

                ccfg.setName(DYNAMIC_CACHE_NAME);

                futs.add(kernal.context().cache().dynamicStartCache(ccfg, null));

                return null;
            }
        }, threadNum, "cache-starter");

        assertEquals(threadNum, futs.size());

        int succeeded = 0;
        int failed = 0;

        for (IgniteInternalFuture<?> fut : futs) {
            try {
                fut.get();

                info("Succeeded: " + System.identityHashCode(fut));

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
                futs.add(kernal.context().cache().dynamicStopCache(DYNAMIC_CACHE_NAME));

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

                ccfg.setName(DYNAMIC_CACHE_NAME);

                IgniteKernal kernal = (IgniteKernal)grid(ThreadLocalRandom.current().nextInt(nodeCount()));

                futs.add(kernal.context().cache().dynamicStartCache(ccfg, null));

                return null;
            }
        }, threadNum, "cache-starter");

        assertEquals(threadNum, futs.size());

        int succeeded = 0;
        int failed = 0;

        for (IgniteInternalFuture<?> fut : futs) {
            try {
                fut.get();

                info("Succeeded: " + System.identityHashCode(fut));

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

                futs.add(kernal.context().cache().dynamicStopCache(DYNAMIC_CACHE_NAME));

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

        ccfg.setName(DYNAMIC_CACHE_NAME);

        kernal.context().cache().dynamicStartCache(ccfg, null).get();

        for (int g = 0; g < nodeCount(); g++) {
            IgniteKernal kernal0 = (IgniteKernal)grid(g);

            for (IgniteInternalFuture f : kernal0.context().cache().context().exchange().exchangeFutures())
                f.get();

            info("Getting cache for node: " + g);

            assertNotNull(grid(g).jcache(DYNAMIC_CACHE_NAME));
        }

        grid(0).jcache(DYNAMIC_CACHE_NAME).put("1", "1");

        for (int g = 0; g < nodeCount(); g++)
            assertEquals("1", grid(g).jcache(DYNAMIC_CACHE_NAME).get("1"));

        // Grab caches before stop.
        final IgniteCache[] caches = new IgniteCache[nodeCount()];

        for (int g = 0; g < nodeCount(); g++)
            caches[g] = grid(g).jcache(DYNAMIC_CACHE_NAME);

        kernal.context().cache().dynamicStopCache(DYNAMIC_CACHE_NAME).get();

        for (int g = 0; g < nodeCount(); g++) {
            final IgniteKernal kernal0 = (IgniteKernal)grid(g);

            final int idx = g;

            for (IgniteInternalFuture f : kernal0.context().cache().context().exchange().exchangeFutures())
                f.get();

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    return kernal0.jcache(DYNAMIC_CACHE_NAME);
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

        ccfg.setName(DYNAMIC_CACHE_NAME);

        kernal.context().cache().dynamicStartCache(ccfg, null).get();

        info(">>>>>>> Deployed dynamic cache");

        startGrid(nodeCount());

        try {
            // Check that cache got deployed on new node.
            IgniteCache<Object, Object> cache = ignite(nodeCount()).jcache(DYNAMIC_CACHE_NAME);

            cache.put("1", "1");

            for (int g = 0; g < nodeCount() + 1; g++) {
                assertEquals("1", grid(g).jcache(DYNAMIC_CACHE_NAME).get("1"));

                assertEquals(nodeCount() + 1, grid(g).affinity(DYNAMIC_CACHE_NAME).mapKeyToPrimaryAndBackups(0).size());
            }

            // Undeploy cache.
            kernal.context().cache().dynamicStopCache(DYNAMIC_CACHE_NAME).get();

            startGrid(nodeCount() + 1);

            // Check that cache is not deployed on new node after undeploy.
            for (int g = 0; g < nodeCount() + 2; g++) {
                final IgniteKernal kernal0 = (IgniteKernal)grid(g);

                for (IgniteInternalFuture f : kernal0.context().cache().context().exchange().exchangeFutures())
                    f.get();

                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return kernal0.jcache(DYNAMIC_CACHE_NAME);
                    }
                }, IllegalArgumentException.class, null);
            }
        }
        finally {
            stopGrid(nodeCount() + 1);
            stopGrid(nodeCount());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeployFilter() throws Exception {
        try {
            testAttribute = false;

            startGrid(nodeCount());

            final IgniteKernal kernal = (IgniteKernal)grid(0);

            CacheConfiguration ccfg = new CacheConfiguration();
            ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

            ccfg.setName(DYNAMIC_CACHE_NAME);

            ccfg.setNodeFilter(NODE_FILTER);

            kernal.context().cache().dynamicStartCache(ccfg, null).get();

            startGrid(nodeCount() + 1);

            for (int i = 0; i < 100; i++)
                grid(0).jcache(DYNAMIC_CACHE_NAME).put(i, i);

            for (int i = 0; i < 100; i++)
                assertEquals(i, grid(1).jcache(DYNAMIC_CACHE_NAME).get(i));

            info("Affinity nodes: " + grid(0).affinity(DYNAMIC_CACHE_NAME).mapKeyToPrimaryAndBackups(0));

            for (int g = 0; g < nodeCount(); g++) {
                for (int i = 0; i < 100; i++) {
                    assertFalse(grid(g).affinity(DYNAMIC_CACHE_NAME).mapKeyToPrimaryAndBackups(i)
                        .contains(grid(nodeCount()).cluster().localNode()));

                    assertFalse(grid(g).affinity(DYNAMIC_CACHE_NAME).mapKeyToPrimaryAndBackups(i)
                        .contains(grid(nodeCount() + 1).cluster().localNode()));
                }
            }

            // Check that cache is not deployed on new node after undeploy.
            for (int g = 0; g < nodeCount() + 2; g++) {
                final IgniteKernal kernal0 = (IgniteKernal)grid(g);

                for (IgniteInternalFuture f : kernal0.context().cache().context().exchange().exchangeFutures())
                    f.get();

                if (g < nodeCount())
                    assertNotNull(grid(g).jcache(DYNAMIC_CACHE_NAME));
                else
                    GridTestUtils.assertThrows(log, new Callable<Object>() {
                        @Override public Object call() throws Exception {
                            return kernal0.cache(DYNAMIC_CACHE_NAME);
                        }
                    }, IllegalArgumentException.class, null);
            }

            kernal.context().cache().dynamicStopCache(DYNAMIC_CACHE_NAME).get();

            stopGrid(nodeCount() + 1);
            stopGrid(nodeCount());
        }
        finally {
            testAttribute = true;
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testFailWhenConfiguredCacheExists() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                final IgniteKernal kernal = (IgniteKernal)grid(0);

                CacheConfiguration ccfg = new CacheConfiguration();
                ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

                // Cache is already configured, should fail.
                ccfg.setName(STATIC_CACHE_NAME);

                ccfg.setNodeFilter(NODE_FILTER);

                return kernal.context().cache().dynamicStartCache(ccfg, null).get();
            }
        }, IgniteCheckedException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientCache() throws Exception {
        try {
            testAttribute = false;

            startGrid(nodeCount());

            final IgniteKernal kernal = (IgniteKernal)grid(0);

            CacheConfiguration ccfg = new CacheConfiguration();
            ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

            ccfg.setName(DYNAMIC_CACHE_NAME);

            ccfg.setNodeFilter(NODE_FILTER);

            kernal.context().cache().dynamicStartCache(ccfg, null).get();

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    IgniteKernal ignite = (IgniteKernal)grid(nodeCount());

                    return ignite.cache(DYNAMIC_CACHE_NAME);
                }
            }, IllegalArgumentException.class, null);

            // Should obtain client cache on new node.
            IgniteCache<Object, Object> clientCache = ignite(nodeCount()).jcache(DYNAMIC_CACHE_NAME);

            clientCache.put("1", "1");

            for (int g = 0; g < nodeCount() + 1; g++)
                assertEquals("1", ignite(g).jcache(DYNAMIC_CACHE_NAME).get("1"));

            kernal.context().cache().dynamicStopCache(DYNAMIC_CACHE_NAME).get();
        }
        finally {
            stopGrid(nodeCount());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartFromClientNode() throws Exception {
        try {
            testAttribute = false;

            startGrid(nodeCount());

            final IgniteKernal kernal = (IgniteKernal)grid(0);

            CacheConfiguration ccfg = new CacheConfiguration();
            ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

            ccfg.setName(DYNAMIC_CACHE_NAME);

            ccfg.setNodeFilter(NODE_FILTER);

            final IgniteKernal started = (IgniteKernal)grid(nodeCount());

            started.context().cache().dynamicStartCache(ccfg, null).get();

            GridCacheAdapter<Object, Object> cache = started.internalCache(DYNAMIC_CACHE_NAME);

            assertNotNull(cache);
            assertFalse(cache.context().affinityNode());

            // Should obtain client cache on new node.
            IgniteCache<Object, Object> clientCache = ignite(nodeCount()).jcache(DYNAMIC_CACHE_NAME);

            clientCache.put("1", "1");

            for (int g = 0; g < nodeCount() + 1; g++)
                assertEquals("1", ignite(g).jcache(DYNAMIC_CACHE_NAME).get("1"));

            kernal.context().cache().dynamicStopCache(DYNAMIC_CACHE_NAME).get();
        }
        finally {
            stopGrid(nodeCount());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartNearCacheFromClientNode() throws Exception {
        try {
            testAttribute = false;

            startGrid(nodeCount());

            final IgniteKernal kernal = (IgniteKernal)grid(0);

            CacheConfiguration ccfg = new CacheConfiguration();
            ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

            ccfg.setName(DYNAMIC_CACHE_NAME);

            ccfg.setNodeFilter(NODE_FILTER);

            final IgniteKernal started = (IgniteKernal)grid(nodeCount());

            NearCacheConfiguration nearCfg = new NearCacheConfiguration();

            started.context().cache().dynamicStartCache(ccfg, nearCfg).get();

            GridCacheAdapter<Object, Object> cache = started.internalCache(DYNAMIC_CACHE_NAME);

            assertNotNull(cache);
            assertFalse(cache.context().affinityNode());
            assertTrue(cache.context().isNear());

            // Should obtain client cache on new node.
            IgniteCache<Object, Object> clientCache = ignite(nodeCount()).jcache(DYNAMIC_CACHE_NAME);

            clientCache.put("1", "1");

            for (int g = 0; g < nodeCount() + 1; g++)
                assertEquals("1", ignite(g).jcache(DYNAMIC_CACHE_NAME).get("1"));

            kernal.context().cache().dynamicStopCache(DYNAMIC_CACHE_NAME).get();
        }
        finally {
            stopGrid(nodeCount());
        }
    }
}
