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

package org.apache.ignite.testframework.junits;

import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.H2CacheStoreStrategy;
import org.apache.ignite.internal.processors.cache.MapCacheStoreStrategy;
import org.apache.ignite.internal.processors.cache.TestCacheStoreStrategy;
import org.apache.ignite.internal.util.lang.GridAbsPredicateX;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.configvariations.CacheStartMode;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_TIERED;
import static org.apache.ignite.cache.CacheMemoryMode.ONHEAP_TIERED;

/**
 * Abstract class for cache configuration variations tests.
 */
public abstract class IgniteCacheConfigVariationsAbstractTest extends IgniteConfigVariationsAbstractTest {
    /** */
    protected static final int CLIENT_NEAR_ONLY_IDX = 2;

    /** Test timeout. */
    private static final long TEST_TIMEOUT = 30 * 1000;

    /** */
    protected static TestCacheStoreStrategy storeStgy;

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIMEOUT;
    }

    /** {@inheritDoc} */
    @Override protected final void beforeTestsStarted() throws Exception {
        initStoreStrategy();
        assert testsCfg != null;
        assert !testsCfg.withClients() || testsCfg.gridCount() >= 3;

        assert testsCfg.testedNodeIndex() >= 0 : "testedNodeIdx: " + testedNodeIdx;

        testedNodeIdx = testsCfg.testedNodeIndex();

        if (testsCfg.isStartCache()) {
            final CacheStartMode cacheStartMode = testsCfg.cacheStartMode();
            final int cnt = testsCfg.gridCount();

            if (cacheStartMode == CacheStartMode.STATIC) {
                info("All nodes will be stopped, new " + cnt + " nodes will be started.");

                Ignition.stopAll(true);

                for (int i = 0; i < cnt; i++) {
                    String gridName = getTestGridName(i);

                    IgniteConfiguration cfg = optimize(getConfiguration(gridName));

                    if (i != CLIENT_NODE_IDX && i != CLIENT_NEAR_ONLY_IDX) {
                        CacheConfiguration cc = cacheConfiguration();

                        cc.setName(cacheName());

                        cfg.setCacheConfiguration(cc);
                    }

                    startGrid(gridName, cfg, null);
                }

                if (testsCfg.withClients() && testsCfg.gridCount() > CLIENT_NEAR_ONLY_IDX)
                    grid(CLIENT_NEAR_ONLY_IDX).createNearCache(cacheName(), new NearCacheConfiguration());
            }
            else if (cacheStartMode == null || cacheStartMode == CacheStartMode.DYNAMIC) {
                super.beforeTestsStarted();

                startCachesDinamically();
            }
            else
                throw new IllegalArgumentException("Unknown cache start mode: " + cacheStartMode);
        }

        if (testsCfg.gridCount() > 1)
            checkTopology(testsCfg.gridCount());

        awaitPartitionMapExchange();

        for (int i = 0; i < gridCount(); i++)
            info("Grid " + i + ": " + grid(i).localNode().id());

        if (testsCfg.withClients()) {
            if (testedNodeIdx != SERVER_NODE_IDX)
                assertEquals(testedNodeIdx == CLIENT_NEAR_ONLY_IDX, nearEnabled());

            info(">>> Starting set of tests [testedNodeIdx=" + testedNodeIdx
                + ", id=" + grid(testedNodeIdx).localNode().id()
                + ", isClient=" + isClientMode()
                + ", nearEnabled=" + nearEnabled() + "]");
        }
    }

    /** Initialize {@link #storeStgy} with respect to the nature of the test */
    void initStoreStrategy() throws IgniteCheckedException {
        if (storeStgy == null)
            storeStgy = isMultiJvm() ? new H2CacheStoreStrategy() : new MapCacheStoreStrategy();
    }

    /**
     * Starts caches dynamically.
     *
     * @throws Exception If failed.
     */
    private void startCachesDinamically() throws Exception {
        for (int i = 0; i < gridCount(); i++) {
            info("Starting cache dinamically on grid: " + i);

            IgniteEx grid = grid(i);

            if (i != CLIENT_NODE_IDX && i != CLIENT_NEAR_ONLY_IDX) {
                CacheConfiguration cc = cacheConfiguration();

                cc.setName(cacheName());

                grid.getOrCreateCache(cc);
            }

            if (testsCfg.withClients() && i == CLIENT_NEAR_ONLY_IDX && grid(i).configuration().isClientMode())
                grid(CLIENT_NEAR_ONLY_IDX).createNearCache(cacheName(), new NearCacheConfiguration());
        }

        awaitPartitionMapExchange();

        for (int i = 0; i < gridCount(); i++)
            assertNotNull(jcache(i));

        for (int i = 0; i < gridCount(); i++)
            assertEquals("Cache is not empty [idx=" + i + ", entrySet=" + jcache(i).localEntries() + ']',
                0, jcache(i).localSize(CachePeekMode.ALL));
    }

    /** {@inheritDoc} */
    @Override protected boolean expectedClient(String testGridName) {
        return getTestGridName(CLIENT_NODE_IDX).equals(testGridName)
            || getTestGridName(CLIENT_NEAR_ONLY_IDX).equals(testGridName);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        if (testsCfg.isStopCache()) {
            for (int i = 0; i < gridCount(); i++) {
                info("Destroing cache on grid: " + i);

                IgniteCache<String, Integer> cache = jcache(i);

                assert i != 0 || cache != null;

                if (cache != null)
                    cache.destroy();
            }
        }

        storeStgy.resetStore();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        if (testsCfg.awaitPartitionMapExchange())
            awaitPartitionMapExchange();

        assert jcache().unwrap(Ignite.class).transactions().tx() == null;

        assertEquals(0, jcache().localSize());
        assertEquals(0, jcache().size());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        Transaction tx = jcache().unwrap(Ignite.class).transactions().tx();

        if (tx != null) {
            tx.close();

            fail("Cache transaction remained after test completion: " + tx);
        }

        String cacheIsNotEmptyMsg = null;

        for (int i = 0; i < gridCount(); i++) {
            info("Checking grid: " + i);

            while (true) {
                try {
                    final int fi = i;

                    boolean cacheIsEmpty = GridTestUtils.waitForCondition(
                        // Preloading may happen as nodes leave, so we need to wait.
                        new GridAbsPredicateX() {
                            @Override public boolean applyx() throws IgniteCheckedException {
                                jcache(fi).removeAll();

                                if (jcache(fi).size(CachePeekMode.ALL) > 0) {
                                    for (Cache.Entry<?, ?> k : jcache(fi).localEntries())
                                        jcache(fi).remove(k.getKey());
                                }

                                int locSize = jcache(fi).localSize(CachePeekMode.ALL);

                                if (locSize != 0) {
                                    info(">>>>> Debug localSize for grid: " + fi + " is " + locSize);
                                    info(">>>>> Debug ONHEAP  localSize for grid: " + fi + " is "
                                        + jcache(fi).localSize(CachePeekMode.ONHEAP));
                                    info(">>>>> Debug OFFHEAP localSize for grid: " + fi + " is "
                                        + jcache(fi).localSize(CachePeekMode.OFFHEAP));
                                    info(">>>>> Debug PRIMARY localSize for grid: " + fi + " is "
                                        + jcache(fi).localSize(CachePeekMode.PRIMARY));
                                    info(">>>>> Debug BACKUP  localSize for grid: " + fi + " is "
                                        + jcache(fi).localSize(CachePeekMode.BACKUP));
                                    info(">>>>> Debug NEAR    localSize for grid: " + fi + " is "
                                        + jcache(fi).localSize(CachePeekMode.NEAR));
                                    info(">>>>> Debug SWAP    localSize for grid: " + fi + " is "
                                        + jcache(fi).localSize(CachePeekMode.SWAP));
                                }

                                return locSize == 0;
                            }
                        }, 10_000);

                    if (cacheIsEmpty)
                        assertTrue("Cache is not empty: " + " localSize = " + jcache(fi).localSize(CachePeekMode.ALL)
                            + ", local entries " + entrySet(jcache(fi).localEntries()), cacheIsEmpty);

                    int primaryKeySize = jcache(i).localSize(CachePeekMode.PRIMARY);
                    int keySize = jcache(i).localSize();
                    int size = jcache(i).localSize();
                    int globalSize = jcache(i).size();
                    int globalPrimarySize = jcache(i).size(CachePeekMode.PRIMARY);

                    info("Size after [idx=" + i +
                        ", size=" + size +
                        ", keySize=" + keySize +
                        ", primarySize=" + primaryKeySize +
                        ", globalSize=" + globalSize +
                        ", globalPrimarySize=" + globalPrimarySize +
                        ", entrySet=" + jcache(i).localEntries() + ']');

                    if (!cacheIsEmpty) {
                        cacheIsNotEmptyMsg = "Cache is not empty: localSize = "
                            + jcache(fi).localSize(CachePeekMode.ALL) + ", local entries "
                            + entrySet(jcache(fi).localEntries());

                        break;
                    }

                    assertEquals("Cache is not empty [idx=" + i + ", entrySet=" + jcache(i).localEntries() + ']',
                        0, jcache(i).localSize(CachePeekMode.ALL));

                    break;
                }
                catch (Exception e) {
                    if (X.hasCause(e, ClusterTopologyCheckedException.class)) {
                        info("Got topology exception while tear down (will retry in 1000ms).");

                        U.sleep(1000);
                    }
                    else
                        throw e;
                }
            }

            if (cacheIsNotEmptyMsg != null)
                break;

            for (Cache.Entry entry : jcache(i).localEntries(CachePeekMode.SWAP))
                jcache(i).remove(entry.getKey());
        }

        assert jcache().unwrap(Ignite.class).transactions().tx() == null;

        if (cacheIsNotEmptyMsg == null)
            assertEquals("Cache is not empty", 0, jcache().localSize(CachePeekMode.ALL));

        storeStgy.resetStore();

        // Restore cache if current cache has garbage.
        if (cacheIsNotEmptyMsg != null) {
            for (int i = 0; i < gridCount(); i++) {
                info("Destroing cache on grid: " + i);

                IgniteCache<String, Integer> cache = jcache(i);

                assert i != 0 || cache != null;

                if (cache != null)
                    cache.destroy();
            }

            assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicateX() {
                @Override public boolean applyx() {
                    for (int i = 0; i < gridCount(); i++) {
                        if (jcache(i) != null)
                            return false;
                    }

                    return true;
                }
            }, 10_000));

            startCachesDinamically();

            log.warning(cacheIsNotEmptyMsg);

            throw new IllegalStateException(cacheIsNotEmptyMsg);
        }

        assertEquals(0, jcache().localSize());
        assertEquals(0, jcache().size());
    }

    /**
     * Put entry to cache store.
     *
     * @param key Key.
     * @param val Value.
     */
    protected void putToStore(Object key, Object val) {
        if (!storeEnabled())
            throw new IllegalStateException("Failed to put to store because store is disabled.");

        storeStgy.putToStore(key, val);
    }

    /**
     * @return Default cache mode.
     */
    protected CacheMode cacheMode() {
        CacheMode mode = cacheConfiguration().getCacheMode();

        return mode == null ? CacheConfiguration.DFLT_CACHE_MODE : mode;
    }

    /**
     * @return Load previous value flag.
     */
    protected boolean isLoadPreviousValue() {
        return cacheConfiguration().isLoadPreviousValue();
    }

    /**
     * @return Cache atomicity mode.
     */
    protected CacheAtomicityMode atomicityMode() {
        return cacheConfiguration().getAtomicityMode();
    }

    /**
     * @return {@code True} if values should be stored off-heap.
     */
    protected CacheMemoryMode memoryMode() {
        return cacheConfiguration().getMemoryMode();
    }

    /**
     * @return {@code True} if swap should happend after localEvict() call.
     */
    protected boolean swapAfterLocalEvict() {
        if (memoryMode() == OFFHEAP_TIERED)
            return false;

        return memoryMode() == ONHEAP_TIERED ? (!offheapEnabled() && swapEnabled()) : swapEnabled();
    }

    /**
     * @return {@code True} if store is enabled.
     */
    protected boolean storeEnabled() {
        return cacheConfiguration().getCacheStoreFactory() != null;
    }

    /**
     * @return {@code True} if offheap memory is enabled.
     */
    protected boolean offheapEnabled() {
        return cacheConfiguration().getOffHeapMaxMemory() >= 0;
    }

    /**
     * @return {@code True} if swap is enabled.
     */
    protected boolean swapEnabled() {
        return cacheConfiguration().isSwapEnabled();
    }

    /**
     * @return {@code true} if near cache should be enabled.
     */
    protected boolean nearEnabled() {
        return grid(testedNodeIdx).cachex(cacheName()).context().isNear();
    }

    /**
     * @return {@code True} if transactions are enabled.
     * @see #txShouldBeUsed()
     */
    protected boolean txEnabled() {
        return atomicityMode() == TRANSACTIONAL;
    }

    /**
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfiguration() {
        return testsCfg.configurationFactory().cacheConfiguration(getTestGridName(testedNodeIdx));
    }

    /**
     * @return {@code True} if transactions should be used.
     */
    protected boolean txShouldBeUsed() {
        return txEnabled() && !isMultiJvm();
    }

    /**
     * @return {@code True} if locking is enabled.
     */
    protected boolean lockingEnabled() {
        return txEnabled();
    }

    /**
     * @return Default cache instance.
     */
    @SuppressWarnings({"unchecked"})
    @Override protected <K, V> IgniteCache<K, V> jcache() {
        return jcache(testedNodeIdx);
    }

    /**
     * @return A not near-only cache.
     */
    protected IgniteCache<String, Integer> serverNodeCache() {
        return jcache(SERVER_NODE_IDX);
    }

    /**
     * @return Cache name.
     */
    protected String cacheName() {
        return "testcache-" + testsCfg.description().hashCode();
    }

    /**
     * @return Transactions instance.
     */
    protected IgniteTransactions transactions() {
        return grid(0).transactions();
    }

    /**
     * @param idx Index of grid.
     * @return Default cache.
     */
    @SuppressWarnings({"unchecked"})
    @Override protected <K, V> IgniteCache<K, V> jcache(int idx) {
        return ignite(idx).cache(cacheName());
    }

    /**
     * @param idx Index of grid.
     * @return Cache context.
     */
    protected GridCacheContext<String, Integer> context(final int idx) {
        if (isRemoteJvm(idx) && !isRemoteJvm())
            throw new UnsupportedOperationException("Operation can't be done automatically via proxy. " +
                "Send task with this logic on remote jvm instead.");

        return ((IgniteKernal)grid(idx)).<String, Integer>internalCache(cacheName()).context();
    }

    /**
     * @param cache Cache.
     * @return {@code True} if cache has OFFHEAP_TIERED memory mode.
     */
    protected static <K, V> boolean offheapTiered(IgniteCache<K, V> cache) {
        return cache.getConfiguration(CacheConfiguration.class).getMemoryMode() == OFFHEAP_TIERED;
    }

    /**
     * Executes regular peek or peek from swap.
     *
     * @param cache Cache projection.
     * @param key Key.
     * @return Value.
     */
    @Nullable protected static <K, V> V peek(IgniteCache<K, V> cache, K key) {
        return offheapTiered(cache) ? cache.localPeek(key, CachePeekMode.SWAP, CachePeekMode.OFFHEAP) :
            cache.localPeek(key, CachePeekMode.ONHEAP);
    }

    /**
     * @param cache Cache.
     * @param key Key.
     * @return {@code True} if cache contains given key.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    protected static boolean containsKey(IgniteCache cache, Object key) throws Exception {
        return offheapTiered(cache) ? cache.localPeek(key, CachePeekMode.OFFHEAP) != null : cache.containsKey(key);
    }
}
