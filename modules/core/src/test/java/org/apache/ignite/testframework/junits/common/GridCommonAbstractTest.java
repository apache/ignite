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

package org.apache.ignite.testframework.junits.common;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.colocated.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.processors.cache.local.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.testframework.junits.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.integration.*;
import javax.net.ssl.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheRebalanceMode.*;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.*;

/**
 * Super class for all common tests.
 */
public abstract class GridCommonAbstractTest extends GridAbstractTest {
    /**Cache peek modes array that consist of only ONHEAP mode. */
    protected static final CachePeekMode[] ONHEAP_PEEK_MODES = new CachePeekMode[] {CachePeekMode.ONHEAP};

    /**
     * @param startGrid If {@code true}, then grid node will be auto-started.
     */
    protected GridCommonAbstractTest(boolean startGrid) {
        super(startGrid);
    }

    /** */
    protected GridCommonAbstractTest() {
        super(false);
    }

    /**
     * @param idx Grid index.
     * @return Cache.
     */
    protected <K, V> IgniteCache<K, V> jcache(int idx) {
        return grid(idx).cache(null);
    }

    /**
     * @param idx Grid index.
     * @param name Cache name.
     * @return Cache.
     */
    protected <K, V> IgniteCache<K, V> jcache(int idx, String name) {
        return grid(idx).cache(name);
    }

    /**
     * @param idx Grid index.
     * @return Cache.
     */
    protected <K, V> GridCacheAdapter<K, V> internalCache(int idx) {
        return ((IgniteKernal)grid(idx)).internalCache(null);
    }

    /**
     * @param idx Grid index.
     * @param name Cache name.
     * @return Cache.
     */
    protected <K, V> GridCacheAdapter<K, V> internalCache(int idx, String name) {
        return ((IgniteKernal)grid(idx)).internalCache(name);
    }

    /**
     * @param ignite Grid.
     * @param name Cache name.
     * @return Cache.
     */
    protected <K, V> GridCacheAdapter<K, V> internalCache(Ignite ignite, String name) {
        return ((IgniteKernal)ignite).internalCache(name);
    }

    /**
     * @param cache Cache.
     * @return Cache.
     */
    protected <K, V> GridCacheAdapter<K, V> internalCache(IgniteCache<K, V> cache) {
        return ((IgniteKernal)cache.unwrap(Ignite.class)).internalCache(cache.getName());
    }

    /**
     * @return Cache.
     */
    protected <K, V> IgniteCache<K, V> jcache() {
        return grid().cache(null);
    }

    /**
     * @param cache Cache.
     * @return Cache.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    protected <K> Set<K> keySet(IgniteCache<K, ?> cache) {
        Set<K> res = new HashSet<>();

        for (Cache.Entry<K, ?> entry : cache)
            res.add(entry.getKey());

        return res;
    }

    /**
     * @return Cache.
     */
    protected <K, V> GridLocalCache<K, V> local() {
        return (GridLocalCache<K, V>)((IgniteKernal)grid()).<K, V>internalCache();
    }

    /**
     * @param cache Cache.
     * @return DHT cache.
     */
    protected static <K, V> GridDhtCacheAdapter<K, V> dht(IgniteCache<K,V> cache) {
        return nearEnabled(cache) ? near(cache).dht() :
            ((IgniteKernal)cache.unwrap(Ignite.class)).<K, V>internalCache(cache.getName()).context().dht();
    }

    /**
     * @return DHT cache.
     */
    protected <K, V> GridDhtCacheAdapter<K, V> dht() {
        return this.<K, V>near().dht();
    }

    /**
     * @param idx Grid index.
     * @return DHT cache.
     */
    protected <K, V> GridDhtCacheAdapter<K, V> dht(int idx) {
        return this.<K, V>near(idx).dht();
    }

    /**
     * @param idx Grid index.
     * @param cache Cache name.
     * @return DHT cache.
     */
    protected <K, V> GridDhtCacheAdapter<K, V> dht(int idx, String cache) {
        return this.<K, V>near(idx, cache).dht();
    }

    /**
     * @param idx Grid index.
     * @param cache Cache name.
     * @return Colocated cache.
     */
    protected <K, V> GridDhtColocatedCache<K, V> colocated(int idx, String cache) {
        return (GridDhtColocatedCache<K, V>)((IgniteKernal)grid(idx)).internalCache(cache);
    }

    /**
     * @param cache Cache.
     * @return {@code True} if near cache is enabled.
     */
    private static <K, V> boolean nearEnabled(GridCacheAdapter<K, V> cache) {
        return isNearEnabled(cache.configuration());
    }

    /**
     * @param cache Cache.
     * @return {@code True} if near cache is enabled.
     */
    protected static <K, V> boolean nearEnabled(IgniteCache<K,V> cache) {
        CacheConfiguration cfg = ((IgniteKernal)cache.unwrap(Ignite.class)).
            <K, V>internalCache(cache.getName()).context().config();

        return isNearEnabled(cfg);
    }

    /**
     * @param cache Cache.
     * @return Near cache.
     */
    private static <K, V> GridNearCacheAdapter<K, V> near(GridCacheAdapter<K, V> cache) {
        return cache.context().near();
    }

    /**
     * @param cache Cache.
     * @return Near cache.
     */
    protected static <K, V> GridNearCacheAdapter<K, V> near(IgniteCache<K,V> cache) {
        return ((IgniteKernal)cache.unwrap(Ignite.class)).<K, V>internalCache(cache.getName()).context().near();
    }

    /**
     * @param cache Cache.
     * @return Colocated cache.
     */
    protected static <K, V> GridDhtColocatedCache<K, V> colocated(IgniteCache<K,V> cache) {
        return ((IgniteKernal)cache.unwrap(Ignite.class)).<K, V>internalCache(cache.getName()).context().colocated();
    }

    /**
     * @param cache Cache.
     * @param keys Keys.
     * @param replaceExistingValues Replace existing values.
     * @throws Exception If failed.
     */
    protected static <K> void loadAll(Cache<K, ?> cache, Set<K> keys, boolean replaceExistingValues) throws Exception {
        final AtomicReference<Exception> ex = new AtomicReference<>();

        final CountDownLatch latch = new CountDownLatch(1);

        cache.loadAll(keys, replaceExistingValues, new CompletionListener() {
            @Override public void onCompletion() {
                latch.countDown();
            }

            @Override public void onException(Exception e) {
                ex.set(e);

                latch.countDown();
            }
        });

        latch.await();

        if (ex.get() != null)
            throw ex.get();
    }

    /**
     * @param cache Cache.
     * @param key Keys.
     * @param replaceExistingValues Replace existing values.
     * @throws Exception If failed.
     */
    protected static <K> void load(Cache<K, ?> cache, K key, boolean replaceExistingValues) throws Exception {
        loadAll(cache, Collections.singleton(key), replaceExistingValues);
    }

    /**
     * @return Near cache.
     */
    protected <K, V> GridNearCacheAdapter<K, V> near() {
        return ((IgniteKernal)grid()).<K, V>internalCache().context().near();
    }

    /**
     * @param idx Grid index.
     * @return Near cache.
     */
    protected <K, V> GridNearCacheAdapter<K, V> near(int idx) {
        return ((IgniteKernal)grid(idx)).<K, V>internalCache().context().near();
    }

    /**
     * @param idx Grid index.
     * @return Colocated cache.
     */
    protected <K, V> GridDhtColocatedCache<K, V> colocated(int idx) {
        return (GridDhtColocatedCache<K, V>)((IgniteKernal)grid(idx)).<K, V>internalCache();
    }

    /**
     * @param idx Grid index.
     * @param cache Cache name.
     * @return Near cache.
     */
    protected <K, V> GridNearCacheAdapter<K, V> near(int idx, String cache) {
        return ((IgniteKernal)grid(idx)).<K, V>internalCache(cache).context().near();
    }

    /** {@inheritDoc} */
    @Override protected final boolean isJunitFrameworkClass() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected final void setUp() throws Exception {
        // Disable SSL hostname verifier.
        HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
            @Override public boolean verify(String s, SSLSession sslSes) {
                return true;
            }
        });

        getTestCounters().incrementStarted();

        super.setUp();
    }

    /** {@inheritDoc} */
    @Override protected final void tearDown() throws Exception {
        getTestCounters().incrementStopped();

        super.tearDown();
    }

    /** {@inheritDoc} */
    @Override protected final Ignite startGridsMultiThreaded(int cnt) throws Exception {
        return startGridsMultiThreaded(cnt, true);
    }

    /**
     * @param cnt Count.
     * @param awaitPartitionMapExchange If we need to await partition map exchange.
     * @return Ignite.
     * @throws Exception If failed.
     */
    protected final Ignite startGridsMultiThreaded(int cnt, boolean awaitPartitionMapExchange) throws Exception {
        Ignite g = super.startGridsMultiThreaded(cnt);

        if (awaitPartitionMapExchange)
            awaitPartitionMapExchange();

        return g;
    }

    /**
     * @throws InterruptedException If interrupted.
     */
    @SuppressWarnings("BusyWait")
    protected void awaitPartitionMapExchange() throws InterruptedException {
        for (Ignite g : G.allGrids()) {
            IgniteKernal g0 = (IgniteKernal)g;

            for (IgniteCacheProxy<?, ?> c : g0.context().cache().jcaches()) {
                CacheConfiguration cfg = c.context().config();

                if (cfg.getCacheMode() == PARTITIONED &&
                    cfg.getRebalanceMode() != NONE &&
                    g.cluster().nodes().size() > 1) {
                    AffinityFunction aff = cfg.getAffinity();

                    GridDhtCacheAdapter<?, ?> dht = dht(c);

                    GridDhtPartitionTopology top = dht.topology();

                    for (int p = 0; p < aff.partitions(); p++) {
                        long start = 0;

                        for (int i = 0; ; i++) {
                            // Must map on updated version of topology.
                            Collection<ClusterNode> affNodes =
                                g0.affinity(cfg.getName()).mapPartitionToPrimaryAndBackups(p);

                            int exp = affNodes.size();

                            Collection<ClusterNode> owners = top.nodes(p, AffinityTopologyVersion.NONE);

                            int actual = owners.size();

                            if (affNodes.size() != owners.size() || !affNodes.containsAll(owners)) {
                                LT.warn(log(), null, "Waiting for topology map update [" +
                                    "grid=" + g.name() +
                                    ", cache=" + cfg.getName() +
                                    ", cacheId=" + dht.context().cacheId() +
                                    ", p=" + p +
                                    ", affNodesCnt=" + exp +
                                    ", ownersCnt=" + actual +
                                    ", affNodes=" + affNodes +
                                    ", owners=" + owners +
                                    ", locNode=" + g.cluster().localNode() + ']');

                                if (i == 0)
                                    start = System.currentTimeMillis();

                                if (System.currentTimeMillis() - start > 30_000)
                                    throw new IgniteException("Timeout of waiting for topology map update [" +
                                        "grid=" + g.name() +
                                        ", cache=" + cfg.getName() +
                                        ", cacheId=" + dht.context().cacheId() +
                                        ", p=" + p +
                                        ", affNodesCnt=" + exp +
                                        ", ownersCnt=" + actual +
                                        ", affNodes=" + affNodes +
                                        ", owners=" + owners +
                                        ", locNode=" + g.cluster().localNode() + ']');

                                Thread.sleep(200); // Busy wait.

                                continue;
                            }

                            if (i > 0)
                                log().warning("Finished waiting for topology map update [grid=" + g.name() +
                                    ", p=" + p + ", duration=" + (System.currentTimeMillis() - start) + "ms]");

                            break;
                        }
                    }
                }
            }
        }
    }

    /**
     * @param ignite Node.
     */
    public void dumpCacheDebugInfo(Ignite ignite) {
        GridKernalContext ctx = ((IgniteKernal)ignite).context();

        log.error("Cache information update [node=" + ignite.name() +
            ", client=" + ignite.configuration().isClientMode() + ']');

        GridCacheSharedContext cctx = ctx.cache().context();

        log.error("Pending transactions:");

        for (IgniteInternalTx tx : cctx.tm().activeTransactions())
            log.error(">>> " + tx);

        log.error("Pending explicit locks:");

        for (GridCacheExplicitLockSpan lockSpan : cctx.mvcc().activeExplicitLocks())
            log.error(">>> " + lockSpan);

        log.error("Pending cache futures:");

        for (GridCacheFuture<?> fut : cctx.mvcc().activeFutures())
            log.error(">>> " + fut);

        log.error("Pending atomic cache futures:");

        for (GridCacheFuture<?> fut : cctx.mvcc().atomicFutures())
            log.error(">>> " + fut);
    }

    /**
     * @param cache Cache.
     * @return Affinity.
     */
    public static <K> Affinity<K> affinity(IgniteCache<K, ?> cache) {
        return cache.unwrap(Ignite.class).affinity(cache.getName());
    }

    /**
     * @param cache Cache.
     * @return Local node.
     */
    public static ClusterNode localNode(IgniteCache<?, ?> cache) {
        return cache.unwrap(Ignite.class).cluster().localNode();
    }

    /**
     * @param cache Cache.
     * @param cnt Keys count.
     * @param startFrom Start value for keys search.
     * @return Collection of keys for which given cache is primary.
     */
    protected List<Integer> primaryKeys(IgniteCache<?, ?> cache, int cnt, int startFrom) {
        assert cnt > 0 : cnt;

        List<Integer> found = new ArrayList<>(cnt);

        ClusterNode locNode = localNode(cache);

        Affinity<Integer> aff = (Affinity<Integer>)affinity(cache);

        for (int i = startFrom; i < startFrom + 100_000; i++) {
            Integer key = i;

            if (aff.isPrimary(locNode, key)) {
                found.add(key);

                if (found.size() == cnt)
                    return found;
            }
        }

        throw new IgniteException("Unable to find " + cnt + " keys as primary for cache.");
    }

    /**
     * @param iterable Iterator
     * @return Set
     */
    protected <K, V> Set<Cache.Entry<K, V>> entrySet(Iterable<Cache.Entry<K, V>> iterable){
        Set<Cache.Entry<K, V>> set = new HashSet<>();

        for (Cache.Entry<K, V> entry : iterable)
            set.add(entry);

        return set;
    }

    /**
     * @param cache Cache.
     * @param cnt Keys count.
     * @return Collection of keys for which given cache is primary.
     */
    protected List<Integer> primaryKeys(IgniteCache<?, ?> cache, int cnt) {
        return primaryKeys(cache, cnt, 1);
    }

    /**
     * @param cache Cache.
     * @param cnt Keys count.
     * @param startFrom Start value for keys search.
     * @return Collection of keys for which given cache is backup.
     */
    protected List<Integer> backupKeys(IgniteCache<?, ?> cache, int cnt, int startFrom) {
        assert cnt > 0 : cnt;

        List<Integer> found = new ArrayList<>(cnt);

        ClusterNode locNode = localNode(cache);

        Affinity<Integer> aff = affinity((IgniteCache<Integer, ?>)cache);

        for (int i = startFrom; i < startFrom + 100_000; i++) {
            Integer key = i;

            if (aff.isBackup(locNode, key)) {
                found.add(key);

                if (found.size() == cnt)
                    return found;
            }
        }

        throw new IgniteException("Unable to find " + cnt + " keys as backup for cache.");
    }

    /**
     * @param cache Cache.
     * @param cnt Keys count.
     * @param startFrom Start value for keys search.
     * @return Collection of keys for which given cache is neither primary nor backup.
     * @throws IgniteCheckedException If failed.
     */
    protected List<Integer> nearKeys(IgniteCache<?, ?> cache, int cnt, int startFrom)
        throws IgniteCheckedException {
        assert cnt > 0 : cnt;

        List<Integer> found = new ArrayList<>(cnt);

        ClusterNode locNode = localNode(cache);

        Affinity<Integer> aff = affinity((IgniteCache<Integer, ?>)cache);

        for (int i = startFrom; i < startFrom + 100_000; i++) {
            Integer key = i;

            if (!aff.isPrimaryOrBackup(locNode, key)) {
                found.add(key);

                if (found.size() == cnt)
                    return found;
            }
        }

        throw new IgniteCheckedException("Unable to find " + cnt + " keys as near for cache.");
    }

    /**
     * @param cache Cache.
     * @return Collection of keys for which given cache is primary.
     * @throws IgniteCheckedException If failed.
     */
    protected Integer primaryKey(IgniteCache<?, ?> cache)
        throws IgniteCheckedException {
        return primaryKeys(cache, 1, 1).get(0);
    }

    /**
     * @param cache Cache.
     * @return Keys for which given cache is backup.
     * @throws IgniteCheckedException If failed.
     */
    protected Integer backupKey(IgniteCache<?, ?> cache)
        throws IgniteCheckedException {
        return backupKeys(cache, 1, 1).get(0);
    }

    /**
     * @param cache Cache.
     * @return Key for which given cache is neither primary nor backup.
     * @throws IgniteCheckedException If failed.
     */
    protected Integer nearKey(IgniteCache<?, ?> cache)
        throws IgniteCheckedException {
        return nearKeys(cache, 1, 1).get(0);
    }

    /**
     * @param key Key.
     */
    protected <K, V> V dhtPeek(K key) throws IgniteCheckedException {
        return localPeek(this.<K, V>dht(), key);
    }

    /**
     * @param idx Index.
     * @param key Key.
     */
    protected <K, V> V dhtPeek(int idx, K key) throws IgniteCheckedException {
        return localPeek(this.<K, V>dht(idx), key);
    }

    /**
     * @param cache Cache.
     * @param key Key.
     */
    protected <K, V> V nearPeek(IgniteCache<K, V> cache, K key) throws IgniteCheckedException {
        return localPeek(near(cache), key);
    }

    /**
     * @param cache Cache.
     * @param key Key.
     */
    protected static <K, V> V dhtPeek(IgniteCache<K, V> cache, K key) throws IgniteCheckedException {
        return localPeek(dht(cache), key);
    }

    /**
     * @param cache Cache.
     * @param key Key.
     */
    protected static <K, V> V localPeek(GridCacheAdapter<K, V> cache, K key) throws IgniteCheckedException {
        return cache.localPeek(key, ONHEAP_PEEK_MODES, null);
    }

    /**
     * @param comp Compute.
     * @param task Task.
     * @param arg Task argument.
     * @return Task future.
     * @throws IgniteCheckedException If failed.
     */
    protected <R> ComputeTaskFuture<R> executeAsync(IgniteCompute comp, ComputeTask task, @Nullable Object arg)
        throws IgniteCheckedException {
        comp = comp.withAsync();

        assertNull(comp.execute(task, arg));

        ComputeTaskFuture<R> fut = comp.future();

        assertNotNull(fut);

        return fut;
    }

    /**
     * @param comp Compute.
     * @param taskName Task name.
     * @param arg Task argument.
     * @return Task future.
     * @throws IgniteCheckedException If failed.
     */
    protected <R> ComputeTaskFuture<R> executeAsync(IgniteCompute comp, String taskName, @Nullable Object arg)
        throws IgniteCheckedException {
        comp = comp.withAsync();

        assertNull(comp.execute(taskName, arg));

        ComputeTaskFuture<R> fut = comp.future();

        assertNotNull(fut);

        return fut;
    }

    /**
     * @param comp Compute.
     * @param taskCls Task class.
     * @param arg Task argument.
     * @return Task future.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    protected <R> ComputeTaskFuture<R> executeAsync(IgniteCompute comp, Class taskCls, @Nullable Object arg)
        throws IgniteCheckedException {
        comp = comp.withAsync();

        assertNull(comp.execute(taskCls, arg));

        ComputeTaskFuture<R> fut = comp.future();

        assertNotNull(fut);

        return fut;
    }

    /**
     * @param evts Events.
     * @param filter Filter.
     * @param types Events types.
     * @return Future.
     * @throws IgniteCheckedException If failed.
     */
    protected <T extends Event> IgniteFuture<T> waitForLocalEvent(IgniteEvents evts,
        @Nullable IgnitePredicate<T> filter, @Nullable int... types) throws IgniteCheckedException {
        evts = evts.withAsync();

        assertTrue(evts.isAsync());

        assertNull(evts.waitForLocal(filter, types));

        IgniteFuture<T> fut = evts.future();

        assertNotNull(fut);

        return fut;
    }

    /**
     * @param e Exception.
     * @param exCls Ex class.
     */
    protected <T extends IgniteException> void assertCacheExceptionWithCause(RuntimeException e, Class<T> exCls) {
        if (exCls.isAssignableFrom(e.getClass()))
            return;

        if (e.getClass() != CacheException.class
            || e.getCause() == null || !exCls.isAssignableFrom(e.getCause().getClass()))
            throw e;
    }

    /**
     * @param cache Cache.
     */
    protected <K, V> GridCacheAdapter<K, V> cacheFromCtx(IgniteCache<K, V> cache) {
        return ((IgniteKernal)cache.unwrap(Ignite.class)).<K, V>internalCache(cache.getName()).context().cache();
    }

    /**
     * @param ignite Grid.
     * @return {@link org.apache.ignite.IgniteCompute} for given grid's local node.
     */
    protected IgniteCompute forLocal(Ignite ignite) {
        return ignite.compute(ignite.cluster().forLocal());
    }

    /**
     * @param prj Projection.
     * @return {@link org.apache.ignite.IgniteCompute} for given projection.
     */
    protected IgniteCompute compute(ClusterGroup prj) {
        return prj.ignite().compute(prj);
    }

    /**
     * @param prj Projection.
     * @return {@link org.apache.ignite.IgniteMessaging} for given projection.
     */
    protected IgniteMessaging message(ClusterGroup prj) {
        return prj.ignite().message(prj);
    }

    /**
     * @param prj Projection.
     * @return {@link org.apache.ignite.IgniteMessaging} for given projection.
     */
    protected IgniteEvents events(ClusterGroup prj) {
        return prj.ignite().events(prj);
    }

    /**
     * @param cfg Configuration.
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfiguration(IgniteConfiguration cfg, String cacheName) {
        for (CacheConfiguration ccfg : cfg.getCacheConfiguration()) {
            if (F.eq(cacheName, ccfg.getName()))
                return ccfg;
        }

        fail("Failed to find cache configuration for cache: " + cacheName);

        return null;
    }

    /**
     * @param key Key.
     * @return Near cache for key.
     */
    protected IgniteCache<Integer, Integer> nearCache(Integer key) {
        List<Ignite> allGrids = Ignition.allGrids();

        assertFalse("There are no alive nodes.", F.isEmpty(allGrids));

        Affinity<Integer> aff = allGrids.get(0).affinity(null);

        Collection<ClusterNode> nodes = aff.mapKeyToPrimaryAndBackups(key);

        for (Ignite ignite : allGrids) {
            if (!nodes.contains(ignite.cluster().localNode()))
                return ignite.cache(null);
        }

        fail();

        return null;
    }

    /**
     * @param key Key.
     * @param cacheName Cache name.
     * @return Near cache for key.
     */
    protected IgniteCache<Integer, Integer> primaryCache(Integer key, String cacheName) {
        return primaryNode(key, cacheName).cache(cacheName);
    }

    /**
     * @param key Key.
     * @param cacheName Cache name.
     * @return Near cache for key.
     */
    protected IgniteCache<Integer, Integer> backupCache(Integer key, String cacheName) {
        return backupNode(key, cacheName).cache(cacheName);
    }

    /**
     * @param key Key.
     * @param cacheName Cache name.
     * @return Ignite instance which has primary cache for given key.
     */
    protected Ignite primaryNode(Object key, String cacheName) {
        List<Ignite> allGrids = Ignition.allGrids();

        assertFalse("There are no alive nodes.", F.isEmpty(allGrids));

        Ignite ignite = allGrids.get(0);

        Affinity<Object> aff = ignite.affinity(cacheName);

        ClusterNode node = aff.mapKeyToNode(key);

        assertNotNull("There are no cache affinity nodes", node);

        return grid(node);
    }

    /**
     * @param key Key.
     * @param cacheName Cache name.
     * @return Ignite instance which has primary cache for given key.
     */
    protected Ignite backupNode(Object key, String cacheName) {
        List<Ignite> allGrids = Ignition.allGrids();

        assertFalse("There are no alive nodes.", F.isEmpty(allGrids));

        Ignite ignite = allGrids.get(0);

        Affinity<Object> aff = ignite.affinity(cacheName);

        Collection<ClusterNode> nodes = aff.mapKeyToPrimaryAndBackups(key);

        assertTrue("Expected more than one node for key [key=" + key + ", nodes=" + nodes +']', nodes.size() > 1);

        Iterator<ClusterNode> it = nodes.iterator();

        it.next(); // Skip primary.

        return grid(it.next());
    }

    /**
     * In ATOMIC cache with CLOCK mode if key is updated from different nodes at same time
     * only one update wins others are ignored (can happen in test event when updates are executed from
     * different nodes sequentially), this delay is used to avoid lost updates.
     *
     * @param cache Cache.
     * @throws Exception If failed.
     */
    protected void atomicClockModeDelay(IgniteCache cache) throws Exception {
        CacheConfiguration ccfg = (CacheConfiguration)cache.getConfiguration(CacheConfiguration.class);

        if (ccfg.getCacheMode() != LOCAL &&
            ccfg.getAtomicityMode() == CacheAtomicityMode.ATOMIC &&
            ccfg.getAtomicWriteOrderMode() == CacheAtomicWriteOrderMode.CLOCK)
            U.sleep(50);
    }

    /**
     * @param exp Expected.
     * @param act Actual.
     */
    protected void assertEqualsCollections(Collection<?> exp, Collection<?> act) {
        if (exp.size() != act.size())
            fail("Collections are not equal:\nExpected:\t" + exp + "\nActual:\t" + act);

        Iterator<?> it1 = exp.iterator();
        Iterator<?> it2 = act.iterator();

        int idx = 0;

        while (it1.hasNext()) {
            Object item1 = it1.next();
            Object item2 = it2.next();

            if (!F.eq(item1, item2))
                fail("Collections are not equal (position " + idx + "):\nExpected: " + exp + "\nActual:   " + act);

            idx++;
        }
    }
}
