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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.integration.CompletionListener;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheExplicitLockSpan;
import org.apache.ignite.internal.processors.cache.GridCacheFuture;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.colocated.GridDhtColocatedCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap2;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.cache.local.GridLocalCache;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.NONE;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isNearEnabled;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Super class for all common tests.
 */
public abstract class GridCommonAbstractTest extends GridAbstractTest {
    /** Cache peek modes array that consist of only ONHEAP mode. */
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
    protected static <K, V> GridCacheAdapter<K, V> internalCache0(IgniteCache<K, V> cache) {
        if (isMultiJvmObject(cache))
            throw new UnsupportedOperationException("Operation can't be supported automatically for multi jvm " +
                "(send closure instead).");

        return ((IgniteKernal)cache.unwrap(Ignite.class)).internalCache(cache.getName());
    }

    /**
     * @param cache Cache.
     * @return Cache.
     */
    protected <K, V> GridCacheAdapter<K, V> internalCache(IgniteCache<K, V> cache) {
        return internalCache0(cache);
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
    protected static <K, V> boolean nearEnabled(final IgniteCache<K,V> cache) {
        CacheConfiguration cfg = GridAbstractTest.executeOnLocalOrRemoteJvm(cache,
            new TestCacheCallable<K, V, CacheConfiguration>() {
                private static final long serialVersionUID = 0L;

                @Override public CacheConfiguration call(Ignite ignite, IgniteCache<K, V> cache) throws Exception {
                    return ((IgniteKernal)ignite).<K, V>internalCache(cache.getName()).context().config();
                }
            });

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
    @SuppressWarnings("unchecked")
    protected static <K> void loadAll(Cache<K, ?> cache, final Set<K> keys, final boolean replaceExistingValues)
        throws Exception {
        IgniteCache<K, Object> cacheCp = (IgniteCache<K, Object>)cache;

        GridAbstractTest.executeOnLocalOrRemoteJvm(cacheCp, new TestCacheRunnable<K, Object>() {
            private static final long serialVersionUID = -3030833765012500545L;

            @Override public void run(Ignite ignite, IgniteCache<K, Object> cache) throws Exception {
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
        });
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
     * @param awaitPartMapExchange If we need to await partition map exchange.
     * @return Ignite.
     * @throws Exception If failed.
     */
    protected final Ignite startGridsMultiThreaded(int cnt, boolean awaitPartMapExchange) throws Exception {
        Ignite g = super.startGridsMultiThreaded(cnt);

        if (awaitPartMapExchange)
            awaitPartitionMapExchange();

        return g;
    }

    /**
     * @throws InterruptedException If interrupted.
     */
    @SuppressWarnings("BusyWait")
    protected void awaitPartitionMapExchange() throws InterruptedException {
        awaitPartitionMapExchange(false, false);
    }

    /**
     * @param waitEvicts If {@code true} will wait for evictions finished.
     * @param waitNode2PartUpdate If {@code true} will wait for nodes node2part info update finished.
     * @throws InterruptedException If interrupted.
     */
    @SuppressWarnings("BusyWait")
    protected void awaitPartitionMapExchange(boolean waitEvicts, boolean waitNode2PartUpdate) throws InterruptedException {
        long timeout = 30_000;

        for (Ignite g : G.allGrids()) {
            IgniteKernal g0 = (IgniteKernal)g;

            for (IgniteCacheProxy<?, ?> c : g0.context().cache().jcaches()) {
                CacheConfiguration cfg = c.context().config();

                if (cfg == null)
                    continue;

                if (cfg.getCacheMode() == PARTITIONED &&
                    cfg.getRebalanceMode() != NONE &&
                    g.cluster().nodes().size() > 1) {
                    AffinityFunction aff = cfg.getAffinity();

                    GridDhtCacheAdapter<?, ?> dht = dht(c);

                    GridDhtPartitionTopology top = dht.topology();

                    for (int p = 0; p < aff.partitions(); p++) {
                        long start = 0;

                        for (int i = 0; ; i++) {
                            boolean match = false;

                            AffinityTopologyVersion readyVer = dht.context().shared().exchange().readyAffinityVersion();

                            if (readyVer.topologyVersion() > 0 && c.context().started()) {
                                // Must map on updated version of topology.
                                Collection<ClusterNode> affNodes =
                                    dht.context().affinity().assignment(readyVer).idealAssignment().get(p);

                                int exp = affNodes.size();

                                GridDhtTopologyFuture topFut = top.topologyVersionFuture();

                                Collection<ClusterNode> owners = (topFut != null && topFut.isDone()) ?
                                    top.nodes(p, AffinityTopologyVersion.NONE) : Collections.<ClusterNode>emptyList();

                                int actual = owners.size();

                                GridDhtLocalPartition loc = top.localPartition(p, readyVer, false);

                                if (affNodes.size() != owners.size() || !affNodes.containsAll(owners) ||
                                    (waitEvicts && loc != null && loc.state() != GridDhtPartitionState.OWNING)) {
                                    LT.warn(log(), null, "Waiting for topology map update [" +
                                        "grid=" + g.name() +
                                        ", cache=" + cfg.getName() +
                                        ", cacheId=" + dht.context().cacheId() +
                                        ", topVer=" + top.topologyVersion() +
                                        ", p=" + p +
                                        ", affNodesCnt=" + exp +
                                        ", ownersCnt=" + actual +
                                        ", affNodes=" + F.nodeIds(affNodes) +
                                        ", owners=" + F.nodeIds(owners) +
                                        ", topFut=" + topFut +
                                        ", locNode=" + g.cluster().localNode() + ']');
                                }
                                else
                                    match = true;
                            }
                            else {
                                LT.warn(log(), null, "Waiting for topology map update [" +
                                    "grid=" + g.name() +
                                    ", cache=" + cfg.getName() +
                                    ", cacheId=" + dht.context().cacheId() +
                                    ", topVer=" + top.topologyVersion() +
                                    ", started=" + dht.context().started() +
                                    ", p=" + p +
                                    ", readVer=" + readyVer +
                                    ", locNode=" + g.cluster().localNode() + ']');
                            }

                            if (!match) {
                                if (i == 0)
                                    start = System.currentTimeMillis();

                                if (System.currentTimeMillis() - start > timeout) {
                                    U.dumpThreads(log);

                                    throw new IgniteException("Timeout of waiting for topology map update [" +
                                        "grid=" + g.name() +
                                        ", cache=" + cfg.getName() +
                                        ", cacheId=" + dht.context().cacheId() +
                                        ", topVer=" + top.topologyVersion() +
                                        ", p=" + p +
                                        ", readVer=" + readyVer +
                                        ", locNode=" + g.cluster().localNode() + ']');
                                }

                                Thread.sleep(20); // Busy wait.

                                continue;
                            }

                            if (i > 0)
                                log().warning("Finished waiting for topology map update [grid=" + g.name() +
                                    ", p=" + p + ", duration=" + (System.currentTimeMillis() - start) + "ms]");

                            break;
                        }
                    }

                    if (waitNode2PartUpdate) {
                        long start = System.currentTimeMillis();

                        boolean failed = true;

                        while (failed) {
                            failed = false;

                            for (GridDhtPartitionMap2 pMap : top.partitionMap(true).values()) {
                                if (failed)
                                    break;

                                for (Map.Entry entry : pMap.entrySet()) {
                                    if (System.currentTimeMillis() - start > timeout) {
                                        U.dumpThreads(log);

                                        throw new IgniteException("Timeout of waiting for partition state update [" +
                                            "grid=" + g.name() +
                                            ", cache=" + cfg.getName() +
                                            ", cacheId=" + dht.context().cacheId() +
                                            ", topVer=" + top.topologyVersion() +
                                            ", locNode=" + g.cluster().localNode() + ']');
                                    }

                                    if (entry.getValue() != GridDhtPartitionState.OWNING) {
                                        LT.warn(log(), null,
                                            "Waiting for correct partition state, should be OWNING [state=" +
                                                entry.getValue() + "]");

                                        Thread.sleep(200); // Busy wait.

                                        failed = true;

                                        break;
                                    }
                                }
                            }
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
    @SuppressWarnings("unchecked")
    protected List<Integer> primaryKeys(IgniteCache<?, ?> cache, final int cnt, final int startFrom) {
        return findKeys(cache, cnt, startFrom, 0);
    }

    /**
     * @param cache Cache.
     * @param cnt Keys count.
     * @param startFrom Start value for keys search.
     * @return Collection of keys for which given cache is primary.
     */
    @SuppressWarnings("unchecked")
    protected List<Integer> findKeys(IgniteCache<?, ?> cache, final int cnt, final int startFrom, final int type) {
        assert cnt > 0 : cnt;

        final List<Integer> found = new ArrayList<>(cnt);

        final ClusterNode locNode = localNode(cache);

        final Affinity<Integer> aff = (Affinity<Integer>)affinity(cache);

        try {
            GridTestUtils.waitForCondition(new PA() {
                @Override public boolean apply() {
                    for (int i = startFrom; i < startFrom + 100_000; i++) {
                        Integer key = i;

                        boolean ok;

                        if (type == 0)
                            ok = aff.isPrimary(locNode, key);
                        else if (type == 1)
                            ok = aff.isBackup(locNode, key);
                        else if (type == 2)
                            ok = !aff.isPrimaryOrBackup(locNode, key);
                        else {
                            fail();

                            return false;
                        }

                        if (ok) {
                            if (!found.contains(key))
                                found.add(key);

                            if (found.size() == cnt)
                                return true;
                        }
                    }

                    return false;
                }
            }, 5000);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }

        if (found.size() != cnt)
            throw new IgniteException("Unable to find " + cnt + " requied keys.");

        return found;
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
    @SuppressWarnings("unchecked")
    protected List<Integer> backupKeys(IgniteCache<?, ?> cache, int cnt, int startFrom) {
        return findKeys(cache, cnt, startFrom, 1);
    }

    /**
     * @param cache Cache.
     * @param cnt Keys count.
     * @param startFrom Start value for keys search.
     * @return Collection of keys for which given cache is neither primary nor backup.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    protected List<Integer> nearKeys(IgniteCache<?, ?> cache, int cnt, int startFrom)
        throws IgniteCheckedException {
        return findKeys(cache, cnt, startFrom, 2);
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
    protected <K, V> IgniteCache<K, V> primaryCache(Object key, String cacheName) {
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
     * @return Ignite instance which has backup cache for given key.
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
     * @param key Key.
     * @param cacheName Cache name.
     * @return Ignite instances which has backup cache for given key.
     */
    protected List<Ignite> backupNodes(Object key, String cacheName) {
        List<Ignite> allGrids = Ignition.allGrids();

        assertFalse("There are no alive nodes.", F.isEmpty(allGrids));

        Ignite ignite = allGrids.get(0);

        Affinity<Object> aff = ignite.affinity(cacheName);

        Collection<ClusterNode> nodes = aff.mapKeyToPrimaryAndBackups(key);

        assertTrue("Expected more than one node for key [key=" + key + ", nodes=" + nodes +']', nodes.size() > 1);

        Iterator<ClusterNode> it = nodes.iterator();

        it.next(); // Skip primary.

        List<Ignite> backups = new ArrayList<>(nodes.size() - 1);

        while (it.hasNext())
            backups.add(grid(it.next()));

        return backups;
    }

    /**
     * In ATOMIC cache with CLOCK mode if key is updated from different nodes at same time
     * only one update wins others are ignored (can happen in test even when updates are executed from
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

    /**
     * @param ignite Ignite instance.
     * @param clo Closure.
     * @return Result of closure execution.
     * @throws Exception If failed.
     */
    protected <T> T doInTransaction(Ignite ignite, Callable<T> clo) throws Exception {
        return doInTransaction(ignite, PESSIMISTIC, REPEATABLE_READ, clo);
    }

    /**
     * @param ignite Ignite instance.
     * @param concurrency Transaction concurrency.
     * @param isolation Transaction isolation.
     * @param clo Closure.
     * @return Result of closure execution.
     * @throws Exception If failed.
     */
    protected <T> T doInTransaction(Ignite ignite,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation,
        Callable<T> clo) throws Exception {
        while (true) {
            try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                T res = clo.call();

                tx.commit();

                return res;
            }
            catch (CacheException e) {
                if (e.getCause() instanceof ClusterTopologyException) {
                    ClusterTopologyException topEx = (ClusterTopologyException)e.getCause();

                    topEx.retryReadyFuture().get();
                }
                else
                    throw e;
            }
            catch (ClusterTopologyException e) {
                IgniteFuture<?> fut = e.retryReadyFuture();

                fut.get();
            }
            catch (TransactionRollbackException ignore) {
                // Safe to retry right away.
            }
        }
    }

    /**
     * @param aff Affinity.
     * @param key Counter.
     * @param node Target node.
     * @return Key.
     */
    protected final Integer keyForNode(Affinity<Object> aff, AtomicInteger key, ClusterNode node) {
        for (int i = 0; i < 100_000; i++) {
            Integer next = key.getAndIncrement();

            if (aff.mapKeyToNode(next).equals(node))
                return next;
        }

        fail("Failed to find key for node: " + node);

        return null;
    }

    /**
     * @param cache Cache.
     * @param qry Query.
     * @return Query plan.
     */
    protected final String queryPlan(IgniteCache<?, ?> cache, SqlFieldsQuery qry) {
        return (String)cache.query(new SqlFieldsQuery("explain " + qry.getSql())
            .setArgs(qry.getArgs())
            .setLocal(qry.isLocal())
            .setCollocated(qry.isCollocated())
            .setPageSize(qry.getPageSize())
            .setDistributedJoins(qry.isDistributedJoins())
            .setEnforceJoinOrder(qry.isEnforceJoinOrder()))
            .getAll().get(0).get(0);
    }
}
