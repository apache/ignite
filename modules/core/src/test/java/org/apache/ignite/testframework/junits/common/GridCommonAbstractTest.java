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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.integration.CompletionListener;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSession;
import junit.framework.AssertionFailedError;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityFunctionContextImpl;
import org.apache.ignite.internal.processors.cache.CacheGroupDescriptor;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheExplicitLockSpan;
import org.apache.ignite.internal.processors.cache.GridCacheFuture;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxyImpl;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.colocated.GridDhtColocatedCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.cache.local.GridLocalCache;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.processors.service.IgniteServiceProcessor;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyTaskArg;
import org.apache.ignite.internal.visor.verify.VisorIdleVerifyTaskV2;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.mxbean.MXBeanDescription;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_EVENT_DRIVEN_SERVICE_PROCESSOR_ENABLED;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheRebalanceMode.NONE;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.isNearEnabled;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Super class for all common tests.
 */
public abstract class GridCommonAbstractTest extends GridAbstractTest {
    /** Cache peek modes array that consist of only ONHEAP mode. */
    protected static final CachePeekMode[] ONHEAP_PEEK_MODES = new CachePeekMode[] {CachePeekMode.ONHEAP};

    /** Service deployment wait timeout. */
    protected static final int SERVICE_DEPLOYMENT_WAIT_TIMEOUT = 10_000;

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
        return grid(idx).cache(DEFAULT_CACHE_NAME).withAllowAtomicOpsInTx();
    }

    /**
     * Get or create instance of cache specified by K,V types;
     * new instance of cache is created for each pair of types key and value
     *
     * @param clsK Key class.
     * @param clsV Value class.
     * @return cache instance
     */
    protected <K, V> IgniteCache<K, V> jcache(CacheConfiguration ccfg, Class<K> clsK, Class<V> clsV) {
        return jcache(grid(), ccfg, clsK, clsV);
    }

    /**
     * Get or create instance of cache specified by K,V types;
     * new instance of cache is created for each pair of types key and value
     *
     * @param ig Ignite.
     * @param ccfg Cache configuration.
     * @param clsK Key class.
     * @param clsV Value class.
     * @return cache instance
     */
    protected <K, V> IgniteCache<K, V> jcache(Ignite ig, CacheConfiguration ccfg, Class<K> clsK, Class<V> clsV) {
        return jcache(ig, ccfg, clsK.getSimpleName() + "-" + clsV.getSimpleName(), clsK, clsV);
    }

    /**
     * Get or create instance of cache specified by K,V types;
     * new instance of cache is created for each pair of types key and value
     *
     * @param ig Ignite.
     * @param ccfg Cache configuration.
     * @param name Cache name.
     * @param clsK Key class.
     * @param clsV Value class.
     * @return cache instance
     */
    @SuppressWarnings("unchecked")
    protected <K, V> IgniteCache<K, V> jcache(Ignite ig,
        CacheConfiguration ccfg,
        @NotNull String name,
        Class<K> clsK,
        Class<V> clsV) {
        CacheConfiguration<K, V> cc = new CacheConfiguration<>(ccfg);
        cc.setName(name);
        cc.setIndexedTypes(clsK, clsV);

        return ig.getOrCreateCache(cc);
    }

    /**
     * Get or create instance of cache specified by K,V types;
     * new instance of cache is created
     *
     * @param ig Ignite.
     * @param ccfg Cache configuration.
     * @param name Cache name.
     * @return cache instance
     */
    @SuppressWarnings("unchecked")
    protected <K, V> IgniteCache<K, V> jcache(Ignite ig,
        CacheConfiguration ccfg,
        @NotNull String name) {
        CacheConfiguration<K, V> cc = new CacheConfiguration<>(ccfg);
        cc.setName(name);

        return ig.getOrCreateCache(cc);
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
        return ((IgniteKernal)grid(idx)).internalCache(DEFAULT_CACHE_NAME);
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
        return grid().cache(DEFAULT_CACHE_NAME).withAllowAtomicOpsInTx();
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
        return (GridLocalCache<K, V>)((IgniteKernal)grid()).<K, V>internalCache(DEFAULT_CACHE_NAME);
    }

    /**
     * @param cache Cache.
     * @return DHT cache.
     */
    protected static <K, V> GridDhtCacheAdapter<K, V> dht(IgniteCache<K, V> cache) {
        return nearEnabled(cache) ? near(cache).dht() :
            ((IgniteKernal)cache.unwrap(Ignite.class)).<K, V>internalCache(cache.getName()).context().dht();
    }

    /**
     * @return DHT cache.
     */
    protected <K, V> GridDhtCacheAdapter<K, V> dht() {
        GridCacheAdapter<K, V> internalCache = ((IgniteKernal)grid()).internalCache(DEFAULT_CACHE_NAME);

        return internalCache.isNear() ? internalCache.context().near().dht() : internalCache.context().dht();
    }

    /**
     * @param idx Grid index.
     * @return DHT cache.
     */
    protected <K, V> GridDhtCacheAdapter<K, V> dht(int idx) {
        GridCacheAdapter<K, V> internalCache = ((IgniteKernal)grid(idx)).internalCache(DEFAULT_CACHE_NAME);

        return internalCache.isNear() ? internalCache.context().near().dht() : internalCache.context().dht();
    }

    /**
     * @param idx Grid index.
     * @param cache Cache name.
     * @return DHT cache.
     */
    protected <K, V> GridDhtCacheAdapter<K, V> dht(int idx, String cache) {
        GridCacheAdapter<K, V> internalCache = ((IgniteKernal)grid(idx)).internalCache(cache);

        return internalCache.isNear() ? internalCache.context().near().dht() : internalCache.context().dht();
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
    protected static <K, V> boolean nearEnabled(final IgniteCache<K, V> cache) {
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
    protected static <K, V> GridNearCacheAdapter<K, V> near(IgniteCache<K, V> cache) {
        return ((IgniteKernal)cache.unwrap(Ignite.class)).<K, V>internalCache(cache.getName()).context().near();
    }

    /**
     * @param cache Cache.
     * @return Colocated cache.
     */
    protected static <K, V> GridDhtColocatedCache<K, V> colocated(IgniteCache<K, V> cache) {
        return ((IgniteKernal)cache.unwrap(Ignite.class)).<K, V>internalCache(cache.getName()).context().colocated();
    }

    /**
     * @param cache Ignite cache.
     * @return CacheAtomicityMode for given cache.
     */
    public static CacheAtomicityMode atomicityMode(IgniteCache cache) {
        return ((CacheConfiguration)cache.getConfiguration(CacheConfiguration.class)).getAtomicityMode();
    }

    /**
     * @param cache Cache.
     * @param keys Keys.
     * @param replaceExistingValues Replace existing values.
     * @throws Exception If failed.
     */
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
        return ((IgniteKernal)grid()).<K, V>internalCache(DEFAULT_CACHE_NAME).context().near();
    }

    /**
     * @param idx Grid index.
     * @return Near cache.
     */
    protected <K, V> GridNearCacheAdapter<K, V> near(int idx) {
        return ((IgniteKernal)grid(idx)).<K, V>internalCache(DEFAULT_CACHE_NAME).context().near();
    }

    /**
     * @param idx Grid index.
     * @return Colocated cache.
     */
    protected <K, V> GridDhtColocatedCache<K, V> colocated(int idx) {
        return (GridDhtColocatedCache<K, V>)((IgniteKernal)grid(idx)).<K, V>internalCache(DEFAULT_CACHE_NAME);
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
    @Override protected void beforeTest() throws Exception {
        // Disable SSL hostname verifier.
        HttpsURLConnection.setDefaultHostnameVerifier(new HostnameVerifier() {
            @Override public boolean verify(String s, SSLSession sslSes) {
                return true;
            }
        });

        super.beforeTest();
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

        if (awaitPartMapExchange) {
            if (!g.active())
                g.active(true);

            awaitPartitionMapExchange();
        }

        return g;
    }

    /**
     * @throws InterruptedException If interrupted.
     */
    protected void awaitPartitionMapExchange() throws InterruptedException {
        awaitPartitionMapExchange(false, false, null);
    }

    /**
     * @param waitEvicts If {@code true} will wait for evictions finished.
     * @param waitNode2PartUpdate If {@code true} will wait for nodes node2part info update finished.
     * @param nodes Optional nodes. If {@code null} method will wait for all nodes, for non null collection nodes will
     *      be filtered
     * @throws InterruptedException If interrupted.
     */
    protected void awaitPartitionMapExchange(
        boolean waitEvicts,
        boolean waitNode2PartUpdate,
        @Nullable Collection<ClusterNode> nodes
    ) throws InterruptedException {
        awaitPartitionMapExchange(waitEvicts, waitNode2PartUpdate, nodes, false);
    }

    /**
     * @return Maximum time of awaiting PartitionMapExchange operation (in milliseconds)
     */
    protected long getPartitionMapExchangeTimeout() {
        return 30_000;
    }

    /**
     * @param waitEvicts If {@code true} will wait for evictions finished.
     * @param waitNode2PartUpdate If {@code true} will wait for nodes node2part info update finished.
     * @param nodes Optional nodes. If {@code null} method will wait for all nodes, for non null collection nodes will
     *      be filtered
     * @param printPartState If {@code true} will print partition state if evictions not happened.
     * @throws InterruptedException If interrupted.
     */
    @SuppressWarnings("BusyWait")
    protected void awaitPartitionMapExchange(
        boolean waitEvicts,
        boolean waitNode2PartUpdate,
        @Nullable Collection<ClusterNode> nodes,
        boolean printPartState
    ) throws InterruptedException {
        long timeout = getPartitionMapExchangeTimeout();

        long startTime = -1;

        Set<String> names = new HashSet<>();

        Ignite crd = null;

        for (Ignite g : G.allGrids()) {
            ClusterNode node = g.cluster().localNode();

            if (crd == null || node.order() < crd.cluster().localNode().order()) {
                crd = g;

                if (node.order() == 1)
                    break;
            }
        }

        if (crd == null)
            return;

        AffinityTopologyVersion waitTopVer = ((IgniteKernal)crd).context().discovery().topologyVersionEx();

        if (waitTopVer.topologyVersion() <= 0)
            waitTopVer = new AffinityTopologyVersion(1, 0);

        for (Ignite g : G.allGrids()) {
            if (nodes != null && !nodes.contains(g.cluster().localNode()))
                continue;

            IgniteKernal g0 = (IgniteKernal)g;

            names.add(g0.configuration().getIgniteInstanceName());

            if (startTime != -1) {
                if (startTime != g0.context().discovery().gridStartTime())
                    fail("Found nodes from different clusters, probable some test does not stop nodes " +
                        "[allNodes=" + names + ']');
            }
            else
                startTime = g0.context().discovery().gridStartTime();

            if (g.cluster().localNode().isDaemon())
                continue;

            IgniteInternalFuture<?> exchFut =
                g0.context().cache().context().exchange().affinityReadyFuture(waitTopVer);

            if (exchFut != null && !exchFut.isDone()) {
                try {
                    exchFut.get(timeout);
                }
                catch (IgniteCheckedException e) {
                    log.error("Failed to wait for exchange [topVer=" + waitTopVer +
                        ", node=" + g0.name() + ']', e);
                }
            }

            for (IgniteCacheProxy<?, ?> c : g0.context().cache().jcaches()) {
                CacheConfiguration cfg = c.context().config();

                if (cfg == null)
                    continue;

                if (cfg.getCacheMode() != LOCAL &&
                    cfg.getRebalanceMode() != NONE &&
                    g.cluster().nodes().size() > 1) {
                    AffinityFunction aff = cfg.getAffinity();

                    GridDhtCacheAdapter<?, ?> dht = dht(c);

                    GridDhtPartitionTopology top = dht.topology();

                    for (int p = 0; p < aff.partitions(); p++) {
                        long start = 0;

                        for (int i = 0; ; i++) {
                            boolean match = false;

                            GridCachePartitionExchangeManager<?, ?> exchMgr = dht.context().shared().exchange();

                            AffinityTopologyVersion readyVer = exchMgr.readyAffinityVersion();

                            // Wait for the exchange completion (there is a race between ready affinity version
                            // initialization and partition topology version update).
                            // Otherwise, there may be an assertion when printing top.readyTopologyVersion().
                            try {
                                IgniteInternalFuture<?> fut = exchMgr.affinityReadyFuture(readyVer);

                                if (fut != null)
                                    fut.get();
                            }
                            catch (IgniteCheckedException e) {
                                throw new IgniteException(e);
                            }

                            if (readyVer.topologyVersion() > 0 && c.context().started()) {
                                // Must map on updated version of topology.
                                List<ClusterNode> affNodes =
                                    dht.context().affinity().assignment(readyVer).idealAssignment().get(p);

                                int affNodesCnt = affNodes.size();

                                GridDhtTopologyFuture topFut = top.topologyVersionFuture();

                                Collection<ClusterNode> owners = (topFut != null && topFut.isDone()) ?
                                    top.owners(p, AffinityTopologyVersion.NONE) : Collections.<ClusterNode>emptyList();

                                int ownerNodesCnt = owners.size();

                                GridDhtLocalPartition loc = top.localPartition(p, readyVer, false);

                                boolean notPrimary = !affNodes.isEmpty() &&
                                    !exchMgr.rebalanceTopologyVersion().equals(AffinityTopologyVersion.NONE) &&
                                    !affNodes.get(0).equals(dht.context().affinity().primaryByPartition(p, readyVer));

                                if (affNodesCnt != ownerNodesCnt || !affNodes.containsAll(owners) ||
                                    (waitEvicts && loc != null && loc.state() != OWNING) ||
                                    notPrimary) {
                                    if (i % 50 == 0)
                                        LT.warn(log(), "Waiting for topology map update [" +
                                            "igniteInstanceName=" + g.name() +
                                            ", cache=" + cfg.getName() +
                                            ", cacheId=" + dht.context().cacheId() +
                                            ", topVer=" + top.readyTopologyVersion() +
                                            ", p=" + p +
                                            ", affNodesCnt=" + affNodesCnt +
                                            ", ownersCnt=" + ownerNodesCnt +
                                            ", affNodes=" + F.nodeIds(affNodes) +
                                            ", owners=" + F.nodeIds(owners) +
                                            ", topFut=" + topFut +
                                            ", locNode=" + g.cluster().localNode() + ']');
                                }
                                else
                                    match = true;
                            }
                            else {
                                if (i % 50 == 0)
                                    LT.warn(log(), "Waiting for topology map update [" +
                                        "igniteInstanceName=" + g.name() +
                                        ", cache=" + cfg.getName() +
                                        ", cacheId=" + dht.context().cacheId() +
                                        ", topVer=" + top.readyTopologyVersion() +
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

                                    if (printPartState)
                                        printPartitionState(c);

                                    throw new IgniteException("Timeout of waiting for topology map update [" +
                                        "igniteInstanceName=" + g.name() +
                                        ", cache=" + cfg.getName() +
                                        ", cacheId=" + dht.context().cacheId() +
                                        ", topVer=" + top.readyTopologyVersion() +
                                        ", p=" + p +
                                        ", readVer=" + readyVer +
                                        ", locNode=" + g.cluster().localNode() + ']');
                                }

                                Thread.sleep(20); // Busy wait.

                                continue;
                            }

                            if (i > 0)
                                log().warning("Finished waiting for topology map update [igniteInstanceName=" +
                                    g.name() + ", p=" + p + ", duration=" + (System.currentTimeMillis() - start) +
                                    "ms]");

                            break;
                        }
                    }

                    if (waitNode2PartUpdate) {
                        long start = System.currentTimeMillis();

                        boolean failed = true;

                        while (failed) {
                            failed = false;

                            for (GridDhtPartitionMap pMap : top.partitionMap(true).values()) {
                                if (failed)
                                    break;

                                for (Map.Entry entry : pMap.entrySet()) {
                                    if (System.currentTimeMillis() - start > timeout) {
                                        U.dumpThreads(log);

                                        throw new IgniteException("Timeout of waiting for partition state update [" +
                                            "igniteInstanceName=" + g.name() +
                                            ", cache=" + cfg.getName() +
                                            ", cacheId=" + dht.context().cacheId() +
                                            ", topVer=" + top.readyTopologyVersion() +
                                            ", locNode=" + g.cluster().localNode() + ']');
                                    }

                                    if (entry.getValue() != OWNING) {
                                        LT.warn(log(),
                                            "Waiting for correct partition state part=" + entry.getKey()
                                                + ", should be OWNING [state=" + entry.getValue() + "], node=" +
                                                g.name() + ", cache=" + c.getName());

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

        log.info("awaitPartitionMapExchange finished");
    }

    /**
     * @param top Topology.
     * @param topVer Version to wait for.
     * @throws Exception If failed.
     */
    protected final void waitForReadyTopology(final GridDhtPartitionTopology top, final AffinityTopologyVersion topVer)
        throws Exception {
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return topVer.compareTo(top.readyTopologyVersion()) <= 0;
            }
        }, 5000);
    }

    /**
     * @param c Cache proxy.
     */
    protected void printPartitionState(IgniteCache<?, ?> c) {
        printPartitionState(c.getConfiguration(CacheConfiguration.class).getName(), 0);
    }

    /**
     * @param cacheName Cache name.
     * @param firstParts Count partition for print (will be print first count partition).
     *
     * Print partitionState for cache.
     */
    protected void printPartitionState(String cacheName, int firstParts) {
        StringBuilder sb = new StringBuilder();

        sb.append("----preload sync futures----\n");

        for (Ignite ig : G.allGrids()) {
            IgniteKernal k = ((IgniteKernal)ig);

            IgniteInternalFuture<?> syncFut = k.internalCache(cacheName)
                .preloader()
                .syncFuture();

            sb.append("nodeId=")
                .append(k.context().localNodeId())
                .append(" consistentId=")
                .append(k.localNode().consistentId())
                .append(" isDone=")
                .append(syncFut.isDone())
                .append("\n");
        }

        sb.append("----rebalance futures----\n");

        for (Ignite ig : G.allGrids()) {
            IgniteKernal k = ((IgniteKernal)ig);

            IgniteInternalFuture<?> f = k.internalCache(cacheName)
                .preloader()
                .rebalanceFuture();

            try {
                sb.append("nodeId=").append(k.context().localNodeId())
                    .append(" isDone=").append(f.isDone())
                    .append(" res=").append(f.isDone() ? f.get() : "N/A")
                    .append(" topVer=")
                    .append((U.hasField(f, "topVer") ?
                        String.valueOf(U.<Object>field(f, "topVer")) : "[unknown] may be it is finished future"))
                    .append("\n");

                Map<UUID, T2<Long, Collection<Integer>>> remaining = U.field(f, "remaining");

                sb.append("remaining:");

                if (remaining.isEmpty())
                    sb.append("empty\n");
                else
                    for (Map.Entry<UUID, T2<Long, Collection<Integer>>> e : remaining.entrySet())
                        sb.append("\nuuid=").append(e.getKey())
                            .append(" startTime=").append(e.getValue().getKey())
                            .append(" parts=").append(Arrays.toString(e.getValue().getValue().toArray()))
                            .append("\n");

            }
            catch (Throwable e) {
                log.error(e.getMessage());
            }
        }

        sb.append("----partition state----\n");

        for (Ignite g : G.allGrids()) {
            IgniteKernal g0 = (IgniteKernal)g;

            sb.append("localNodeId=").append(g0.localNode().id())
                .append(" grid=").append(g0.name())
                .append("\n");

            IgniteCacheProxy<?, ?> cache = g0.context().cache().jcache(cacheName);

            GridDhtCacheAdapter<?, ?> dht = dht(cache);

            GridDhtPartitionTopology top = dht.topology();

            int parts = firstParts == 0 ? cache.context()
                .config()
                .getAffinity()
                .partitions() : firstParts;

            for (int p = 0; p < parts; p++) {
                AffinityTopologyVersion readyVer = dht.context().shared().exchange().readyAffinityVersion();

                Collection<UUID> affNodes = F.nodeIds(dht.context()
                    .affinity()
                    .assignment(readyVer)
                    .idealAssignment()
                    .get(p));

                GridDhtLocalPartition part = top.localPartition(p, AffinityTopologyVersion.NONE, false);

                sb.append("local part=");

                if (part != null) {
                    sb.append(p).append(" counters=")
                        .append(part.dataStore().partUpdateCounter())
                        .append(" fullSize=")
                        .append(part.fullSize())
                        .append(" state=").append(part.state())
                        .append(" reservations=").append(part.reservations());
                }
                else
                    sb.append(p).append(" is null");

                sb.append(" isAffNode=")
                    .append(affNodes.contains(g0.localNode().id()))
                    .append("\n");

                for (UUID nodeId : F.nodeIds(g0.context().discovery().allNodes())) {
                    if (!nodeId.equals(g0.localNode().id()))
                        sb.append(" nodeId=")
                            .append(nodeId)
                            .append(" part=")
                            .append(p)
                            .append(" counters=")
                            .append(part == null ? "NA" : part.dataStore().partUpdateCounter())
                            .append(" fullSize=")
                            .append(part == null ? "NA" : part.fullSize())
                            .append(" state=")
                            .append(top.partitionState(nodeId, p))
                            .append(" isAffNode=")
                            .append(affNodes.contains(nodeId))
                            .append("\n");
                }
            }

            sb.append("\n");
        }

        log.info("dump partitions state for <" + cacheName + ">:\n" + sb.toString());
    }

    /**
     * Use method for manual rebalancing cache on all nodes. Note that using
     * <pre name="code" class="java">
     *   for (int i = 0; i < G.allGrids(); i++)
     *     grid(i).cache(CACHE_NAME).rebalance().get();
     * </pre>
     * for rebalancing cache will lead to flaky test cases.
     *
     * @param ignite Ignite server instance for getting {@code compute} facade over all cluster nodes.
     * @param cacheName Cache name for manual rebalancing on cluster. Usually used when used when
     * {@link CacheConfiguration#getRebalanceDelay()} configuration parameter set to {@code -1} value.
     * @throws IgniteCheckedException If fails.
     */
    protected void manualCacheRebalancing(Ignite ignite,
        final String cacheName) throws IgniteCheckedException {
        if (ignite.configuration().isClientMode())
            return;

        IgniteFuture<Void> fut =
            ignite.compute().withTimeout(5_000).broadcastAsync(new ManualRebalancer(cacheName));

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return fut.isDone();
            }
        }, 5_000));
    }

    /**
     *
     */
    private static class ManualRebalancer implements IgniteRunnable {
        /** */
        @LoggerResource
        IgniteLogger log;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        private final String cacheName;

        public ManualRebalancer(String cacheName) {
            this.cacheName = cacheName;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            IgniteCache<?, ?> cache = ignite.cache(cacheName);

            assertNotNull(cache);

            while (!cache.rebalance().get()) {
                try {
                    U.sleep(100);
                }
                catch (IgniteInterruptedCheckedException e) {
                    throw new IgniteException(e);
                }
            }

            if (log.isInfoEnabled())
                log.info("Manual rebalance finished [node=" + ignite.name() + ", cache=" + cacheName + "]");
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
    protected List<Integer> primaryKeys(IgniteCache<?, ?> cache, final int cnt, final int startFrom) {
        return findKeys(cache, cnt, startFrom, 0);
    }

    /**
     * @param cache Cache.
     * @param cnt Keys count.
     * @param startFrom Start value for keys search.
     * @return Collection of keys for which given cache is primary.
     */
    protected List<Integer> findKeys(IgniteCache<?, ?> cache, final int cnt, final int startFrom, final int type) {
        return findKeys(null, cache, cnt, startFrom, type);
    }

    /**
     * @param node Node.
     * @param cache Cache.
     * @param cnt Keys count.
     * @param startFrom Start value for keys search.
     * @return Collection of keys for which given cache is primary.
     */
    protected List<Integer> findKeys(@Nullable ClusterNode node, IgniteCache<?, ?> cache,
        final int cnt, final int startFrom, final int type) {
        assert cnt > 0 : cnt;

        final List<Integer> found = new ArrayList<>(cnt);

        final ClusterNode node0 = node != null ? node : localNode(cache);

        final Affinity<Integer> aff = (Affinity<Integer>)affinity(cache);

        try {
            GridTestUtils.waitForCondition(new PA() {
                @Override public boolean apply() {
                    for (int i = startFrom; i < startFrom + 100_000; i++) {
                        Integer key = i;

                        boolean ok;

                        if (type == 0)
                            ok = aff.isPrimary(node0, key);
                        else if (type == 1)
                            ok = aff.isBackup(node0, key);
                        else if (type == 2)
                            ok = !aff.isPrimaryOrBackup(node0, key);
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
    protected <K, V> Set<Cache.Entry<K, V>> entrySet(Iterable<Cache.Entry<K, V>> iterable) {
        Set<Cache.Entry<K, V>> set = new HashSet<>();

        for (Cache.Entry<K, V> entry : iterable)
            set.add(entry);

        return set;
    }

    /**
     * @param cache Cache.
     * @param part Partition.
     * @param cnt Count.
     * @param skipCnt Skip keys from start.
     * @return List of keys for partition.
     */
    protected List<Integer> partitionKeys(IgniteCache<?, ?> cache, int part, int cnt, int skipCnt) {
        IgniteCacheProxyImpl proxy = cache.unwrap(IgniteCacheProxyImpl.class);

        GridCacheContext<?, ?> cctx = proxy.context();

        int k = 0, c = 0, skip0 = 0;

        List<Integer> keys = new ArrayList<>(cnt);

        while(c < cnt) {
            if (cctx.affinity().partition(k) == part) {
                if (skip0 < skipCnt) {
                    k++;
                    skip0++;

                    continue;
                }

                c++;

                keys.add(k);
            }

            k++;
        }

        return keys;
    }

    /**
     * @param cache Cache.
     * @param part Partition.
     * @return Unbounded iterator for partition keys.
     */
    protected Iterator<Integer> partitionKeysIterator(IgniteCache<?, ?> cache, int part) {
        IgniteCacheProxyImpl proxy = cache.unwrap(IgniteCacheProxyImpl.class);

        GridCacheContext<?, ?> cctx = proxy.context();

        return new Iterator<Integer>() {
            int cur, next = 0;

            {
                advance();
            }

            private void advance() {
                while(cctx.affinity().partition(cur = next++) != part);
            }

            @Override public boolean hasNext() {
                return true;
            }

            @Override public Integer next() {
                int tmp = cur;

                advance();

                return tmp;
            }
        };
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
        return findKeys(cache, cnt, startFrom, 1);
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
        return findKeys(cache, cnt, startFrom, 2);
    }

    /**
     * Return list of keys that are primary for given node on current topology,
     * but primary node will change after new node will be added.
     *
     * @param ign Ignite.
     * @param cacheName Cache name.
     * @param size Number of keys.
     * @return List of keys.
     */
    protected final List<Integer> movingKeysAfterJoin(Ignite ign, String cacheName, int size) {
        return movingKeysAfterJoin(ign, cacheName, size, null, null);
    }

    /**
     * Return list of keys that are primary for given node on current topology,
     * but primary node will change after new node will be added.
     *
     * @param ign Ignite.
     * @param cacheName Cache name.
     * @param size Number of keys.
     * @param nodeInitializer Node initializer closure.
     * @param joiningNodeConsistentId Joining node consistent id.
     * @return List of keys.
     */
    protected final List<Integer> movingKeysAfterJoin(Ignite ign, String cacheName, int size,
        @Nullable IgniteInClosure<ClusterNode> nodeInitializer, @Nullable String joiningNodeConsistentId) {
        if (joiningNodeConsistentId == null)
            assertEquals("Expected consistentId is set to node name", ign.name(), ign.cluster().localNode().consistentId());

        ArrayList<ClusterNode> nodes = new ArrayList<>(ign.cluster().nodes());

        List<List<ClusterNode>> calcAff = calcAffinity(ign.cache(cacheName), nodes);

        GridTestNode fakeNode = new GridTestNode(UUID.randomUUID(), null);

        if (nodeInitializer != null)
            nodeInitializer.apply(fakeNode);

        fakeNode.consistentId(joiningNodeConsistentId == null ? getTestIgniteInstanceName(nodes.size()) :
            joiningNodeConsistentId);

        nodes.add(fakeNode);

        List<List<ClusterNode>> calcAff2 = calcAffinity(ign.cache(cacheName), nodes);

        Set<Integer> movedParts = new HashSet<>();

        UUID locId = ign.cluster().localNode().id();

        for (int i = 0; i < calcAff.size(); i++) {
            if (calcAff.get(i).get(0).id().equals(locId) && !calcAff2.get(i).get(0).id().equals(locId))
                movedParts.add(i);
        }

        List<Integer> keys = new ArrayList<>();

        Affinity<Integer> aff = ign.affinity(cacheName);

        for (int i = 0; i < 10_000; i++) {
            int keyPart = aff.partition(i);

            if (movedParts.contains(keyPart)) {
                keys.add(i);

                if (keys.size() == size)
                    break;
            }
        }

        assertEquals("Failed to find moving keys [movedPats=" + movedParts + ", keys=" + keys + ']', size, keys.size());

        return keys;
    }

    /**
     * Returns list of partitions what will be evicted after new node's join.
     *
     * @param ign Node to find evicting partitions.
     * @param cache Cach.
     * @param size Size.
     * @return List of moving partition
     */
    protected List<Integer> evictingPartitionsAfterJoin(Ignite ign, IgniteCache<?, ?> cache, int size) {
        ArrayList<ClusterNode> nodes = new ArrayList<>(ign.cluster().nodes());

        List<List<ClusterNode>> ideal1 = calcAffinity(cache, nodes);

        GridTestNode fakeNode = new GridTestNode(UUID.randomUUID(), null);

        fakeNode.consistentId(getTestIgniteInstanceName(nodes.size()));

        nodes.add(fakeNode);

        List<List<ClusterNode>> ideal2 = calcAffinity(cache, nodes);

        Map<ClusterNode, BitSet> m1 = U.newHashMap(nodes.size());
        Map<ClusterNode, BitSet> m2 = U.newHashMap(nodes.size());

        int parts = cache.getConfiguration(CacheConfiguration.class).getAffinity().partitions();

        for (int p = 0; p < parts; p++) {
            List<ClusterNode> assign1 = new ArrayList<>(ideal1.get(p));
            List<ClusterNode> assign2 = new ArrayList<>(ideal2.get(p));

            final int finalP = p;

            IgniteBiInClosure<Map<ClusterNode, BitSet>, ClusterNode> updater = (map, node) -> {
                BitSet set = map.get(node);

                if (set == null)
                    map.put(node, (set = new BitSet(parts)));

                set.set(finalP);
            };

            for (ClusterNode node : assign1)
                updater.apply(m1, node);

            for (ClusterNode node : assign2)
                updater.apply(m2, node);
        }

        List<Integer> partsToRet = new ArrayList<>(size);

        BitSet before = m1.get(ign.cluster().localNode());
        BitSet after = m2.get(ign.cluster().localNode());

        for (int p = before.nextSetBit(0); p >= 0; p = before.nextSetBit(p+1)) {
            if (!after.get(p)) {
                partsToRet.add(p);

                if (partsToRet.size() == size)
                    break;
            }
        }

        return partsToRet;
    }

    /**
     * @param cache Cache.
     * @param nodes Nodes.
     */
    private List<List<ClusterNode>> calcAffinity(IgniteCache<?, ?> cache, List<ClusterNode> nodes) {
        IgniteCacheProxyImpl proxy = cache.unwrap(IgniteCacheProxyImpl.class);

        GridCacheContext<?, ?> cctx = proxy.context();

        AffinityFunction func = cctx.config().getAffinity();

        AffinityFunctionContext ctx = new GridAffinityFunctionContextImpl(
            nodes,
            null,
            null,
            AffinityTopologyVersion.NONE,
            cctx.config().getBackups());

        return func.assignPartitions(ctx);
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
        return cache.localPeek(key, null);
    }

    /**
     * @param cache Cache.
     * @param key Key.
     */
    protected static <K, V> V localPeekOnHeap(GridCacheAdapter<K, V> cache, K key) throws IgniteCheckedException {
        return cache.localPeek(key, new CachePeekMode[] {CachePeekMode.ONHEAP});
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
        ComputeTaskFuture<R> fut = comp.executeAsync(task, arg);

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
        ComputeTaskFuture<R> fut = comp.executeAsync(taskName, arg);

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
        ComputeTaskFuture<R> fut = comp.executeAsync(taskCls, arg);

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
        IgniteFuture<T> fut = evts.waitForLocalAsync(filter, types);

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

        Affinity<Integer> aff = allGrids.get(0).affinity(DEFAULT_CACHE_NAME);

        Collection<ClusterNode> nodes = aff.mapKeyToPrimaryAndBackups(key);

        for (Ignite ignite : allGrids) {
            if (!nodes.contains(ignite.cluster().localNode()))
                return ignite.cache(DEFAULT_CACHE_NAME);
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

        assertTrue("Expected more than one node for key [key=" + key + ", nodes=" + nodes + ']', nodes.size() > 1);

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

        assertTrue("Expected more than one node for key [key=" + key + ", nodes=" + nodes + ']', nodes.size() > 1);

        Iterator<ClusterNode> it = nodes.iterator();

        it.next(); // Skip primary.

        List<Ignite> backups = new ArrayList<>(nodes.size() - 1);

        while (it.hasNext())
            backups.add(grid(it.next()));

        return backups;
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
    protected static <T> T doInTransaction(Ignite ignite,
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
     *
     */
    protected void cleanPersistenceDir() throws Exception {
        assertTrue("Grids are not stopped", F.isEmpty(G.allGrids()));

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "cp", false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "marshaller", false));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), "binary_meta", false));
    }

    /**
     * @param name Instance name.
     */
    protected void cleanPersistenceDir(String name) throws Exception {
        String dn2DirName = name.replace(".", "_");

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR + "/" + dn2DirName, true));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR + "/wal/" + dn2DirName, true));
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR + "/wal/archive/" + dn2DirName, true));
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

    /**
     * @param expData Expected cache data.
     * @param cacheName Cache name.
     */
    protected final void checkCacheData(Map<?, ?> expData, String cacheName) {
        assert !expData.isEmpty();

        List<Ignite> nodes = G.allGrids();

        assertFalse(nodes.isEmpty());

        for (Ignite node : nodes) {
            IgniteCache<Object, Object> cache = node.cache(cacheName);

            for (Map.Entry<?, ?> e : expData.entrySet()) {
                assertEquals("Invalid value [key=" + e.getKey() + ", node=" + node.name() + ']',
                    e.getValue(),
                    cache.get(e.getKey()));
            }
        }
    }

    /**
     * @param nodesCnt Expected nodes number or {@code -1} to use all nodes.
     * @throws Exception If failed.
     */
    protected final void checkOnePhaseCommitReturnValuesCleaned(final int nodesCnt) throws Exception {
        final List<Ignite> nodes;

        if (nodesCnt == -1) {
            nodes = G.allGrids();

            assertTrue(!nodes.isEmpty());
        }
        else {
            nodes = new ArrayList<>(nodesCnt);

            for (int i = 0; i < nodesCnt; i++)
                nodes.add(grid(i));
        }

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (Ignite node : nodes) {
                    Map completedVersHashMap = completedTxsMap(node);

                    for (Object o : completedVersHashMap.values()) {
                        if (!(o instanceof Boolean))
                            return false;
                    }
                }

                return true;
            }
        }, 5000);

        for (Ignite node : nodes) {
            Map completedVersHashMap = completedTxsMap(node);

            for (Object o : completedVersHashMap.values()) {
                assertTrue("completedVersHashMap contains " + o.getClass().getName() + " instead of boolean. " +
                    "These values should be replaced by boolean after onePhaseCommit finished. " +
                    "[node=" + node.name() + "]", o instanceof Boolean);
            }
        }
    }

    /**
     * Sets baseline topology.
     */
    public void resetBaselineTopology() {
        Ignite node = G.allGrids().get(0);

        node.cluster().setBaselineTopology(node.cluster().topologyVersion());
    }

    /**
     * @param ignite Node.
     * @return Completed txs map.
     */
    private Map completedTxsMap(Ignite ignite) {
        IgniteTxManager tm = ((IgniteKernal)ignite).context().cache().context().tm();

        return U.field(tm, "completedVersHashMap");
    }

    /**
     *
     */
    protected final void checkCacheDiscoveryDataConsistent() {
        Map<Integer, CacheGroupDescriptor> cacheGrps = null;
        Map<String, DynamicCacheDescriptor> caches = null;

        for (Ignite node : G.allGrids()) {
            Map<Integer, CacheGroupDescriptor> cacheGrps0 =
                ((IgniteKernal)node).context().cache().cacheGroupDescriptors();
            Map<String, DynamicCacheDescriptor> caches0 =
                ((IgniteKernal)node).context().cache().cacheDescriptors();

            assertNotNull(cacheGrps0);
            assertNotNull(caches0);

            if (cacheGrps == null) {
                cacheGrps = cacheGrps0;
                caches = caches0;
            }
            else {
                assertEquals(cacheGrps.size(), cacheGrps0.size());

                for (Map.Entry<Integer, CacheGroupDescriptor> e : cacheGrps.entrySet()) {
                    CacheGroupDescriptor desc = e.getValue();
                    CacheGroupDescriptor desc0 = cacheGrps0.get(e.getKey());

                    assertNotNull(desc0);
                    checkGroupDescriptorsData(desc, desc0);
                }

                for (Map.Entry<String, DynamicCacheDescriptor> e : caches.entrySet()) {
                    DynamicCacheDescriptor desc = e.getValue();
                    DynamicCacheDescriptor desc0 = caches.get(e.getKey());

                    assertNotNull(desc0);
                    assertEquals(desc.deploymentId(), desc0.deploymentId());
                    assertEquals(desc.receivedFrom(), desc0.receivedFrom());
                    assertEquals(desc.startTopologyVersion(), desc0.startTopologyVersion());
                    assertEquals(desc.cacheConfiguration().getName(), desc0.cacheConfiguration().getName());
                    assertEquals(desc.cacheConfiguration().getGroupName(), desc0.cacheConfiguration().getGroupName());
                    checkGroupDescriptorsData(desc.groupDescriptor(), desc0.groupDescriptor());
                }
            }
        }
    }

    /**
     * @param desc First descriptor.
     * @param desc0 Second descriptor.
     */
    private void checkGroupDescriptorsData(CacheGroupDescriptor desc, CacheGroupDescriptor desc0) {
        assertEquals(desc.groupName(), desc0.groupName());
        assertEquals(desc.sharedGroup(), desc0.sharedGroup());
        assertEquals(desc.deploymentId(), desc0.deploymentId());
        assertEquals(desc.receivedFrom(), desc0.receivedFrom());
        assertEquals(desc.startTopologyVersion(), desc0.startTopologyVersion());
        assertEquals(desc.config().getName(), desc0.config().getName());
        assertEquals(desc.config().getGroupName(), desc0.config().getGroupName());
        assertEquals(desc.caches(), desc0.caches());
    }

    /**
     * Forces checkpoint on all available nodes.
     *
     * @throws IgniteCheckedException If checkpoint was failed.
     */
    protected void forceCheckpoint() throws IgniteCheckedException {
        forceCheckpoint(G.allGrids());
    }

    /**
     * Forces checkpoint on specified node.
     *
     * @param node Node to force checkpoint on it.
     * @throws IgniteCheckedException If checkpoint was failed.
     */
    protected void forceCheckpoint(Ignite node) throws IgniteCheckedException {
        forceCheckpoint(Collections.singletonList(node));
    }

    /**
     * Forces checkpoint on all specified nodes.
     *
     * @param nodes Nodes to force checkpoint on them.
     * @throws IgniteCheckedException If checkpoint was failed.
     */
    protected void forceCheckpoint(Collection<Ignite> nodes) throws IgniteCheckedException {
        for (Ignite ignite : nodes) {
            if (ignite.cluster().localNode().isClient())
                continue;

            GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)((IgniteEx)ignite).context()
                    .cache().context().database();

            dbMgr.waitForCheckpoint("test");
        }
    }

    /**
     * Compares checksums between primary and backup partitions of specified caches.
     * Works properly only on idle cluster - there may be false positive conflict reports if data in cluster is being
     * concurrently updated.
     *
     * @param ig Ignite instance.
     * @param caches Cache names (if null, all user caches will be verified).
     * @return Conflicts result.
     * @throws IgniteException If none caches or node found.
     */
    protected IdleVerifyResultV2 idleVerify(Ignite ig, String... caches) {
        IgniteEx ig0 = (IgniteEx)ig;

        Set<String> cacheNames = new HashSet<>();

        if (F.isEmpty(caches))
            cacheNames.addAll(ig0.cacheNames());
        else
            Collections.addAll(cacheNames, caches);

        if (cacheNames.isEmpty())
            throw new IgniteException("None cache for checking.");

        ClusterNode node = !ig0.localNode().isClient() ? ig0.localNode() : ig0.cluster().forServers().forRandom().node();

        if (node == null)
            throw new IgniteException("None server node for verification.");

        VisorIdleVerifyTaskArg taskArg = new VisorIdleVerifyTaskArg(cacheNames);

        return ig.compute().execute(
            VisorIdleVerifyTaskV2.class.getName(),
            new VisorTaskArgument<>(node.id(), taskArg, false)
        );
    }

    /**
     * Checks if all txs and mvcc futures are finished.
     */
    protected void checkFutures() {
        for (Ignite ignite : G.allGrids()) {
            IgniteEx ig = (IgniteEx)ignite;

            final Collection<GridCacheFuture<?>> futs = ig.context().cache().context().mvcc().activeFutures();

            boolean hasFutures = false;

            for (GridCacheFuture<?> fut : futs) {
                if (!fut.isDone()) {
                    log.error("Expecting no active future [node=" + ig.localNode().id() + ", fut=" + fut + ']');

                    hasFutures = true;
                }
            }

            if (hasFutures)
                fail("Some cache futures are not finished");

            Collection<IgniteInternalTx> txs = ig.context().cache().context().tm().activeTransactions();

            for (IgniteInternalTx tx : txs)
                log.error("Expecting no active transaction [node=" + ig.localNode().id() + ", tx=" + tx + ']');

            if (!txs.isEmpty())
                fail("Some transaction are not finished");
        }
    }

    /**
     * Load data into single partition.
     *
     * @param p Partition.
     * @param cacheName Cache name.
     * @param total Total keys count.
     * @param skip Skip keys from the beginning.
     * @param putType Optional put type (default putAll). Possible values 0, 1, 2, 3.
     *
     * @return List of last keys.
     */
    protected List<Integer> loadDataToPartition(int p, String gridName, String cacheName, int total, int skip, int... putType) {
        IgniteCache<Integer, Integer> cache = grid(gridName).cache(cacheName);

        List<Integer> keys = partitionKeys(cache, p, total, skip);

        int mode = putType != null && putType.length > 0 ? putType[0] : 0;

        Map<Integer, Integer> map = keys.stream().collect(Collectors.toMap(k -> k, k -> k, (a, b) -> a, TreeMap::new));

        switch (mode) {
            case 0:
                cache.putAll(map);

                break;

            case 1:
                for (Map.Entry<Integer, Integer> entry : map.entrySet())
                    cache.put(entry.getKey(), entry.getValue());

                break;

            default:
                try(IgniteDataStreamer<Integer, Integer> ds = grid(gridName).dataStreamer(cacheName)) {
                    ds.allowOverwrite(mode == 2);

                    ds.addData(map);
                }

                break;
        }

        return keys;
    }

    /**
     * @param ignite Ignite instance.
     * @param topVer Topology version to wait.
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    protected void waitForServicesReadyTopology(final IgniteEx ignite,
        final AffinityTopologyVersion topVer) throws IgniteInterruptedCheckedException {
        if (!(ignite.context().service() instanceof IgniteServiceProcessor))
            return;

        final IgniteServiceProcessor srvcProc = (IgniteServiceProcessor)ignite.context().service();

        GridTestUtils.waitForCondition(() -> {
            AffinityTopologyVersion readyTopVer = srvcProc.deployment().readyTopologyVersion();

            return topVer.compareTo(readyTopVer) <= 0;
        }, SERVICE_DEPLOYMENT_WAIT_TIMEOUT);
    }

    /**
     * @return {@code false} if value of a system property "IGNITE_EVENT_DRIVEN_SERVICE_PROCESSOR_ENABLED" is "false",
     * otherwise {@code true}.
     */
    protected static boolean isEventDrivenServiceProcessorEnabled() {
        return getBoolean(IGNITE_EVENT_DRIVEN_SERVICE_PROCESSOR_ENABLED, true);
    }

    /**
     * Wait for {@link EventType#EVT_NODE_METRICS_UPDATED} event will be receieved.
     *
     * @param countUpdate Number of events.
     */
    protected void awaitMetricsUpdate(int countUpdate) throws InterruptedException {
        awaitMetricsUpdate(countUpdate, G.allGrids());
    }

    /**
     * Wait for {@link EventType#EVT_NODE_METRICS_UPDATED} event will be receieved on specific nodes.
     *
     * @param countUpdate Number of events.
     * @param grids Collection of Ignite instances we are listening for metrics update
     * @throws InterruptedException
     */
    protected void awaitMetricsUpdate(int countUpdate, Collection<Ignite> grids) throws InterruptedException {
        if (countUpdate > 0) {

            final CountDownLatch latch = new CountDownLatch(G.allGrids().size() * grids.size() * countUpdate);

            final IgnitePredicate<Event> lsnr = new IgnitePredicate<Event>() {
                @Override public boolean apply(Event evt) {
                    assert evt.type() == EventType.EVT_NODE_METRICS_UPDATED;

                    latch.countDown();

                    return true;
                }
            };

            for (Ignite g : grids)
                g.events().localListen(lsnr, EventType.EVT_NODE_METRICS_UPDATED);

            // Wait for metrics update.
            assert latch.await(10, TimeUnit.SECONDS);

            for (Ignite g : grids)
                g.events().stopLocalListen(lsnr);
        }
    }

    /**
     * @param partId Partition id.
     * @param gridName Grid name.
     *
     * @return Partition update counter or {@code null} if node is not an owner.
     */
    protected @Nullable PartitionUpdateCounter counter(int partId, String gridName) {
        @Nullable GridDhtLocalPartition locPart =
            internalCache(grid(gridName).cache(DEFAULT_CACHE_NAME)).context().topology().localPartition(partId);

        return locPart == null || locPart.state() != OWNING ? null : locPart.dataStore().partUpdateCounter();
    }

    /**
     * @param partId Partition id.
     * @param cacheName Cache name.
     * @param gridName Grid name.
     *
     * @return Partition update counter or {@code null} if node is not an owner.
     */
    protected @Nullable PartitionUpdateCounter counter(int partId, String cacheName, String gridName) {
        @Nullable GridDhtLocalPartition locPart =
            internalCache(grid(gridName).cache(cacheName)).context().topology().localPartition(partId);

        return locPart == null || locPart.state() != OWNING ? null : locPart.dataStore().partUpdateCounter();
    }

    /**
     * @param res Response.
     */
    protected void assertPartitionsSame(IdleVerifyResultV2 res) throws AssertionFailedError {
        if (res.hasConflicts()) {
            StringBuilder b = new StringBuilder();

            res.print(b::append);

            fail(b.toString());
        }
    }

    /**
     * @param partId Partition.
     * @param withReserveCntr {@code True} to compare reserve counters. Because reserve counters are synced during
     * PME invoking with {@code true} makes sense only after PME was finished.
     */
    protected void assertCountersSame(int partId, boolean withReserveCntr) throws AssertionFailedError {
        PartitionUpdateCounter cntr0 = null;

        List<T3<String, @Nullable PartitionUpdateCounter, Boolean>> cntrMap = G.allGrids().stream().filter(ignite ->
            !ignite.configuration().isClientMode()).map(ignite ->
            new T3<>(ignite.name(), counter(partId, ignite.name()),
                ignite.affinity(DEFAULT_CACHE_NAME).isPrimary(ignite.cluster().localNode(), partId))).collect(toList());

        for (T3<String, PartitionUpdateCounter, Boolean> cntr : cntrMap) {
            if (cntr.get2() == null)
                continue;

            if (cntr0 != null) {
                assertEquals("Expecting same counters [partId=" + partId +
                    ", cntrs=" + cntrMap + ']', cntr0, cntr.get2());

                if (withReserveCntr)
                    assertEquals("Expecting same reservation counters [partId=" + partId +
                            ", cntrs=" + cntrMap + ']',
                        cntr0.reserved(), cntr.get2().reserved());
            }

            cntr0 = cntr.get2();
        }
    }

    /**
     * Checks that return types of all registered ignite metrics methods are correct.
     * Also checks that all classes from {@code namesToCheck} are registered as mbeans.
     *
     * @param ignite Ignite instance to collect metrics from.
     * @param namesToCheck Mbean classes names that must be registered in {@code MBeanServer}.
     * @throws Exception If failed to obtain mbeans.
     */
    protected void validateMbeans(Ignite ignite, String... namesToCheck) throws Exception {
        logMbeansValidation(getNotRegisteredMbeans(ignite, namesToCheck), "Not registered mbeans");
        logMbeansValidation(getInvalidMbeansMethods(ignite), "Invalid metrics methods");
    }

    /**
     * @param ignite Ignite instance to collect metrics from.
     * @param namesToCheck Mbean classes names that must be registered in {@code MBeanServer}.
     * @return {@code Set} of class names that are contained in {@code namesToCheck}
     *          but not registered in {@code MBeanServer}.
     */
    protected Set<String> getNotRegisteredMbeans(Ignite ignite, String... namesToCheck) {
        MBeanServer srv = ignite.configuration().getMBeanServer();

        Set<String> beancClsNames = srv.queryMBeans(null, null).stream()
            .map(ObjectInstance::getClassName)
            .collect(toSet());

        return Arrays.stream(namesToCheck)
            .filter(nameToCheck -> beancClsNames.stream().noneMatch(clsName -> clsName.contains(nameToCheck)))
            .collect(toSet());
    }

    /**
     * @param ignite Ignite instance to collect metrics from.
     * @return {@code Set} of metrics methods that have forbidden return types.
     * @throws Exception If failed to obtain metrics.
     */
    protected Set<String> getInvalidMbeansMethods(Ignite ignite) throws Exception {
        Set<String> sysMetricsPackages = new HashSet<>();
        sysMetricsPackages.add("sun.management");
        sysMetricsPackages.add("javax.management");

        MBeanServer srv = ignite.configuration().getMBeanServer();

        Set<String> invalidMethods = new HashSet<>();

        final Set<ObjectInstance> instances = srv.queryMBeans(null, null);

        for (ObjectInstance instance: instances) {
            final String clsName = instance.getClassName();

            if (sysMetricsPackages.stream().anyMatch(clsName::startsWith))
                continue;

            Class c;

            try {
                c = Class.forName(clsName);
            }
            catch (ClassNotFoundException e) {
                log.warning("Failed to load class: " + clsName);

                continue;
            }

            for (Class interf : c.getInterfaces()) {
                for (Method m : interf.getMethods()) {
                    if (!m.isAnnotationPresent(MXBeanDescription.class))
                        continue;

                    if (!validateMetricsMethod(m))
                        invalidMethods.add(m.toString());
                }
            }
        }

        return invalidMethods;
    }

    /**  */
    private void logMbeansValidation(Set<String> invalidSet, String errorMsgPrefix) {
        if (!invalidSet.isEmpty()) {
            log.info("****************************************");
            log.info(errorMsgPrefix + ":");

            invalidSet.stream()
                .sorted()
                .forEach(log::info);

            log.info("****************************************");

            fail(errorMsgPrefix + " detected^");
        }
    }

    /**
     * Validates return type for metrics method.
     * Validity rules are not carved in stone and can be changed in future.
     * See https://issues.apache.org/jira/browse/IGNITE-12629.
     *
     * @param m Metric method to check.
     * @return {@code True} if method return type is allowed.
     */
    private boolean validateMetricsMethod(Method m) {
        Set<String> primitives = new HashSet<>();
        primitives.add("char");
        primitives.add("short");
        primitives.add("int");
        primitives.add("long");
        primitives.add("double");
        primitives.add("float");
        primitives.add("byte");
        primitives.add("boolean");
        primitives.add("void");

        Set<String> allowedPackages = new HashSet<>();
        allowedPackages.add("java.lang");
        allowedPackages.add("java.util");

        final String returnTypeName = m.getGenericReturnType().getTypeName();

        if (primitives.stream().anyMatch(type -> type.equals(returnTypeName) || (type + "[]").equals(returnTypeName)))
            return true;

        String[] parts = returnTypeName.split("[<>,]");

        for (String part: parts) {
            if (allowedPackages.stream().noneMatch(pack -> part.trim().startsWith(pack)))
                return false;
        }

        return true;
    }
}
