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

package org.apache.ignite.internal.processors.affinity;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterGroupEmptyCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.util.GridLeanMap;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CX1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.GridClosureCallMode.BROADCAST;
import static org.apache.ignite.internal.processors.affinity.GridAffinityUtils.affinityJob;
import static org.apache.ignite.internal.processors.affinity.GridAffinityUtils.unmarshall;

/**
 * Data affinity processor.
 */
public class GridAffinityProcessor extends GridProcessorAdapter {
    /** Affinity map cleanup delay (ms). */
    private static final long AFFINITY_MAP_CLEAN_UP_DELAY = 3000;

    /** Log. */
    private final IgniteLogger log;

    /** Affinity map. */
    private final ConcurrentSkipListMap<AffinityAssignmentKey, IgniteInternalFuture<AffinityInfo>> affMap = new ConcurrentSkipListMap<>();

    /** Listener. */
    private final GridLocalEventListener lsnr = new GridLocalEventListener() {
        @Override public void onEvent(Event evt) {
            int evtType = evt.type();

            assert evtType == EVT_NODE_FAILED || evtType == EVT_NODE_LEFT;

            if (affMap.isEmpty())
                return; // Skip empty affinity map.

            final DiscoveryEvent discoEvt = (DiscoveryEvent)evt;

            // Clean up affinity functions if such cache no more exists.
            final Collection<String> caches = ctx.cache().cacheNames();

            final Collection<AffinityAssignmentKey> rmv = new HashSet<>();

            for (AffinityAssignmentKey key : affMap.keySet()) {
                if (!caches.contains(key.cacheName) || key.topVer.topologyVersion() < discoEvt.topologyVersion() - 10)
                    rmv.add(key);
            }

            if (!rmv.isEmpty()) {
                ctx.timeout().addTimeoutObject(
                    new GridTimeoutObjectAdapter(
                        IgniteUuid.fromUuid(ctx.localNodeId()),
                        AFFINITY_MAP_CLEAN_UP_DELAY) {
                            @Override public void onTimeout() {
                                affMap.keySet().removeAll(rmv);
                            }
                        });
            }
        }
    };

    /**
     * @param ctx Context.
     */
    public GridAffinityProcessor(GridKernalContext ctx) {
        super(ctx);

        log = ctx.log(GridAffinityProcessor.class);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        ctx.event().addLocalEventListener(lsnr, EVT_NODE_FAILED, EVT_NODE_LEFT);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        ctx.event().removeLocalEventListener(lsnr);
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) throws IgniteCheckedException {
        affMap.clear();
    }

    /**
     * @param cacheName Cache name.
     * @param key Key.
     * @return Key partition.
     * @throws IgniteCheckedException If failed.
     */
    public int partition(String cacheName, Object key) throws IgniteCheckedException {
        return partition(cacheName, key, null);
    }

    /**
     * @param cacheName Cache name (needed only if {@code aff} is not provided.
     * @param key Key.
     * @param aff Affinity information.
     * @return Key partition.
     * @throws IgniteCheckedException If failed.
     */
    public int partition(String cacheName, Object key, @Nullable AffinityInfo aff) throws IgniteCheckedException {
        assert cacheName != null;

        if (key instanceof KeyCacheObject) {
            int part = ((KeyCacheObject)key).partition();

            if (part >= 0)
                return part;
        }

        return partition0(cacheName, key, aff);
    }

    /**
     * @param cacheName Cache name (needed only if {@code aff} is not provided.
     * @param key Key.
     * @param aff Affinity.
     * @return Key partition.
     * @throws IgniteCheckedException If failed.
     */
    public int partition0(String cacheName, Object key, @Nullable AffinityInfo aff) throws IgniteCheckedException {
        assert cacheName != null;

        if (aff == null) {
            aff = affinityCache(cacheName);

            if (aff == null)
                throw new IgniteCheckedException("Failed to get cache affinity (cache was not started " +
                        "yet or cache was already stopped): " + cacheName);
        }

        return aff.affFunc.partition(aff.affinityKey(key));
    }

    /**
     * Maps partition to a node.
     *
     * @param cacheName Cache name.
     * @param partId partition.
     * @param topVer Affinity topology version.
     * @return Picked node.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public ClusterNode mapPartitionToNode(String cacheName, int partId, AffinityTopologyVersion topVer)
        throws IgniteCheckedException {
        assert cacheName != null;

        AffinityInfo affInfo = affinityCache(cacheName, topVer);

        return affInfo != null ? F.first(affInfo.assignment().get(partId)) : null;
    }

    /**
     * Removes cached affinity instances with affinity topology versions less than {@code topVer}.
     *
     * @param topVer topology version.
     */
    public void removeCachedAffinity(AffinityTopologyVersion topVer) {
        assert topVer != null;

        int oldSize = affMap.size();

        Iterator<Map.Entry<AffinityAssignmentKey, IgniteInternalFuture<AffinityInfo>>> it =
            affMap.headMap(new AffinityAssignmentKey(topVer)).entrySet().iterator();

        while (it.hasNext()) {
            Map.Entry<AffinityAssignmentKey, IgniteInternalFuture<AffinityInfo>> entry = it.next();

            assert entry.getValue() != null;

            if (!entry.getValue().isDone())
                continue;

            it.remove();
        }

        if (log.isDebugEnabled())
            log.debug("Affinity cached values were cleared: " + (oldSize - affMap.size()));
    }

    /**
     * Maps keys to nodes for given cache.
     *
     * @param cacheName Cache name.
     * @param keys Keys to map.
     * @return Map of nodes to keys.
     * @throws IgniteCheckedException If failed.
     */
    public <K> Map<ClusterNode, Collection<K>> mapKeysToNodes(String cacheName, @Nullable Collection<? extends K> keys)
        throws IgniteCheckedException {
        assert cacheName != null;

        return keysToNodes(cacheName, keys);
    }

    /**
     * Maps single key to a node.
     *
     * @param cacheName Cache name.
     * @param key Key to map.
     * @return Picked node.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public <K> ClusterNode mapKeyToNode(String cacheName, K key) throws IgniteCheckedException {
        assert cacheName != null;

        Map<ClusterNode, Collection<K>> map = keysToNodes(cacheName, F.asList(key));

        return !F.isEmpty(map) ? F.first(map.keySet()) : null;
    }

    /**
     * Maps single key to a node.
     *
     * @param cacheName Cache name.
     * @param key Key to map.
     * @return Picked node.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public <K> ClusterNode mapKeyToNode(String cacheName, K key, AffinityTopologyVersion topVer)
        throws IgniteCheckedException {
        assert cacheName != null;

        Map<ClusterNode, Collection<K>> map = keysToNodes(cacheName, F.asList(key), topVer);

        return map != null ? F.first(map.keySet()) : null;
    }

    /**
     * Map single key to primary and backup nodes.
     *
     * @param cacheName Cache name.
     * @param key Key to map.
     * @param topVer Topology version.
     * @return Affinity nodes, primary first.
     * @throws IgniteCheckedException If failed.
     */
    public <K> List<ClusterNode> mapKeyToPrimaryAndBackups(String cacheName,
        K key,
        AffinityTopologyVersion topVer)
        throws IgniteCheckedException
    {
        assert cacheName != null;

        A.notNull(key, "key");
        AffinityInfo affInfo = affinityCache(cacheName, topVer);

        if (affInfo == null)
            return Collections.emptyList();

        int part = partition(cacheName, key, affInfo);

        return affInfo.assignment.get(part);
    }

    /**
     * Gets affinity key for cache key.
     *
     * @param cacheName Cache name.
     * @param key Cache key.
     * @return Affinity key.
     * @throws IgniteCheckedException In case of error.
     */
    @Nullable public Object affinityKey(String cacheName, @Nullable Object key) throws IgniteCheckedException {
        assert cacheName != null;

        if (key == null)
            return null;

        AffinityInfo affInfo = affinityCache(cacheName);

        if (affInfo == null)
            return null;

        return affInfo.affinityKey(key);
    }

    /**
     * @param cacheName Cache name.
     * @return Cache affinity.
     */
    public <K> CacheAffinityProxy<K> affinityProxy(String cacheName) {
        CU.validateCacheName(cacheName);

        return new CacheAffinityProxy<>(cacheName);
    }

    /**
     * @param cacheName Cache name.
     * @param keys Keys.
     * @return Affinity map.
     * @throws IgniteCheckedException If failed.
     */
    private <K> Map<ClusterNode, Collection<K>> keysToNodes(@Nullable final String cacheName,
        Collection<? extends K> keys) throws IgniteCheckedException {
        return keysToNodes(cacheName, keys, null);
    }

    /**
     * @param cacheName Cache name.
     * @param keys Keys.
     * @param topVer Topology version.
     * @return Affinity map.
     * @throws IgniteCheckedException If failed.
     */
    private <K> Map<ClusterNode, Collection<K>> keysToNodes(@Nullable final String cacheName,
        Collection<? extends K> keys, AffinityTopologyVersion topVer) throws IgniteCheckedException {
        if (F.isEmpty(keys))
            return Collections.emptyMap();

        AffinityInfo affInfo = affinityCache(cacheName, topVer);

        return affInfo != null ? affinityMap(affInfo, keys) : Collections.emptyMap();
    }

    /**
     * @param cacheName Cache name.
     * @return Affinity cache.
     * @throws IgniteCheckedException In case of error.
     */
    @Nullable private AffinityInfo affinityCache(final String cacheName)
        throws IgniteCheckedException {
        return affinityCache(cacheName, null);
    }

    /**
     * @param cacheName Cache name.
     * @param topVer Topology version.
     * @return Affinity cache.
     * @throws IgniteCheckedException In case of error.
     */
    @Nullable private AffinityInfo affinityCache(final String cacheName, @Nullable AffinityTopologyVersion topVer)
        throws IgniteCheckedException {
        return affinityCacheFuture(cacheName, topVer).get();
    }

    /**
     * @param cacheName Cache name.
     * @param topVer Topology version.
     * @return Affinity cache.
     * @throws IgniteCheckedException In case of error.
     */
    public IgniteInternalFuture<AffinityInfo> affinityCacheFuture(final String cacheName, @Nullable AffinityTopologyVersion topVer)
        throws IgniteCheckedException {
        assert cacheName != null;

        IgniteInternalFuture<AffinityInfo> locFetchFut = localAffinityInfo(cacheName, topVer);

        if (locFetchFut != null)
            return locFetchFut;

        return remoteAffinityInfo(cacheName, topVer);
    }

    /**
     * Tries to fetch affinity info based on local cache affinity info. If cache with the given name is not started
     * locally, will return {@code null}.
     *
     * @param cacheName Cache name to fetch.
     * @param topVer Topology version to use.
     * @return Future with affinity info or {@code null} if cache is not started locally.
     * @throws IgniteCheckedException If failed to start for local cache context initialization.
     */
    private IgniteInternalFuture<AffinityInfo> localAffinityInfo(
        String cacheName,
        @Nullable AffinityTopologyVersion topVer
    ) throws IgniteCheckedException {
        if (topVer == null)
            topVer = ctx.cache().context().exchange().readyAffinityVersion();

        AffinityAssignmentKey key = new AffinityAssignmentKey(cacheName, topVer);

        IgniteInternalFuture<AffinityInfo> fut = affMap.get(key);

        if (fut != null)
            return fut;

        GridCacheAdapter<Object, Object> cache = ctx.cache().internalCache(cacheName);

        if (cache != null) {
            GridCacheContext<Object, Object> cctx = cache.context();

            cctx.awaitStarted();

            AffinityAssignment assign0 = cctx.affinity().assignment(topVer);

            try {
                cctx.gate().enter();
            }
            catch (IllegalStateException ignored) {
                return new GridFinishedFuture<>((AffinityInfo)null);
            }

            try {
                // using legacy GridAffinityAssignment for compatibility.
                AffinityInfo info = new AffinityInfo(
                    cctx.config().getAffinity(),
                    cctx.config().getAffinityMapper(),
                    new GridAffinityAssignment(topVer, assign0.assignment(), assign0.idealAssignment()),
                    cctx.cacheObjectContext()
                );

                GridFinishedFuture<AffinityInfo> fut0 = new GridFinishedFuture<>(info);

                IgniteInternalFuture<AffinityInfo> old = affMap.putIfAbsent(key, fut0);

                if (old != null)
                    return old;

                return fut0;
            }
            finally {
                cctx.gate().leave();
            }
        }

        return null;
    }

    /**
     * Tries to fetch affinity from remote nodes. If there are no nodes with the cache with the given name started,
     * the retured future will be completed with {@code null}.
     *
     * @param cacheName Cache name to fetch affinity.
     * @param topVer Topology version to fetch affinity.
     * @return Affinity assignment fetch future.
     */
    private IgniteInternalFuture<AffinityInfo> remoteAffinityInfo(
        String cacheName,
        @Nullable AffinityTopologyVersion topVer
    ) {
        if (topVer == null)
            topVer = ctx.discovery().topologyVersionEx();

        AffinityAssignmentKey key = new AffinityAssignmentKey(cacheName, topVer);

        List<ClusterNode> cacheNodes = ctx.discovery().cacheNodes(cacheName, topVer);

        DynamicCacheDescriptor desc = ctx.cache().cacheDescriptor(cacheName);

        if (desc == null || F.isEmpty(cacheNodes)) {
            if (ctx.clientDisconnected())
                return new GridFinishedFuture<>(new IgniteClientDisconnectedCheckedException(ctx.cluster().clientReconnectFuture(),
                    "Failed to get affinity mapping, client disconnected."));

            return new GridFinishedFuture<>((AffinityInfo)null);
        }

        if (desc.cacheConfiguration().getCacheMode() == LOCAL)
            return new GridFinishedFuture<>(new IgniteCheckedException("Failed to map keys for LOCAL cache: " + cacheName));

        AffinityFuture fut0 = new AffinityFuture(cacheName, topVer, cacheNodes);

        IgniteInternalFuture<AffinityInfo> old = affMap.putIfAbsent(key, fut0);

        if (old != null)
            return old;

        fut0.getAffinityFromNextNode();

        return fut0;
    }

    /**
     *
     */
    private class AffinityFuture extends GridFutureAdapter<AffinityInfo> {
        /** */
        private final String cacheName;

        /** */
        private final AffinityTopologyVersion topVer;

        /** */
        private final List<ClusterNode> cacheNodes;

        /** */
        private int nodeIdx;

        /**
         * @param cacheName Cache name.
         * @param topVer Topology version.
         * @param cacheNodes Cache nodes.
         */
        AffinityFuture(String cacheName, AffinityTopologyVersion topVer, List<ClusterNode> cacheNodes) {
            this.cacheName = cacheName;
            this.topVer = topVer;
            this.cacheNodes = cacheNodes;
        }

        /**
         *
         */
        void getAffinityFromNextNode() {
            while (nodeIdx < cacheNodes.size()) {
                final ClusterNode node = cacheNodes.get(nodeIdx);

                nodeIdx++;

                if (!ctx.discovery().alive(node.id()))
                    continue;

                affinityInfoFromNode(cacheName, topVer, node).listen(new CI1<IgniteInternalFuture<AffinityInfo>>() {
                    @Override public void apply(IgniteInternalFuture<AffinityInfo> fut) {
                        try {
                            onDone(fut.get());
                        }
                        catch (IgniteCheckedException e) {
                            if (e instanceof ClusterTopologyCheckedException || X.hasCause(e, ClusterTopologyException.class)) {
                                if (log.isDebugEnabled())
                                    log.debug("Failed to get affinity from node, node failed [cache=" + cacheName +
                                            ", node=" + node.id() + ", msg=" + e.getMessage() + ']');

                                getAffinityFromNextNode();

                                return;
                            }

                            if (log.isDebugEnabled())
                                log.debug("Failed to get affinity from node [cache=" + cacheName +
                                        ", node=" + node.id() + ", msg=" + e.getMessage() + ']');

                            onDone(new IgniteCheckedException("Failed to get affinity mapping from node: " + node.id(), e));
                        }
                    }
                });

                return;
            }

            onDone(new ClusterGroupEmptyCheckedException("Failed to get cache affinity, all cache nodes failed: " + cacheName));
        }
    }

    /**
     * Requests {@link AffinityFunction} and {@link AffinityKeyMapper} from remote node.
     *
     * @param cacheName Name of cache on which affinity is requested.
     * @param topVer Topology version.
     * @param n Node from which affinity is requested.
     * @return Affinity future.
     */
    private IgniteInternalFuture<AffinityInfo> affinityInfoFromNode(String cacheName, AffinityTopologyVersion topVer, ClusterNode n) {
        IgniteInternalFuture<GridTuple3<GridAffinityMessage, GridAffinityMessage, GridAffinityAssignment>> fut = ctx.closure()
            .callAsyncNoFailover(BROADCAST, affinityJob(cacheName, topVer), F.asList(n), true/*system pool*/, 0, false);

        return fut.chain(new CX1<IgniteInternalFuture<GridTuple3<GridAffinityMessage, GridAffinityMessage, GridAffinityAssignment>>, AffinityInfo>() {
            @Override public AffinityInfo applyx(IgniteInternalFuture<GridTuple3<GridAffinityMessage, GridAffinityMessage, GridAffinityAssignment>> fut) throws IgniteCheckedException {
                GridTuple3<GridAffinityMessage, GridAffinityMessage, GridAffinityAssignment> t = fut.get();

                AffinityFunction f = (AffinityFunction)unmarshall(ctx, n.id(), t.get1());
                AffinityKeyMapper m = (AffinityKeyMapper)unmarshall(ctx, n.id(), t.get2());

                assert m != null;

                // Bring to initial state.
                f.reset();
                m.reset();

                CacheConfiguration ccfg = ctx.cache().cacheConfiguration(cacheName);

                return new AffinityInfo(f, m, t.get3(), ctx.cacheObjects().contextForCache(ccfg));
            }
        });
    }

    /**
     * @param aff Affinity function.
     * @param keys Keys.
     * @return Affinity map.
     * @throws IgniteCheckedException If failed.
     */
    private <K> Map<ClusterNode, Collection<K>> affinityMap(AffinityInfo aff, Collection<? extends K> keys)
        throws IgniteCheckedException {
        assert aff != null;
        assert !F.isEmpty(keys);

        try {
            if (keys.size() == 1)
                return Collections.singletonMap(primary(aff, F.first(keys)), (Collection<K>)keys);

            Map<ClusterNode, Collection<K>> map = new GridLeanMap<>();

            for (K k : keys) {
                ClusterNode n = primary(aff, k);

                Collection<K> mapped = map.get(n);

                if (mapped == null)
                    map.put(n, mapped = new LinkedList<>());

                mapped.add(k);
            }

            return map;
        }
        catch (IgniteException e) {
            // Affinity calculation may lead to IgniteException if no cache nodes found for pair cacheName+topVer.
            throw new IgniteCheckedException("Failed to get affinity map for keys: " + keys, e);
        }
    }

    /**
     * Get primary node for cached key.
     *
     * @param aff Affinity function.
     * @param key Key to check.
     * @return Primary node for given key.
     * @throws IgniteCheckedException In case of error.
     */
    private <K> ClusterNode primary(AffinityInfo aff, K key) throws IgniteCheckedException {
        int part = aff.affFunc.partition(aff.affinityKey(key));

        Collection<ClusterNode> nodes = aff.assignment.get(part);

        if (F.isEmpty(nodes))
            throw new IgniteCheckedException("Failed to get affinity nodes [aff=" + aff + ", key=" + key + ']');

        return nodes.iterator().next();
    }

    /**
     * @param aff Affinity function.
     * @param nodeFilter Node class.
     * @param backups Number of backups.
     * @param parts Number of partitions.
     * @return Key to find caches with similar affinity.
     */
    public Object similaryAffinityKey(AffinityFunction aff,
        IgnitePredicate<ClusterNode> nodeFilter,
        int backups,
        int parts) {
        return new SimilarAffinityKey(aff.getClass(), nodeFilter.getClass(), backups, parts);
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>>");
        X.println(">>> Affinity processor memory stats [igniteInstanceName=" + ctx.igniteInstanceName() + ']');
        X.println(">>>   affMapSize: " + affMap.size());
    }

    /**
     *
     */
    private static class AffinityInfo {
        /** Affinity function. */
        private AffinityFunction affFunc;

        /** Mapper */
        private AffinityKeyMapper mapper;

        /** Assignment. */
        private GridAffinityAssignment assignment;

        /** */
        private CacheObjectContext cacheObjCtx;

        /**
         * @param affFunc Affinity function.
         * @param mapper Affinity key mapper.
         * @param assignment Partition assignment.
         * @param cacheObjCtx Cache objects context.
         */
        private AffinityInfo(AffinityFunction affFunc,
            AffinityKeyMapper mapper,
            GridAffinityAssignment assignment,
            CacheObjectContext cacheObjCtx) {
            this.affFunc = affFunc;
            this.mapper = mapper;
            this.assignment = assignment;
            this.cacheObjCtx = cacheObjCtx;
        }

        /**
         * @param key Key.
         * @return Affinity key.
         */
        private Object affinityKey(Object key) {
            if (key instanceof CacheObject && !(key instanceof BinaryObject))
                key = ((CacheObject)key).value(cacheObjCtx, false);

            return mapper.affinityKey(key);
        }

        /**
         * @return Cache affinity function.
         */
        private AffinityFunction affinityFunction() {
            return affFunc;
        }

        /**
         * @return Affinity assignment.
         */
        private GridAffinityAssignment assignment() {
            return assignment;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(AffinityInfo.class, this);
        }
    }

    /**
     *
     */
    private static class AffinityAssignmentKey implements Comparable<AffinityAssignmentKey> {
        /** */
        private String cacheName;

        /** */
        private AffinityTopologyVersion topVer;

        /**
         * @param cacheName Cache name.
         * @param topVer Topology version.
         */
        private AffinityAssignmentKey(@NotNull String cacheName, @NotNull AffinityTopologyVersion topVer) {
            this.cacheName = cacheName;
            this.topVer = topVer;
        }

        /**
         * Current constructor should be used only in removeCachedAffinity for creating of the special keys for removing.
         *
         * @param topVer Topology version.
         */
        private AffinityAssignmentKey(@NotNull AffinityTopologyVersion topVer) {
            this.topVer = topVer;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof AffinityAssignmentKey))
                return false;

            AffinityAssignmentKey that = (AffinityAssignmentKey)o;

            return topVer.equals(that.topVer) && F.eq(cacheName, that.cacheName);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = cacheName != null ? cacheName.hashCode() : 0;

            res = 31 * res + topVer.hashCode();

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(AffinityAssignmentKey.class, this);
        }

        /** {@inheritDoc} */
        @Override public int compareTo(AffinityAssignmentKey o) {
            assert o != null;

            if (this == o)
                return 0;

            int res = this.topVer.compareTo(o.topVer);

            // Key with null cache name must be less than any key with not null cache name for the same topVer.
            if (res == 0) {
                if (cacheName == null && o.cacheName != null)
                    return -1;

                if (cacheName != null && o.cacheName == null)
                    return 1;

                if (cacheName == null && o.cacheName == null)
                    return 0;

                return cacheName.compareTo(o.cacheName);
            }

            return res;
        }
    }

    /**
     * Grid cache affinity.
     */
    private class CacheAffinityProxy<K> implements Affinity<K> {
        /** Cache name. */
        private final String cacheName;

        /**
         * @param cacheName Cache name.
         */
        public CacheAffinityProxy(String cacheName) {
            this.cacheName = cacheName;
        }

        /** {@inheritDoc} */
        @Override public int partitions() {
            ctx.gateway().readLock();

            try {
                return cache().affinityFunction().partitions();
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
            finally {
                ctx.gateway().readUnlock();
            }
        }

        /** {@inheritDoc} */
        @Override public int partition(K key) {
            ctx.gateway().readLock();

            try {
                return partition0(cacheName, key, cache());
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
            finally {
                ctx.gateway().readUnlock();
            }
        }

        /** {@inheritDoc} */
        @Override public boolean isPrimary(ClusterNode n, K key) {
            ctx.gateway().readLock();

            try {
                return cache().assignment().primaryPartitions(n.id()).contains(partition(key));
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
            finally {
                ctx.gateway().readUnlock();
            }
        }

        /** {@inheritDoc} */
        @Override public boolean isBackup(ClusterNode n, K key) {
            ctx.gateway().readLock();

            try {
                return cache().assignment().backupPartitions(n.id()).contains(partition(key));
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
            finally {
                ctx.gateway().readUnlock();
            }
        }

        /** {@inheritDoc} */
        @Override public boolean isPrimaryOrBackup(ClusterNode n, K key) {
            ctx.gateway().readLock();

            try {
                return isPrimary(n, key) || isBackup(n, key);
            }
            finally {
                ctx.gateway().readUnlock();
            }
        }

        /** {@inheritDoc} */
        @Override public int[] primaryPartitions(ClusterNode n) {
            ctx.gateway().readLock();

            try {
                Set<Integer> parts = cache().assignment().primaryPartitions(n.id());

                return U.toIntArray(parts);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
            finally {
                ctx.gateway().readUnlock();
            }
        }

        /** {@inheritDoc} */
        @Override public int[] backupPartitions(ClusterNode n) {
            ctx.gateway().readLock();

            try {
                Set<Integer> parts = cache().assignment().backupPartitions(n.id());

                return U.toIntArray(parts);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
            finally {
                ctx.gateway().readUnlock();
            }
        }

        /** {@inheritDoc} */
        @Override public int[] allPartitions(ClusterNode n) {
            ctx.gateway().readLock();

            try {
                GridAffinityAssignment assignment = cache().assignment();

                int[] primary = U.toIntArray(assignment.primaryPartitions(n.id()));
                int[] backup = U.toIntArray(assignment.backupPartitions(n.id()));

                return U.addAll(primary, backup);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
            finally {
                ctx.gateway().readUnlock();
            }
        }

        /** {@inheritDoc} */
        @Override public Object affinityKey(K key) {
            ctx.gateway().readLock();

            try {
                return cache().affinityKey(key);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
            finally {
                ctx.gateway().readUnlock();
            }
        }

        /** {@inheritDoc} */
        @Override public Map<ClusterNode, Collection<K>> mapKeysToNodes(@Nullable Collection<? extends K> keys) {
            ctx.gateway().readLock();

            try {
                if (F.isEmpty(keys))
                    return Collections.emptyMap();

                AffinityInfo affInfo = cache();

                return affinityMap(affInfo, keys);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
            finally {
                ctx.gateway().readUnlock();
            }
        }

        /** {@inheritDoc} */
        @Nullable @Override public ClusterNode mapKeyToNode(K key) {
            A.notNull(key, "key");

            ctx.gateway().readLock();

            try {
                AffinityInfo affInfo = cache();

                Map<ClusterNode, Collection<K>> map = affinityMap(affInfo, Collections.singletonList(key));

                return F.first(map.keySet());
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
            finally {
                ctx.gateway().readUnlock();
            }
        }

        /** {@inheritDoc} */
        @Override public Collection<ClusterNode> mapKeyToPrimaryAndBackups(K key) {
            ctx.gateway().readLock();

            try {
                AffinityInfo aff = cache();

                return aff.assignment().get(GridAffinityProcessor.this.partition(cacheName, key, aff));
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
            finally {
                ctx.gateway().readUnlock();
            }
        }

        /** {@inheritDoc} */
        @Override public ClusterNode mapPartitionToNode(int part) {
            ctx.gateway().readLock();

            try {
                return F.first(cache().assignment().get(part));
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
            finally {
                ctx.gateway().readUnlock();
            }
        }

        /** {@inheritDoc} */
        @Override public Map<Integer, ClusterNode> mapPartitionsToNodes(Collection<Integer> parts) {
            ctx.gateway().readLock();

            try {
                Map<Integer, ClusterNode> map = new HashMap<>();

                if (!F.isEmpty(parts)) {
                    AffinityInfo aff = cache();

                    for (int p : parts)
                        map.put(p, F.first(aff.assignment().get(p)));
                }

                return map;
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
            finally {
                ctx.gateway().readUnlock();
            }
        }

        /** {@inheritDoc} */
        @Override public Collection<ClusterNode> mapPartitionToPrimaryAndBackups(int part) {
            ctx.gateway().readLock();

            try {
                return cache().assignment().get(part);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
            finally {
                ctx.gateway().readUnlock();
            }
        }

        /**
         * @return Affinity info for current topology version.
         * @throws IgniteCheckedException If failed.
         */
        private AffinityInfo cache() throws IgniteCheckedException {
            AffinityInfo aff = affinityCache(cacheName);

            if (aff == null)
                throw new IgniteException("Failed to find cache (cache was not started " +
                    "yet or cache was already stopped): " + cacheName);

            return aff;
        }
    }

    /**
     *
     */
    private static class SimilarAffinityKey {
        /** */
        private final int backups;

        /** */
        private final Class<?> affFuncCls;

        /** */
        private final Class<?> filterCls;

        /** */
        private final int partsCnt;

        /** */
        private final int hash;

        /**
         * @param affFuncCls Affinity function class.
         * @param filterCls Node filter class.
         * @param backups Number of backups.
         * @param partsCnt Number of partitions.
         */
        SimilarAffinityKey(Class<?> affFuncCls, Class<?> filterCls, int backups, int partsCnt) {
            this.backups = backups;
            this.affFuncCls = affFuncCls;
            this.filterCls = filterCls;
            this.partsCnt = partsCnt;

            int hash = backups;
            hash = 31 * hash + affFuncCls.hashCode();
            hash = 31 * hash + filterCls.hashCode();
            hash = 31 * hash + partsCnt;

            this.hash = hash;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return hash;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (o == this)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            SimilarAffinityKey key = (SimilarAffinityKey)o;

            return backups == key.backups &&
                affFuncCls == key.affFuncCls &&
                filterCls == key.filterCls &&
                partsCnt == key.partsCnt;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(SimilarAffinityKey.class, this);
        }
    }
}
