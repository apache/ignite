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
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.util.GridLeanMap;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
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

    /** Retries to get affinity in case of error. */
    private static final int ERROR_RETRIES = 3;

    /** Time to wait between errors (in milliseconds). */
    private static final long ERROR_WAIT = 500;

    /** Null cache name. */
    private static final String NULL_NAME = U.id8(UUID.randomUUID());

    /** Affinity map. */
    private final ConcurrentMap<AffinityAssignmentKey, IgniteInternalFuture<AffinityInfo>> affMap = new ConcurrentHashMap8<>();

    /** Listener. */
    private final GridLocalEventListener lsnr = new GridLocalEventListener() {
        @Override public void onEvent(Event evt) {
            int evtType = evt.type();

            assert evtType == EVT_NODE_FAILED || evtType == EVT_NODE_LEFT || evtType == EVT_NODE_JOINED;

            if (affMap.isEmpty())
                return; // Skip empty affinity map.

            final DiscoveryEvent discoEvt = (DiscoveryEvent)evt;

            // Clean up affinity functions if such cache no more exists.
            if (evtType == EVT_NODE_FAILED || evtType == EVT_NODE_LEFT) {
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
        }
    };

    /**
     * @param ctx Context.
     */
    public GridAffinityProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        ctx.event().addLocalEventListener(lsnr, EVT_NODE_FAILED, EVT_NODE_LEFT, EVT_NODE_JOINED);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        ctx.event().removeLocalEventListener(lsnr);
    }

    /**
     * @param cacheName Cache name.
     * @param key Key.
     * @return Key partition.
     * @throws IgniteCheckedException If failed.
     */
    public int partition(@Nullable String cacheName, Object key) throws IgniteCheckedException {
        return partition(cacheName, key, null);
    }

    /**
     * @param cacheName Cache name (needed only if {@code aff} is not provided.
     * @param key Key.
     * @param aff Affinity information.
     * @return Key partition.
     * @throws IgniteCheckedException If failed.
     */
    public int partition(@Nullable String cacheName,
        Object key,
        @Nullable AffinityInfo aff) throws IgniteCheckedException {
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
    public int partition0(@Nullable String cacheName,
        Object key,
        @Nullable AffinityInfo aff) throws IgniteCheckedException {
        if (aff == null) {
            aff = affinityCache(cacheName, ctx.discovery().topologyVersionEx());

            if (aff == null)
                throw new IgniteCheckedException("Failed to get cache affinity.");
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
    @Nullable public ClusterNode mapPartitionToNode(@Nullable String cacheName, int partId,
        AffinityTopologyVersion topVer)
        throws IgniteCheckedException {
        AffinityInfo affInfo = affinityCache(cacheName, topVer);

        return affInfo != null ? F.first(affInfo.assignment().get(partId)) : null;
    }


    /**
     * Maps keys to nodes for given cache.
     *
     * @param cacheName Cache name.
     * @param keys Keys to map.
     * @return Map of nodes to keys.
     * @throws IgniteCheckedException If failed.
     */
    public <K> Map<ClusterNode, Collection<K>> mapKeysToNodes(@Nullable String cacheName,
        @Nullable Collection<? extends K> keys) throws IgniteCheckedException {
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
    @Nullable public <K> ClusterNode mapKeyToNode(@Nullable String cacheName, K key) throws IgniteCheckedException {
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
    @Nullable public <K> ClusterNode mapKeyToNode(@Nullable String cacheName, K key,
        AffinityTopologyVersion topVer) throws IgniteCheckedException {
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
    public <K> List<ClusterNode> mapKeyToPrimaryAndBackups(@Nullable String cacheName,
        K key,
        AffinityTopologyVersion topVer)
        throws IgniteCheckedException
    {
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
    @SuppressWarnings("unchecked")
    @Nullable public Object affinityKey(@Nullable String cacheName, @Nullable Object key) throws IgniteCheckedException {
        if (key == null)
            return null;

        AffinityInfo affInfo = affinityCache(cacheName, ctx.discovery().topologyVersionEx());

        if (affInfo == null)
            return null;

        return affInfo.affinityKey(key);
    }

    /**
     * @param cacheName Cache name.
     * @return Cache affinity.
     */
    public <K> CacheAffinityProxy<K> affinityProxy(String cacheName) {
        return new CacheAffinityProxy<>(cacheName);
    }

    /**
     * @param cacheName Cache name.
     * @return Non-null cache name.
     */
    private String maskNull(@Nullable String cacheName) {
        return cacheName == null ? NULL_NAME : cacheName;
    }

    /**
     * @param cacheName Cache name.
     * @param keys Keys.
     * @return Affinity map.
     * @throws IgniteCheckedException If failed.
     */
    private <K> Map<ClusterNode, Collection<K>> keysToNodes(@Nullable final String cacheName,
        Collection<? extends K> keys) throws IgniteCheckedException {
        return keysToNodes(cacheName, keys, ctx.discovery().topologyVersionEx());
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

        return affInfo != null ? affinityMap(affInfo, keys) : Collections.<ClusterNode, Collection<K>>emptyMap();
    }

    /**
     * @param cacheName Cache name.
     * @param topVer Topology version.
     * @return Affinity cache.
     * @throws IgniteCheckedException In case of error.
     */
    @SuppressWarnings("ErrorNotRethrown")
    @Nullable private AffinityInfo affinityCache(@Nullable final String cacheName, AffinityTopologyVersion topVer)
        throws IgniteCheckedException {
        AffinityAssignmentKey key = new AffinityAssignmentKey(cacheName, topVer);

        IgniteInternalFuture<AffinityInfo> fut = affMap.get(key);

        if (fut != null)
            return fut.get();

        ClusterNode loc = ctx.discovery().localNode();

        // Check local node.
        Collection<ClusterNode> cacheNodes = ctx.discovery().cacheNodes(cacheName, topVer);

        if (cacheNodes.contains(loc)) {
            GridCacheAdapter<Object, Object> cache = ctx.cache().internalCache(cacheName);

            // Cache is being stopped.
            if (cache == null)
                return null;

            GridCacheContext<Object, Object> cctx = cache.context();

            cctx.awaitStarted();

            try {
                cctx.gate().enter();
            }
            catch (IllegalStateException ignored) {
                return null;
            }

            try {
                AffinityAssignment assign0 = cctx.affinity().assignment(topVer);

                GridAffinityAssignment assign = assign0 instanceof GridAffinityAssignment ?
                    (GridAffinityAssignment)assign0 :
                    new GridAffinityAssignment(topVer, assign0.assignment(), assign0.idealAssignment());

                AffinityInfo info = new AffinityInfo(
                    cctx.config().getAffinity(),
                    cctx.config().getAffinityMapper(),
                    assign,
                    cctx.cacheObjectContext());

                IgniteInternalFuture<AffinityInfo> old = affMap.putIfAbsent(key, new GridFinishedFuture<>(info));

                if (old != null)
                    info = old.get();

                return info;
            }
            finally {
                cctx.gate().leave();
            }
        }

        if (F.isEmpty(cacheNodes))
            return null;

        GridFutureAdapter<AffinityInfo> fut0 = new GridFutureAdapter<>();

        IgniteInternalFuture<AffinityInfo> old = affMap.putIfAbsent(key, fut0);

        if (old != null)
            return old.get();

        int max = ERROR_RETRIES;
        int cnt = 0;

        Iterator<ClusterNode> it = cacheNodes.iterator();

        // We are here because affinity has not been fetched yet, or cache mode is LOCAL.
        while (true) {
            cnt++;

            if (!it.hasNext())
                it = cacheNodes.iterator();

            // Double check since we deal with dynamic view.
            if (!it.hasNext())
                // Exception will be caught in this method.
                throw new IgniteCheckedException("No cache nodes in topology for cache name: " + cacheName);

            ClusterNode n = it.next();

            CacheMode mode = ctx.cache().cacheMode(cacheName);

            assert mode != null;

            // Map all keys to a single node, if the cache mode is LOCAL.
            if (mode == LOCAL) {
                fut0.onDone(new IgniteCheckedException("Failed to map keys for LOCAL cache."));

                // Will throw exception.
                fut0.get();
            }

            try {
                // Resolve cache context for remote node.
                // Set affinity function before counting down on latch.
                fut0.onDone(affinityInfoFromNode(cacheName, topVer, n));

                break;
            }
            catch (IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to get affinity from node (will retry) [cache=" + cacheName +
                        ", node=" + U.toShortString(n) + ", msg=" + e.getMessage() + ']');

                if (cnt < max) {
                    U.sleep(ERROR_WAIT);

                    continue;
                }

                affMap.remove(maskNull(cacheName), fut0);

                fut0.onDone(new IgniteCheckedException("Failed to get affinity mapping from node: " + n, e));

                break;
            }
            catch (RuntimeException | Error e) {
                fut0.onDone(new IgniteCheckedException("Failed to get affinity mapping from node: " + n, e));

                break;
            }
        }

        return fut0.get();
    }

    /**
     * Requests {@link AffinityFunction} and {@link AffinityKeyMapper} from remote node.
     *
     * @param cacheName Name of cache on which affinity is requested.
     * @param topVer Topology version.
     * @param n Node from which affinity is requested.
     * @return Affinity cached function.
     * @throws IgniteCheckedException If either local or remote node cannot get deployment for affinity objects.
     */
    private AffinityInfo affinityInfoFromNode(@Nullable String cacheName, AffinityTopologyVersion topVer, ClusterNode n)
        throws IgniteCheckedException {
        GridTuple3<GridAffinityMessage, GridAffinityMessage, GridAffinityAssignment> t = ctx.closure()
            .callAsyncNoFailover(BROADCAST, affinityJob(cacheName, topVer), F.asList(n), true/*system pool*/, 0).get();

        AffinityFunction f = (AffinityFunction)unmarshall(ctx, n.id(), t.get1());
        AffinityKeyMapper m = (AffinityKeyMapper)unmarshall(ctx, n.id(), t.get2());

        assert m != null;

        // Bring to initial state.
        f.reset();
        m.reset();

        CacheConfiguration ccfg = ctx.cache().cacheConfiguration(cacheName);

        return new AffinityInfo(f, m, t.get3(), ctx.cacheObjects().contextForCache(ccfg));
    }

    /**
     * @param aff Affinity function.
     * @param keys Keys.
     * @return Affinity map.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings({"unchecked"})
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
        int part = partition(null, key, aff);

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
        X.println(">>> Affinity processor memory stats [grid=" + ctx.gridName() + ']');
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
    private static class AffinityAssignmentKey {
        /** */
        private String cacheName;

        /** */
        private AffinityTopologyVersion topVer;

        /**
         * @param cacheName Cache name.
         * @param topVer Topology version.
         */
        private AffinityAssignmentKey(String cacheName, @NotNull AffinityTopologyVersion topVer) {
            this.cacheName = cacheName;
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
            AffinityInfo aff = affinityCache(cacheName, ctx.discovery().topologyVersionEx());

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
            hash= 31 * hash + partsCnt;

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
