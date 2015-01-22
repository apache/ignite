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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.internal.managers.eventstorage.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.apache.ignite.internal.processors.timeout.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.events.IgniteEventType.*;
import static org.apache.ignite.cache.GridCacheMode.*;
import static org.apache.ignite.internal.GridClosureCallMode.*;
import static org.apache.ignite.internal.processors.affinity.GridAffinityUtils.*;

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
    private final ConcurrentMap<AffinityAssignmentKey, IgniteFuture<AffinityInfo>> affMap = new ConcurrentHashMap8<>();

    /** Listener. */
    private final GridLocalEventListener lsnr = new GridLocalEventListener() {
        @Override public void onEvent(IgniteEvent evt) {
            int evtType = evt.type();

            assert evtType == EVT_NODE_FAILED || evtType == EVT_NODE_LEFT || evtType == EVT_NODE_JOINED;

            if (affMap.isEmpty())
                return; // Skip empty affinity map.

            final IgniteDiscoveryEvent discoEvt = (IgniteDiscoveryEvent)evt;

            // Clean up affinity functions if such cache no more exists.
            if (evtType == EVT_NODE_FAILED || evtType == EVT_NODE_LEFT) {
                final Collection<String> caches = new HashSet<>();

                for (ClusterNode clusterNode : ctx.discovery().allNodes())
                    caches.addAll(U.cacheNames(clusterNode));

                final Collection<AffinityAssignmentKey> rmv = new GridLeanSet<>();

                for (AffinityAssignmentKey key : affMap.keySet()) {
                    if (!caches.contains(key.cacheName) || key.topVer < discoEvt.topologyVersion() - 1)
                        rmv.add(key);
                }

                ctx.timeout().addTimeoutObject(new GridTimeoutObjectAdapter(
                    IgniteUuid.fromUuid(ctx.localNodeId()), AFFINITY_MAP_CLEAN_UP_DELAY) {
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
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws IgniteCheckedException {
        ctx.event().addLocalEventListener(lsnr, EVT_NODE_FAILED, EVT_NODE_LEFT, EVT_NODE_JOINED);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        if (ctx != null && ctx.event() != null)
            ctx.event().removeLocalEventListener(lsnr);
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
     * Maps keys to nodes on default cache.
     *
     * @param keys Keys to map.
     * @return Map of nodes to keys.
     * @throws IgniteCheckedException If failed.
     */
    public <K> Map<ClusterNode, Collection<K>> mapKeysToNodes(@Nullable Collection<? extends K> keys)
        throws IgniteCheckedException {
        return keysToNodes(null, keys);
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

        return map != null ? F.first(map.keySet()) : null;
    }

    /**
     * Maps single key to a node.
     *
     * @param cacheName Cache name.
     * @param key Key to map.
     * @return Picked node.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public <K> ClusterNode mapKeyToNode(@Nullable String cacheName, K key, long topVer) throws IgniteCheckedException {
        Map<ClusterNode, Collection<K>> map = keysToNodes(cacheName, F.asList(key), topVer);

        return map != null ? F.first(map.keySet()) : null;
    }

    /**
     * Maps single key to a node on default cache.
     *
     * @param key Key to map.
     * @return Picked node.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public <K> ClusterNode mapKeyToNode(K key) throws IgniteCheckedException {
        return mapKeyToNode(null, key);
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

        AffinityInfo affInfo = affinityCache(cacheName, ctx.discovery().topologyVersion());

        if (affInfo == null || affInfo.mapper == null)
            return null;

        if (affInfo.portableEnabled)
            key = ctx.portable().marshalToPortable(key);

        return affInfo.mapper.affinityKey(key);
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
        return keysToNodes(cacheName, keys, ctx.discovery().topologyVersion());
    }

    /**
     * @param cacheName Cache name.
     * @param keys Keys.
     * @param topVer Topology version.
     * @return Affinity map.
     * @throws IgniteCheckedException If failed.
     */
    private <K> Map<ClusterNode, Collection<K>> keysToNodes(@Nullable final String cacheName,
        Collection<? extends K> keys, long topVer) throws IgniteCheckedException {
        if (F.isEmpty(keys))
            return Collections.emptyMap();

        ClusterNode loc = ctx.discovery().localNode();

        if (U.hasCache(loc, cacheName) && ctx.cache().cache(cacheName).configuration().getCacheMode() == LOCAL)
            return F.asMap(loc, (Collection<K>)keys);

        AffinityInfo affInfo = affinityCache(cacheName, topVer);

        return affInfo != null ? affinityMap(affInfo, keys) : Collections.<ClusterNode, Collection<K>>emptyMap();
    }

    /**
     * @param cacheName Cache name.
     * @return Affinity cache.
     * @throws IgniteCheckedException In case of error.
     */
    @SuppressWarnings("ErrorNotRethrown")
    private AffinityInfo affinityCache(@Nullable final String cacheName, long topVer) throws IgniteCheckedException {
        AffinityAssignmentKey key = new AffinityAssignmentKey(cacheName, topVer);

        IgniteFuture<AffinityInfo> fut = affMap.get(key);

        if (fut != null)
            return fut.get();

        ClusterNode loc = ctx.discovery().localNode();

        // Check local node.
        if (U.hasCache(loc, cacheName)) {
            GridCacheContext<Object,Object> cctx = ctx.cache().internalCache(cacheName).context();

            AffinityInfo info = new AffinityInfo(
                cctx.config().getAffinity(),
                cctx.config().getAffinityMapper(),
                new GridAffinityAssignment(topVer, cctx.affinity().assignments(topVer)),
                cctx.portableEnabled());

            IgniteFuture<AffinityInfo> old = affMap.putIfAbsent(key, new GridFinishedFuture<>(ctx, info));

            if (old != null)
                info = old.get();

            return info;
        }

        Collection<ClusterNode> cacheNodes = F.view(
            ctx.discovery().remoteNodes(),
            new P1<ClusterNode>() {
                @Override public boolean apply(ClusterNode n) {
                    return U.hasCache(n, cacheName);
                }
            });

        if (F.isEmpty(cacheNodes))
            return null;

        GridFutureAdapter<AffinityInfo> fut0 = new GridFutureAdapter<>();

        IgniteFuture<AffinityInfo> old = affMap.putIfAbsent(key, fut0);

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

            GridCacheMode mode = U.cacheMode(n, cacheName);

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
     * Requests {@link GridCacheAffinityFunction} and {@link org.apache.ignite.cache.affinity.GridCacheAffinityKeyMapper} from remote node.
     *
     * @param cacheName Name of cache on which affinity is requested.
     * @param n Node from which affinity is requested.
     * @return Affinity cached function.
     * @throws IgniteCheckedException If either local or remote node cannot get deployment for affinity objects.
     */
    private AffinityInfo affinityInfoFromNode(@Nullable String cacheName, long topVer, ClusterNode n)
        throws IgniteCheckedException {
        GridTuple3<GridAffinityMessage, GridAffinityMessage, GridAffinityAssignment> t = ctx.closure()
            .callAsyncNoFailover(BALANCE, affinityJob(cacheName, topVer), F.asList(n), true/*system pool*/).get();

        GridCacheAffinityFunction f = (GridCacheAffinityFunction)unmarshall(ctx, n.id(), t.get1());
        GridCacheAffinityKeyMapper m = (GridCacheAffinityKeyMapper)unmarshall(ctx, n.id(), t.get2());

        assert m != null;

        // Bring to initial state.
        f.reset();
        m.reset();

        Boolean portableEnabled = U.portableEnabled(n, cacheName);

        return new AffinityInfo(f, m, t.get3(), portableEnabled != null && portableEnabled);
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
        int part = aff.affFunc.partition(aff.mapper.affinityKey(key));

        Collection<ClusterNode> nodes = aff.assignment.get(part);

        if (F.isEmpty(nodes))
            throw new IgniteCheckedException("Failed to get affinity nodes [aff=" + aff + ", key=" + key + ']');

        return nodes.iterator().next();
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
        private GridCacheAffinityFunction affFunc;

        /** Mapper */
        private GridCacheAffinityKeyMapper mapper;

        /** Assignment. */
        private GridAffinityAssignment assignment;

        /** Portable enabled flag. */
        private boolean portableEnabled;

        /**
         * @param affFunc Affinity function.
         * @param mapper Affinity key mapper.
         * @param assignment Partition assignment.
         * @param portableEnabled Portable enabled flag.
         */
        private AffinityInfo(GridCacheAffinityFunction affFunc, GridCacheAffinityKeyMapper mapper,
            GridAffinityAssignment assignment, boolean portableEnabled) {
            this.affFunc = affFunc;
            this.mapper = mapper;
            this.assignment = assignment;
            this.portableEnabled = portableEnabled;
        }
    }

    /**
     *
     */
    private static class AffinityAssignmentKey {
        /** */
        private String cacheName;

        /** */
        private long topVer;

        /**
         * @param cacheName Cache name.
         * @param topVer Topology version.
         */
        private AffinityAssignmentKey(String cacheName, long topVer) {
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

            return topVer == that.topVer && F.eq(cacheName, that.cacheName);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = cacheName != null ? cacheName.hashCode() : 0;

            res = 31 * res + (int)(topVer ^ (topVer >>> 32));

            return res;
        }
    }
}
