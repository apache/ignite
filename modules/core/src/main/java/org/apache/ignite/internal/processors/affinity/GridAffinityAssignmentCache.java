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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridNodeOrderComparator;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDefaultAffinityKeyMapper;
import org.apache.ignite.internal.processors.cache.GridCacheInternal;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;
import org.jsr166.ConcurrentLinkedHashMap;

import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;

/**
 * Affinity cached function.
 */
public class GridAffinityAssignmentCache {
    /** Cache name. */
    private final String cacheName;

    /** Number of backups. */
    private int backups;

    /** Affinity function. */
    private final AffinityFunction aff;

    /** Partitions count. */
    private final int partsCnt;

    /** Affinity mapper function. */
    private final AffinityKeyMapper affMapper;

    /** Affinity calculation results cache: topology version => partition => nodes. */
    private final ConcurrentLinkedHashMap<AffinityTopologyVersion, GridAffinityAssignment> affCache;

    /** Cache item corresponding to the head topology version. */
    private final AtomicReference<GridAffinityAssignment> head;

    /** Discovery manager. */
    private final GridCacheContext ctx;

    /** Ready futures. */
    private final ConcurrentMap<AffinityTopologyVersion, AffinityReadyFuture> readyFuts = new ConcurrentHashMap8<>();

    /** Log. */
    private IgniteLogger log;

    /** Node stop flag. */
    private volatile IgniteCheckedException stopErr;

    /**
     * Constructs affinity cached calculations.
     *
     * @param ctx Kernal context.
     * @param cacheName Cache name.
     * @param aff Affinity function.
     * @param affMapper Affinity key mapper.
     * @param backups Number of backups.
     */
    @SuppressWarnings("unchecked")
    public GridAffinityAssignmentCache(GridCacheContext ctx,
        String cacheName,
        AffinityFunction aff,
        AffinityKeyMapper affMapper,
        int backups)
    {
        assert ctx != null;
        assert aff != null;
        assert affMapper != null;

        this.ctx = ctx;
        this.aff = aff;
        this.affMapper = affMapper;
        this.cacheName = cacheName;
        this.backups = backups;

        log = ctx.logger(GridAffinityAssignmentCache.class);

        partsCnt = aff.partitions();
        affCache = new ConcurrentLinkedHashMap<>();
        head = new AtomicReference<>(new GridAffinityAssignment(AffinityTopologyVersion.NONE));
    }

    /**
     * Initializes affinity with given topology version and assignment. The assignment is calculated on remote nodes
     * and brought to local node on partition map exchange.
     *
     * @param topVer Topology version.
     * @param affAssignment Affinity assignment for topology version.
     */
    public void initialize(AffinityTopologyVersion topVer, List<List<ClusterNode>> affAssignment) {
        GridAffinityAssignment assignment = new GridAffinityAssignment(topVer, affAssignment);

        affCache.put(topVer, assignment);
        head.set(assignment);

        for (Map.Entry<AffinityTopologyVersion, AffinityReadyFuture> entry : readyFuts.entrySet()) {
            if (entry.getKey().compareTo(topVer) <= 0) {
                if (log.isDebugEnabled())
                    log.debug("Completing topology ready future (initialized affinity) " +
                        "[locNodeId=" + ctx.localNodeId() + ", futVer=" + entry.getKey() + ", topVer=" + topVer + ']');

                entry.getValue().onDone(topVer);
            }
        }
    }

    /**
     * Kernal stop callback.
     *
     * @param err Error.
     */
    public void onKernalStop(IgniteCheckedException err) {
        stopErr = err;

        for (AffinityReadyFuture fut : readyFuts.values())
            fut.onDone(err);
    }

    /**
     *
     */
    public void onReconnected() {
        affCache.clear();

        head.set(new GridAffinityAssignment(AffinityTopologyVersion.NONE));

        stopErr = null;
    }

    /**
     * Calculates affinity cache for given topology version.
     *
     * @param topVer Topology version to calculate affinity cache for.
     * @param discoEvt Discovery event that caused this topology version change.
     * @return Affinity assignments.
     */
    @SuppressWarnings("IfMayBeConditional")
    public List<List<ClusterNode>> calculate(AffinityTopologyVersion topVer, DiscoveryEvent discoEvt) {
        if (log.isDebugEnabled())
            log.debug("Calculating affinity [topVer=" + topVer + ", locNodeId=" + ctx.localNodeId() +
                ", discoEvt=" + discoEvt + ']');

        Iterator<AffinityTopologyVersion> it = affCache.descendingKeySet().iterator();

        AffinityTopologyVersion prevVer = null;

        if (it.hasNext())
            prevVer = it.next();

        GridAffinityAssignment prev = prevVer == null ? null : affCache.get(prevVer);

        List<ClusterNode> sorted;

        if (ctx.isLocal())
            // For local cache always use local node.
            sorted = Collections.singletonList(ctx.localNode());
        else {
            // Resolve nodes snapshot for specified topology version.
            sorted = new ArrayList<>(ctx.discovery().cacheAffinityNodes(cacheName, topVer));

            Collections.sort(sorted, GridNodeOrderComparator.INSTANCE);
        }

        List<List<ClusterNode>> prevAssignment = prev == null ? null : prev.assignment();

        List<List<ClusterNode>> assignment;

        if (prevAssignment != null && discoEvt != null) {
            boolean affNode = ctx.discovery().cacheAffinityNode(discoEvt.eventNode(), ctx.name());

            if (!affNode)
                assignment = prevAssignment;
            else
                assignment = aff.assignPartitions(new GridAffinityFunctionContextImpl(sorted, prevAssignment,
                    discoEvt, topVer, backups));
        }
        else
            assignment = aff.assignPartitions(new GridAffinityFunctionContextImpl(sorted, prevAssignment, discoEvt,
                topVer, backups));

        assert assignment != null;

        GridAffinityAssignment updated = new GridAffinityAssignment(topVer, assignment);

        updated = F.addIfAbsent(affCache, topVer, updated);

        // Update top version, if required.
        while (true) {
            GridAffinityAssignment headItem = head.get();

            if (headItem.topologyVersion().compareTo(topVer) >= 0)
                break;

            if (head.compareAndSet(headItem, updated))
                break;
        }

        for (Map.Entry<AffinityTopologyVersion, AffinityReadyFuture> entry : readyFuts.entrySet()) {
            if (entry.getKey().compareTo(topVer) <= 0) {
                if (log.isDebugEnabled())
                    log.debug("Completing topology ready future (calculated affinity) [locNodeId=" + ctx.localNodeId() +
                        ", futVer=" + entry.getKey() + ", topVer=" + topVer + ']');

                entry.getValue().onDone(topVer);
            }
        }

        return updated.assignment();
    }

    /**
     * Copies previous affinity assignment when discovery event does not cause affinity assignment changes
     * (e.g. client node joins on leaves).
     *
     * @param evt Event.
     * @param topVer Topology version.
     */
    public void clientEventTopologyChange(DiscoveryEvent evt, AffinityTopologyVersion topVer) {
        GridAffinityAssignment aff = head.get();

        assert evt.type() == EVT_DISCOVERY_CUSTOM_EVT  || aff.primaryPartitions(evt.eventNode().id()).isEmpty() : evt;
        assert evt.type() == EVT_DISCOVERY_CUSTOM_EVT  || aff.backupPartitions(evt.eventNode().id()).isEmpty() : evt;

        GridAffinityAssignment assignmentCpy = new GridAffinityAssignment(topVer, aff);

        affCache.put(topVer, assignmentCpy);
        head.set(assignmentCpy);

        for (Map.Entry<AffinityTopologyVersion, AffinityReadyFuture> entry : readyFuts.entrySet()) {
            if (entry.getKey().compareTo(topVer) <= 0) {
                if (log.isDebugEnabled())
                    log.debug("Completing topology ready future (use previous affinity) " +
                        "[locNodeId=" + ctx.localNodeId() + ", futVer=" + entry.getKey() + ", topVer=" + topVer + ']');

                entry.getValue().onDone(topVer);
            }
        }
    }

    /**
     * @return Last calculated affinity version.
     */
    public AffinityTopologyVersion lastVersion() {
        return head.get().topologyVersion();
    }

    /**
     * Clean up outdated cache items.
     *
     * @param topVer Actual topology version, older versions will be removed.
     */
    public void cleanUpCache(AffinityTopologyVersion topVer) {
        if (log.isDebugEnabled())
            log.debug("Cleaning up cache for version [locNodeId=" + ctx.localNodeId() +
                ", topVer=" + topVer + ']');

        for (Iterator<AffinityTopologyVersion> it = affCache.keySet().iterator(); it.hasNext(); )
            if (it.next().compareTo(topVer) < 0)
                it.remove();
    }

    /**
     * @param topVer Topology version.
     * @return Affinity assignment.
     */
    public List<List<ClusterNode>> assignments(AffinityTopologyVersion topVer) {
        GridAffinityAssignment aff = cachedAffinity(topVer);

        return aff.assignment();
    }

    /**
     * Gets future that will be completed after topology with version {@code topVer} is calculated.
     *
     * @param topVer Topology version to await for.
     * @return Future that will be completed after affinity for topology version {@code topVer} is calculated.
     */
    @Nullable public IgniteInternalFuture<AffinityTopologyVersion> readyFuture(AffinityTopologyVersion topVer) {
        GridAffinityAssignment aff = head.get();

        if (aff.topologyVersion().compareTo(topVer) >= 0) {
            if (log.isDebugEnabled())
                log.debug("Returning finished future for readyFuture [head=" + aff.topologyVersion() +
                    ", topVer=" + topVer + ']');

            return null;
        }

        GridFutureAdapter<AffinityTopologyVersion> fut = F.addIfAbsent(readyFuts, topVer,
            new AffinityReadyFuture(topVer));

        aff = head.get();

        if (aff.topologyVersion().compareTo(topVer) >= 0) {
            if (log.isDebugEnabled())
                log.debug("Completing topology ready future right away [head=" + aff.topologyVersion() +
                    ", topVer=" + topVer + ']');

            fut.onDone(topVer);
        }
        else if (stopErr != null)
            fut.onDone(stopErr);

        return fut;
    }

    /**
     * @return Partition count.
     */
    public int partitions() {
        return partsCnt;
    }

    /**
     * NOTE: Use this method always when you need to calculate partition id for
     * a key provided by user. It's required since we should apply affinity mapper
     * logic in order to find a key that will eventually be passed to affinity function.
     *
     * @param key Key.
     * @return Partition.
     */
    public int partition(Object key) {
        return aff.partition(affinityKey(key));
    }

    /**
     * If Key is {@link GridCacheInternal GridCacheInternal} entry when won't passed into user's mapper and
     * will use {@link GridCacheDefaultAffinityKeyMapper default}.
     *
     * @param key Key.
     * @return Affinity key.
     */
    private Object affinityKey(Object key) {
        if (key instanceof CacheObject)
            key = ((CacheObject)key).value(ctx.cacheObjectContext(), false);

        return (key instanceof GridCacheInternal ? ctx.defaultAffMapper() : affMapper).affinityKey(key);
    }

    /**
     * Gets affinity nodes for specified partition.
     *
     * @param part Partition.
     * @param topVer Topology version.
     * @return Affinity nodes.
     */
    public List<ClusterNode> nodes(int part, AffinityTopologyVersion topVer) {
        // Resolve cached affinity nodes.
        return cachedAffinity(topVer).get(part);
    }

    /**
     * Get primary partitions for specified node ID.
     *
     * @param nodeId Node ID to get primary partitions for.
     * @param topVer Topology version.
     * @return Primary partitions for specified node ID.
     */
    public Set<Integer> primaryPartitions(UUID nodeId, AffinityTopologyVersion topVer) {
        return cachedAffinity(topVer).primaryPartitions(nodeId);
    }

    /**
     * Get backup partitions for specified node ID.
     *
     * @param nodeId Node ID to get backup partitions for.
     * @param topVer Topology version.
     * @return Backup partitions for specified node ID.
     */
    public Set<Integer> backupPartitions(UUID nodeId, AffinityTopologyVersion topVer) {
        return cachedAffinity(topVer).backupPartitions(nodeId);
    }

    /**
     * Get cached affinity for specified topology version.
     *
     * @param topVer Topology version.
     * @return Cached affinity.
     */
    private GridAffinityAssignment cachedAffinity(AffinityTopologyVersion topVer) {
        if (topVer.equals(AffinityTopologyVersion.NONE))
            topVer = lastVersion();
        else
            awaitTopologyVersion(topVer);

        assert topVer.topologyVersion() >= 0 : topVer;

        GridAffinityAssignment cache = head.get();

        if (!cache.topologyVersion().equals(topVer)) {
            cache = affCache.get(topVer);

            if (cache == null) {
                throw new IllegalStateException("Getting affinity for topology version earlier than affinity is " +
                    "calculated [locNodeId=" + ctx.localNodeId() +
                    ", cache=" + cacheName +
                    ", topVer=" + topVer +
                    ", head=" + head.get().topologyVersion() +
                    ", history=" + affCache.keySet() +
                    ']');
            }
        }

        assert cache.topologyVersion().equals(topVer) : "Invalid cached affinity: " + cache;

        return cache;
    }

    /**
     * @param topVer Topology version to wait.
     */
    private void awaitTopologyVersion(AffinityTopologyVersion topVer) {
        GridAffinityAssignment aff = head.get();

        if (aff.topologyVersion().compareTo(topVer) >= 0)
            return;

        try {
            if (log.isDebugEnabled())
                log.debug("Will wait for topology version [locNodeId=" + ctx.localNodeId() +
                ", topVer=" + topVer + ']');

            IgniteInternalFuture<AffinityTopologyVersion> fut = readyFuture(topVer);

            if (fut != null)
                fut.get();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to wait for affinity ready future for topology version: " + topVer,
                e);
        }
    }

    /**
     * Affinity ready future. Will remove itself from ready futures map.
     */
    private class AffinityReadyFuture extends GridFutureAdapter<AffinityTopologyVersion> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private AffinityTopologyVersion reqTopVer;

        /**
         *
         * @param reqTopVer Required topology version.
         */
        private AffinityReadyFuture(AffinityTopologyVersion reqTopVer) {
            this.reqTopVer = reqTopVer;
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(AffinityTopologyVersion res, @Nullable Throwable err) {
            assert res != null || err != null;

            boolean done = super.onDone(res, err);

            if (done)
                readyFuts.remove(reqTopVer, this);

            return done;
        }
    }
}