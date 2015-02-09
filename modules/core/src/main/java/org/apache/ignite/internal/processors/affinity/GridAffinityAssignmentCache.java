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
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.portables.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheDistributionMode.*;

/**
 * Affinity cached function.
 */
public class GridAffinityAssignmentCache {
    /** Node order comparator. */
    private static final Comparator<ClusterNode> nodeCmp = new GridNodeOrderComparator();

    /** Cache name. */
    private final String cacheName;

    /** Number of backups. */
    private int backups;

    /** Affinity function. */
    private final CacheAffinityFunction aff;

    /** Partitions count. */
    private final int partsCnt;

    /** Affinity mapper function. */
    private final CacheAffinityKeyMapper affMapper;

    /** Affinity calculation results cache: topology version => partition => nodes. */
    private final ConcurrentMap<Long, GridAffinityAssignment> affCache;

    /** Cache item corresponding to the head topology version. */
    private final AtomicReference<GridAffinityAssignment> head;

    /** Discovery manager. */
    private final GridCacheContext ctx;

    /** Ready futures. */
    private final ConcurrentMap<Long, AffinityReadyFuture> readyFuts = new ConcurrentHashMap8<>();

    /** Log. */
    private IgniteLogger log;

    /**
     * Constructs affinity cached calculations.
     *
     * @param ctx Kernal context.
     * @param cacheName Cache name.
     * @param aff Affinity function.
     * @param affMapper Affinity key mapper.
     */
    @SuppressWarnings("unchecked")
    public GridAffinityAssignmentCache(GridCacheContext ctx, String cacheName, CacheAffinityFunction aff,
        CacheAffinityKeyMapper affMapper, int backups) {
        this.ctx = ctx;
        this.aff = aff;
        this.affMapper = affMapper;
        this.cacheName = cacheName;
        this.backups = backups;

        log = ctx.logger(GridAffinityAssignmentCache.class);

        partsCnt = aff.partitions();
        affCache = new ConcurrentLinkedHashMap<>();
        head = new AtomicReference<>(new GridAffinityAssignment(-1));
    }

    /**
     * Initializes affinity with given topology version and assignment. The assignment is calculated on remote nodes
     * and brought to local node on partition map exchange.
     *
     * @param topVer Topology version.
     * @param affAssignment Affinity assignment for topology version.
     */
    public void initialize(long topVer, List<List<ClusterNode>> affAssignment) {
        GridAffinityAssignment assignment = new GridAffinityAssignment(topVer, affAssignment);

        affCache.put(topVer, assignment);
        head.set(assignment);

        for (Map.Entry<Long, AffinityReadyFuture> entry : readyFuts.entrySet()) {
            if (entry.getKey() >= topVer)
                entry.getValue().onDone(topVer);
        }
    }

    /**
     * Calculates affinity cache for given topology version.
     *
     * @param topVer Topology version to calculate affinity cache for.
     * @param discoEvt Discovery event that caused this topology version change.
     */
    @SuppressWarnings("IfMayBeConditional")
    public List<List<ClusterNode>> calculate(long topVer, DiscoveryEvent discoEvt) {
        if (log.isDebugEnabled())
            log.debug("Calculating affinity [topVer=" + topVer + ", locNodeId=" + ctx.localNodeId() +
                ", discoEvt=" + discoEvt + ']');

        GridAffinityAssignment prev = affCache.get(topVer - 1);

        List<ClusterNode> sorted;

        if (ctx.isLocal())
            // For local cache always use local node.
            sorted = Collections.singletonList(ctx.localNode());
        else {
            // Resolve nodes snapshot for specified topology version.
            Collection<ClusterNode> nodes = ctx.discovery().cacheAffinityNodes(cacheName, topVer);

            sorted = sort(nodes);
        }

        List<List<ClusterNode>> prevAssignment = prev == null ? null : prev.assignment();

        List<List<ClusterNode>> assignment;

        if (prevAssignment != null && discoEvt != null) {
            CacheDistributionMode distroMode = U.distributionMode(discoEvt.eventNode(), ctx.name());

            if (distroMode == null || // no cache on node.
                distroMode == CLIENT_ONLY || distroMode == NEAR_ONLY)
                assignment = prevAssignment;
            else
                assignment = aff.assignPartitions(new GridCacheAffinityFunctionContextImpl(sorted, prevAssignment,
                    discoEvt, topVer, backups));
        }
        else
            assignment = aff.assignPartitions(new GridCacheAffinityFunctionContextImpl(sorted, prevAssignment, discoEvt,
                topVer, backups));

        assert assignment != null;

        GridAffinityAssignment updated = new GridAffinityAssignment(topVer, assignment);

        updated = F.addIfAbsent(affCache, topVer, updated);

        // Update top version, if required.
        while (true) {
            GridAffinityAssignment headItem = head.get();

            if (headItem.topologyVersion() >= topVer)
                break;

            if (head.compareAndSet(headItem, updated))
                break;
        }

        for (Map.Entry<Long, AffinityReadyFuture> entry : readyFuts.entrySet()) {
            if (entry.getKey() <= topVer) {
                if (log.isDebugEnabled())
                    log.debug("Completing topology ready future (calculated affinity) [locNodeId=" + ctx.localNodeId() +
                        ", futVer=" + entry.getKey() + ", topVer=" + topVer + ']');

                entry.getValue().onDone(topVer);
            }
        }

        return updated.assignment();
    }

    /**
     * @return Last calculated affinity version.
     */
    public long lastVersion() {
        return head.get().topologyVersion();
    }

    /**
     * Clean up outdated cache items.
     *
     * @param topVer Actual topology version, older versions will be removed.
     */
    public void cleanUpCache(long topVer) {
        if (log.isDebugEnabled())
            log.debug("Cleaning up cache for version [locNodeId=" + ctx.localNodeId() +
                ", topVer=" + topVer + ']');

        for (Iterator<Long> it = affCache.keySet().iterator(); it.hasNext(); )
            if (it.next() < topVer)
                it.remove();
    }

    /**
     * @param topVer Topology version.
     * @return Affinity assignment.
     */
    public List<List<ClusterNode>> assignments(long topVer) {
        GridAffinityAssignment aff = cachedAffinity(topVer);

        return aff.assignment();
    }

    /**
     * Gets future that will be completed after topology with version {@code topVer} is calculated.
     *
     * @param topVer Topology version to await for.
     * @return Future that will be completed after affinity for topology version {@code topVer} is calculated.
     */
    public IgniteInternalFuture<Long> readyFuture(long topVer) {
        GridAffinityAssignment aff = head.get();

        if (aff.topologyVersion() >= topVer) {
            if (log.isDebugEnabled())
                log.debug("Returning finished future for readyFuture [head=" + aff.topologyVersion() +
                    ", topVer=" + topVer + ']');

            return null;
        }

        GridFutureAdapter<Long> fut = F.addIfAbsent(readyFuts, topVer,
            new AffinityReadyFuture(ctx.kernalContext()));

        aff = head.get();

        if (aff.topologyVersion() >= topVer) {
            if (log.isDebugEnabled())
                log.debug("Completing topology ready future right away [head=" + aff.topologyVersion() +
                    ", topVer=" + topVer + ']');

            fut.onDone(topVer);
        }

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
        if (ctx.portableEnabled()) {
            try {
                key = ctx.marshalToPortable(key);
            }
            catch (PortableException e) {
                U.error(log, "Failed to marshal key to portable: " + key, e);
            }
        }

        return aff.partition(affMapper.affinityKey(key));
    }

    /**
     * Gets affinity nodes for specified partition.
     *
     * @param part Partition.
     * @param topVer Topology version.
     * @return Affinity nodes.
     */
    public List<ClusterNode> nodes(int part, long topVer) {
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
    public Set<Integer> primaryPartitions(UUID nodeId, long topVer) {
        return cachedAffinity(topVer).primaryPartitions(nodeId);
    }

    /**
     * Get backup partitions for specified node ID.
     *
     * @param nodeId Node ID to get backup partitions for.
     * @param topVer Topology version.
     * @return Backup partitions for specified node ID.
     */
    public Set<Integer> backupPartitions(UUID nodeId, long topVer) {
        return cachedAffinity(topVer).backupPartitions(nodeId);
    }

    /**
     * Get cached affinity for specified topology version.
     *
     * @param topVer Topology version.
     * @return Cached affinity.
     */
    private GridAffinityAssignment cachedAffinity(long topVer) {
        if (topVer == -1)
            topVer = lastVersion();
        else
            awaitTopologyVersion(topVer);

        assert topVer >= 0 : topVer;

        GridAffinityAssignment cache = head.get();

        if (cache.topologyVersion() != topVer) {
            cache = affCache.get(topVer);

            if (cache == null) {
                throw new IllegalStateException("Getting affinity for topology version earlier than affinity is " +
                    "calculated [locNodeId=" + ctx.localNodeId() + ", topVer=" + topVer +
                    ", head=" + head.get().topologyVersion() + ']');
            }
        }

        assert cache.topologyVersion() == topVer : "Invalid cached affinity: " + cache;

        return cache;
    }

    /**
     * @param topVer Topology version to wait.
     */
    private void awaitTopologyVersion(long topVer) {
        GridAffinityAssignment aff = head.get();

        if (aff.topologyVersion() >= topVer)
            return;

        try {
            if (log.isDebugEnabled())
                log.debug("Will wait for topology version [locNodeId=" + ctx.localNodeId() +
                ", topVer=" + topVer + ']');

            IgniteInternalFuture<Long> fut = readyFuture(topVer);

            if (fut != null)
                fut.get();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to wait for affinity ready future for topology version: " + topVer,
                e);
        }
    }

    /**
     * Sorts nodes according to order.
     *
     * @param nodes Nodes to sort.
     * @return Sorted list of nodes.
     */
    private List<ClusterNode> sort(Collection<ClusterNode> nodes) {
        List<ClusterNode> sorted = new ArrayList<>(nodes.size());

        sorted.addAll(nodes);

        Collections.sort(sorted, nodeCmp);

        return sorted;
    }

    /**
     * Affinity ready future. Will remove itself from ready futures map.
     */
    private class AffinityReadyFuture extends GridFutureAdapter<Long> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Empty constructor required by {@link Externalizable}.
         */
        public AffinityReadyFuture() {
            // No-op.
        }

        /**
         * @param ctx Kernal context.
         */
        private AffinityReadyFuture(GridKernalContext ctx) {
            super(ctx);
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(Long res, @Nullable Throwable err) {
            assert res != null;

            boolean done = super.onDone(res, err);

            if (done)
                readyFuts.remove(res, this);

            return done;
        }
    }
}
