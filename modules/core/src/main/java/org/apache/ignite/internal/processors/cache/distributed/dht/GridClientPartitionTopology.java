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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap2;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.EVICTED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.OWNING;

/**
 * Partition topology for node which does not have any local partitions.
 */
@GridToStringExclude
public class GridClientPartitionTopology implements GridDhtPartitionTopology {
    /** If true, then check consistency. */
    private static final boolean CONSISTENCY_CHECK = false;

    /** Flag to control amount of output for full map. */
    private static final boolean FULL_MAP_DEBUG = false;

    /** */
    private static final Long ZERO = 0L;

    /** Cache shared context. */
    private GridCacheSharedContext cctx;

    /** Cache ID. */
    private int cacheId;

    /** Logger. */
    private final IgniteLogger log;

    /** Node to partition map. */
    private GridDhtPartitionFullMap node2part;

    /** Partition to node map. */
    private Map<Integer, Set<UUID>> part2node = new HashMap<>();

    /** */
    private GridDhtPartitionExchangeId lastExchangeId;

    /** */
    private AffinityTopologyVersion topVer = AffinityTopologyVersion.NONE;

    /** */
    private volatile boolean stopping;

    /** A future that will be completed when topology with version topVer will be ready to use. */
    private volatile GridDhtTopologyFuture topReadyFut;

    /** */
    private final GridAtomicLong updateSeq = new GridAtomicLong(1);

    /** Lock. */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /** Partition update counters. */
    private Map<Integer, Long> cntrMap = new HashMap<>();

    /** */
    private final Object similarAffKey;

    /**
     * @param cctx Context.
     * @param cacheId Cache ID.
     * @param exchFut Exchange ID.
     * @param similarAffKey Key to find caches with similar affinity.
     */
    public GridClientPartitionTopology(
        GridCacheSharedContext cctx,
        int cacheId,
        GridDhtPartitionsExchangeFuture exchFut,
        Object similarAffKey
    ) {
        this.cctx = cctx;
        this.cacheId = cacheId;
        this.similarAffKey = similarAffKey;

        topVer = exchFut.topologyVersion();

        log = cctx.logger(getClass());

        lock.writeLock().lock();

        try {
            beforeExchange0(cctx.localNode(), exchFut);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * @return Key to find caches with similar affinity.
     */
    @Nullable public Object similarAffinityKey() {
        return similarAffKey;
    }

    /**
     * @return Full map string representation.
     */
    @SuppressWarnings( {"ConstantConditions"})
    private String fullMapString() {
        return node2part == null ? "null" : FULL_MAP_DEBUG ? node2part.toFullString() : node2part.toString();
    }

    /**
     * @param map Map to get string for.
     * @return Full map string representation.
     */
    @SuppressWarnings( {"ConstantConditions"})
    private String mapString(GridDhtPartitionMap2 map) {
        return map == null ? "null" : FULL_MAP_DEBUG ? map.toFullString() : map.toString();
    }

    /**
     * @return Cache ID.
     */
    public int cacheId() {
        return cacheId;
    }

    /** {@inheritDoc} */
    @SuppressWarnings( {"LockAcquiredButNotSafelyReleased"})
    @Override public void readLock() {
        lock.readLock().lock();
    }

    /** {@inheritDoc} */
    @Override public void readUnlock() {
        lock.readLock().unlock();
    }

    /** {@inheritDoc} */
    @Override public void updateTopologyVersion(
        GridDhtPartitionExchangeId exchId,
        GridDhtPartitionsExchangeFuture exchFut,
        long updSeq,
        boolean stopping
    ) throws IgniteInterruptedCheckedException {
        U.writeLock(lock);

        try {
            assert exchId.topologyVersion().compareTo(topVer) > 0 : "Invalid topology version [topVer=" + topVer +
                ", exchId=" + exchId + ']';

            this.stopping = stopping;

            topVer = exchId.topologyVersion();

            updateSeq.setIfGreater(updSeq);

            topReadyFut = exchFut;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public AffinityTopologyVersion topologyVersion() {
        lock.readLock().lock();

        try {
            assert topVer.topologyVersion() > 0;

            return topVer;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public GridDhtTopologyFuture topologyVersionFuture() {
        assert topReadyFut != null;

        return topReadyFut;
    }

    /** {@inheritDoc} */
    @Override public boolean stopping() {
        return stopping;
    }

    /** {@inheritDoc} */
    @Override public void initPartitions(GridDhtPartitionsExchangeFuture exchFut) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void beforeExchange(GridDhtPartitionsExchangeFuture exchFut, boolean initParts)
        throws IgniteCheckedException {
        ClusterNode loc = cctx.localNode();

        U.writeLock(lock);

        try {
            if (stopping)
                return;

            beforeExchange0(loc, exchFut);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * @param loc Local node.
     * @param exchFut Exchange future.
     */
    private void beforeExchange0(ClusterNode loc, GridDhtPartitionsExchangeFuture exchFut) {
        GridDhtPartitionExchangeId exchId = exchFut.exchangeId();

        assert topVer.equals(exchId.topologyVersion()) : "Invalid topology version [topVer=" +
            topVer + ", exchId=" + exchId + ']';

        if (!exchId.isJoined())
            removeNode(exchId.nodeId());

        // In case if node joins, get topology at the time of joining node.
        ClusterNode oldest = cctx.discovery().oldestAliveCacheServerNode(topVer);

        assert oldest != null;

        if (log.isDebugEnabled())
            log.debug("Partition map beforeExchange [exchId=" + exchId + ", fullMap=" + fullMapString() + ']');

        long updateSeq = this.updateSeq.incrementAndGet();

        // If this is the oldest node.
        if (oldest.id().equals(loc.id()) || exchFut.isCacheAdded(cacheId, exchId.topologyVersion())) {
            if (node2part == null) {
                node2part = new GridDhtPartitionFullMap(oldest.id(), oldest.order(), updateSeq);

                if (log.isDebugEnabled())
                    log.debug("Created brand new full topology map on oldest node [exchId=" +
                        exchId + ", fullMap=" + fullMapString() + ']');
            }
            else if (!node2part.valid()) {
                node2part = new GridDhtPartitionFullMap(oldest.id(), oldest.order(), updateSeq, node2part, false);

                if (log.isDebugEnabled())
                    log.debug("Created new full topology map on oldest node [exchId=" + exchId + ", fullMap=" +
                        node2part + ']');
            }
            else if (!node2part.nodeId().equals(loc.id())) {
                node2part = new GridDhtPartitionFullMap(oldest.id(), oldest.order(), updateSeq, node2part, false);

                if (log.isDebugEnabled())
                    log.debug("Copied old map into new map on oldest node (previous oldest node left) [exchId=" +
                        exchId + ", fullMap=" + fullMapString() + ']');
            }
        }

        consistencyCheck();

        if (log.isDebugEnabled())
            log.debug("Partition map after beforeExchange [exchId=" + exchId + ", fullMap=" +
                fullMapString() + ']');
    }

    /** {@inheritDoc} */
    @Override public boolean afterExchange(GridDhtPartitionsExchangeFuture exchFut) throws IgniteCheckedException {
        AffinityTopologyVersion topVer = exchFut.topologyVersion();

        lock.writeLock().lock();

        try {
            assert topVer.equals(exchFut.topologyVersion()) : "Invalid topology version [topVer=" +
                topVer + ", exchId=" + exchFut.exchangeId() + ']';

            if (log.isDebugEnabled())
                log.debug("Partition map before afterExchange [exchId=" + exchFut.exchangeId() + ", fullMap=" +
                    fullMapString() + ']');

            updateSeq.incrementAndGet();

            consistencyCheck();
        }
        finally {
            lock.writeLock().unlock();
        }

        return false;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridDhtLocalPartition localPartition(
        int p,
        AffinityTopologyVersion topVer,
        boolean create
    )
        throws GridDhtInvalidPartitionException {
        if (!create)
            return null;

        throw new GridDhtInvalidPartitionException(p, "Adding entry to evicted partition (often may be caused by " +
            "inconsistent 'key.hashCode()' implementation) " +
            "[part=" + p + ", topVer=" + topVer + ", this.topVer=" + this.topVer + ']');
    }

    /** {@inheritDoc} */
    @Override public GridDhtLocalPartition localPartition(Object key, boolean create) {
        return localPartition(1, AffinityTopologyVersion.NONE, create);
    }

    /** {@inheritDoc} */
    @Override public void releasePartitions(int... parts) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public List<GridDhtLocalPartition> localPartitions() {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public Collection<GridDhtLocalPartition> currentLocalPartitions() {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public void onRemoved(GridDhtCacheEntry e) {
        assert false : "Entry should not be removed from client topology: " + e;
    }

    /** {@inheritDoc} */
    @Override public GridDhtPartitionMap2 localPartitionMap() {
        lock.readLock().lock();

        try {
            return new GridDhtPartitionMap2(cctx.localNodeId(), updateSeq.get(), topVer,
                Collections.<Integer, GridDhtPartitionState>emptyMap(), true);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public GridDhtPartitionState partitionState(UUID nodeId, int part) {
        lock.readLock().lock();

        try {
            GridDhtPartitionMap2 partMap = node2part.get(nodeId);

            if (partMap != null) {
                GridDhtPartitionState state = partMap.get(part);

                return state == null ? EVICTED : state;
            }

            return EVICTED;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public List<ClusterNode> nodes(int p, AffinityTopologyVersion topVer) {
        lock.readLock().lock();

        try {
            assert node2part != null && node2part.valid() : "Invalid node-to-partitions map [topVer=" + topVer +
                ", node2part=" + node2part + ']';

            List<ClusterNode> nodes = null;

            Collection<UUID> nodeIds = part2node.get(p);

            if (!F.isEmpty(nodeIds)) {
                for (UUID nodeId : nodeIds) {
                    ClusterNode n = cctx.discovery().node(nodeId);

                    if (n != null && (topVer.topologyVersion() < 0 || n.order() <= topVer.topologyVersion())) {
                        if (nodes == null)
                            nodes = new ArrayList<>(nodeIds.size());

                        nodes.add(n);
                    }
                }
            }

            return nodes;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * @param p Partition.
     * @param topVer Topology version ({@code -1} for all nodes).
     * @param state Partition state.
     * @param states Additional partition states.
     * @return List of nodes for the partition.
     */
    private List<ClusterNode> nodes(int p, AffinityTopologyVersion topVer, GridDhtPartitionState state, GridDhtPartitionState... states) {
        Collection<UUID> allIds = topVer.topologyVersion() > 0 ? F.nodeIds(CU.allNodes(cctx, topVer)) : null;

        lock.readLock().lock();

        try {
            assert node2part != null && node2part.valid() : "Invalid node-to-partitions map [topVer=" + topVer +
                ", allIds=" + allIds + ", node2part=" + node2part + ']';

            Collection<UUID> nodeIds = part2node.get(p);

            // Node IDs can be null if both, primary and backup, nodes disappear.
            int size = nodeIds == null ? 0 : nodeIds.size();

            if (size == 0)
                return Collections.emptyList();

            List<ClusterNode> nodes = new ArrayList<>(size);

            for (UUID id : nodeIds) {
                if (topVer.topologyVersion() > 0 && !allIds.contains(id))
                    continue;

                if (hasState(p, id, state, states)) {
                    ClusterNode n = cctx.discovery().node(id);

                    if (n != null && (topVer.topologyVersion() < 0 || n.order() <= topVer.topologyVersion()))
                        nodes.add(n);
                }
            }

            return nodes;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public List<ClusterNode> owners(int p, AffinityTopologyVersion topVer) {
        return nodes(p, topVer, OWNING);
    }

    /** {@inheritDoc} */
    @Override public List<ClusterNode> owners(int p) {
        return owners(p, AffinityTopologyVersion.NONE);
    }

    /** {@inheritDoc} */
    @Override public List<ClusterNode> moving(int p) {
        return nodes(p, AffinityTopologyVersion.NONE, MOVING);
    }

    /**
     * @param p Partition.
     * @param topVer Topology version.
     * @return List of nodes in state OWNING or MOVING.
     */
    private List<ClusterNode> ownersAndMoving(int p, AffinityTopologyVersion topVer) {
        return nodes(p, topVer, OWNING, MOVING);
    }

    /** {@inheritDoc} */
    @Override public long updateSequence() {
        return updateSeq.get();
    }

    /**
     * @return Last update sequence.
     */
    public long lastUpdateSequence() {
        lock.writeLock().lock();

        try {
            return updateSeq.incrementAndGet();
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public GridDhtPartitionFullMap partitionMap(boolean onlyActive) {
        lock.readLock().lock();

        try {
            assert node2part != null && node2part.valid() : "Invalid node2part [node2part: " + node2part +
                ", locNodeId=" + cctx.localNodeId() +
                ", gridName=" + cctx.gridName() + ']';

            GridDhtPartitionFullMap m = node2part;

            return new GridDhtPartitionFullMap(m.nodeId(), m.nodeOrder(), m.updateSequence(), m, onlyActive);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection"})
    @Nullable @Override public boolean update(@Nullable GridDhtPartitionExchangeId exchId,
        GridDhtPartitionFullMap partMap,
        Map<Integer, Long> cntrMap) {
        if (log.isDebugEnabled())
            log.debug("Updating full partition map [exchId=" + exchId + ", parts=" + fullMapString() + ']');

        lock.writeLock().lock();

        try {
            if (exchId != null && lastExchangeId != null && lastExchangeId.compareTo(exchId) >= 0) {
                if (log.isDebugEnabled())
                    log.debug("Stale exchange id for full partition map update (will ignore) [lastExchId=" +
                        lastExchangeId + ", exchId=" + exchId + ']');

                return false;
            }

            if (node2part != null && node2part.compareTo(partMap) >= 0) {
                if (log.isDebugEnabled())
                    log.debug("Stale partition map for full partition map update (will ignore) [lastExchId=" +
                        lastExchangeId + ", exchId=" + exchId + ", curMap=" + node2part + ", newMap=" + partMap + ']');

                return false;
            }

            updateSeq.incrementAndGet();

            if (exchId != null)
                lastExchangeId = exchId;

            if (node2part != null) {
                for (GridDhtPartitionMap2 part : node2part.values()) {
                    GridDhtPartitionMap2 newPart = partMap.get(part.nodeId());

                    // If for some nodes current partition has a newer map,
                    // then we keep the newer value.
                    if (newPart != null && newPart.updateSequence() < part.updateSequence()) {
                        if (log.isDebugEnabled())
                            log.debug("Overriding partition map in full update map [exchId=" + exchId + ", curPart=" +
                                mapString(part) + ", newPart=" + mapString(newPart) + ']');

                        partMap.put(part.nodeId(), part);
                    }
                }

                for (Iterator<UUID> it = partMap.keySet().iterator(); it.hasNext();) {
                    UUID nodeId = it.next();

                    if (!cctx.discovery().alive(nodeId)) {
                        if (log.isDebugEnabled())
                            log.debug("Removing left node from full map update [nodeId=" + nodeId + ", partMap=" +
                                partMap + ']');

                        it.remove();
                    }
                }
            }

            node2part = partMap;

            Map<Integer, Set<UUID>> p2n = new HashMap<>();

            for (Map.Entry<UUID, GridDhtPartitionMap2> e : partMap.entrySet()) {
                for (Integer p : e.getValue().keySet()) {
                    Set<UUID> ids = p2n.get(p);

                    if (ids == null)
                        // Initialize HashSet to size 3 in anticipation that there won't be
                        // more than 3 nodes per partitions.
                        p2n.put(p, ids = U.newHashSet(3));

                    ids.add(e.getKey());
                }
            }

            part2node = p2n;

            if (cntrMap != null)
                this.cntrMap = new HashMap<>(cntrMap);

            consistencyCheck();

            if (log.isDebugEnabled())
                log.debug("Partition map after full update: " + fullMapString());

            return false;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public boolean update(@Nullable GridDhtPartitionExchangeId exchId,
        GridDhtPartitionMap2 parts,
        Map<Integer, Long> cntrMap,
        boolean checkEvictions) {
        if (log.isDebugEnabled())
            log.debug("Updating single partition map [exchId=" + exchId + ", parts=" + mapString(parts) + ']');

        if (!cctx.discovery().alive(parts.nodeId())) {
            if (log.isDebugEnabled())
                log.debug("Received partition update for non-existing node (will ignore) [exchId=" + exchId +
                    ", parts=" + parts + ']');

            return false;
        }

        lock.writeLock().lock();

        try {
            if (stopping)
                return false;

            if (lastExchangeId != null && exchId != null && lastExchangeId.compareTo(exchId) > 0) {
                if (log.isDebugEnabled())
                    log.debug("Stale exchange id for single partition map update (will ignore) [lastExchId=" +
                        lastExchangeId + ", exchId=" + exchId + ']');

                return false;
            }

            if (exchId != null)
                lastExchangeId = exchId;

            if (node2part == null) {
                // Create invalid partition map.
                node2part = new GridDhtPartitionFullMap();
            }

            GridDhtPartitionMap2 cur = node2part.get(parts.nodeId());

            if (cur != null && cur.updateSequence() >= parts.updateSequence()) {
                if (log.isDebugEnabled())
                    log.debug("Stale update sequence for single partition map update (will ignore) [exchId=" + exchId +
                        ", curSeq=" + cur.updateSequence() + ", newSeq=" + parts.updateSequence() + ']');

                return false;
            }

            long updateSeq = this.updateSeq.incrementAndGet();

            node2part.updateSequence(updateSeq);

            boolean changed = cur == null || !cur.equals(parts);

            if (changed) {
                node2part.put(parts.nodeId(), parts);

                // Add new mappings.
                for (Integer p : parts.keySet()) {
                    Set<UUID> ids = part2node.get(p);

                    if (ids == null)
                        // Initialize HashSet to size 3 in anticipation that there won't be
                        // more than 3 nodes per partition.
                        part2node.put(p, ids = U.newHashSet(3));

                    ids.add(parts.nodeId());
                }

                // Remove obsolete mappings.
                if (cur != null) {
                    for (Integer p : cur.keySet()) {
                        if (parts.containsKey(p))
                            continue;

                        Set<UUID> ids = part2node.get(p);

                        if (ids != null)
                            ids.remove(parts.nodeId());
                    }
                }
            }
            else
                cur.updateSequence(parts.updateSequence(), parts.topologyVersion());

            if (cntrMap != null) {
                for (Map.Entry<Integer, Long> e : cntrMap.entrySet()) {
                    Long cntr = this.cntrMap.get(e.getKey());

                    if (cntr == null || cntr < e.getValue())
                        this.cntrMap.put(e.getKey(), e.getValue());
                }
            }

            consistencyCheck();

            if (log.isDebugEnabled())
                log.debug("Partition map after single update: " + fullMapString());

            return changed;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void checkEvictions() {
        // No-op.
    }

    /**
     * Updates value for single partition.
     *
     * @param p Partition.
     * @param nodeId Node ID.
     * @param state State.
     * @param updateSeq Update sequence.
     */
    private void updateLocal(int p, UUID nodeId, GridDhtPartitionState state, long updateSeq) {
        assert lock.isWriteLockedByCurrentThread();
        assert nodeId.equals(cctx.localNodeId());

        // In case if node joins, get topology at the time of joining node.
        ClusterNode oldest = cctx.discovery().oldestAliveCacheServerNode(topVer);

        // If this node became the oldest node.
        if (oldest.id().equals(cctx.localNodeId())) {
            long seq = node2part.updateSequence();

            if (seq != updateSeq) {
                if (seq > updateSeq) {
                    if (this.updateSeq.get() < seq) {
                        // Update global counter if necessary.
                        boolean b = this.updateSeq.compareAndSet(this.updateSeq.get(), seq + 1);

                        assert b : "Invalid update sequence [updateSeq=" + updateSeq + ", seq=" + seq +
                            ", curUpdateSeq=" + this.updateSeq.get() + ", node2part=" + node2part.toFullString() + ']';

                        updateSeq = seq + 1;
                    }
                    else
                        updateSeq = seq;
                }

                node2part.updateSequence(updateSeq);
            }
        }

        GridDhtPartitionMap2 map = node2part.get(nodeId);

        if (map == null)
            node2part.put(nodeId, map = new GridDhtPartitionMap2(nodeId, updateSeq, topVer,
                Collections.<Integer, GridDhtPartitionState>emptyMap(), false));

        map.updateSequence(updateSeq, topVer);

        map.put(p, state);

        Set<UUID> ids = part2node.get(p);

        if (ids == null)
            part2node.put(p, ids = U.newHashSet(3));

        ids.add(nodeId);
    }

    /**
     * @param nodeId Node to remove.
     */
    private void removeNode(UUID nodeId) {
        assert nodeId != null;
        assert lock.writeLock().isHeldByCurrentThread();

        ClusterNode oldest = cctx.discovery().oldestAliveCacheServerNode(topVer);

        ClusterNode loc = cctx.localNode();

        if (node2part != null) {
            if (oldest.equals(loc) && !node2part.nodeId().equals(loc.id())) {
                updateSeq.setIfGreater(node2part.updateSequence());

                node2part = new GridDhtPartitionFullMap(loc.id(), loc.order(), updateSeq.incrementAndGet(),
                    node2part, false);
            }
            else
                node2part = new GridDhtPartitionFullMap(node2part, node2part.updateSequence());

            part2node = new HashMap<>(part2node);

            GridDhtPartitionMap2 parts = node2part.remove(nodeId);

            if (parts != null) {
                for (Integer p : parts.keySet()) {
                    Set<UUID> nodeIds = part2node.get(p);

                    if (nodeIds != null) {
                        nodeIds.remove(nodeId);

                        if (nodeIds.isEmpty())
                            part2node.remove(p);
                    }
                }
            }

            consistencyCheck();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean own(GridDhtLocalPartition part) {
        assert false : "Client topology should never own a partition: " + part;

        return false;
    }

    /** {@inheritDoc} */
    @Override public void onEvicted(GridDhtLocalPartition part, boolean updateSeq) {
        assert updateSeq || lock.isWriteLockedByCurrentThread();

        lock.writeLock().lock();

        try {
            if (stopping)
                return;

            assert part.state() == EVICTED;

            long seq = updateSeq ? this.updateSeq.incrementAndGet() : this.updateSeq.get();

            updateLocal(part.id(), cctx.localNodeId(), part.state(), seq);

            consistencyCheck();
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, Long> updateCounters(boolean skipZeros) {
        lock.readLock().lock();

        try {
            if (skipZeros) {
                Map<Integer, Long> res = U.newHashMap(cntrMap.size());

                for (Map.Entry<Integer, Long> e : cntrMap.entrySet()) {
                    if (!e.getValue().equals(ZERO))
                        res.put(e.getKey(), e.getValue());
                }

                return res;
            }
            else
                return new HashMap<>(cntrMap);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean rebalanceFinished(AffinityTopologyVersion topVer) {
        assert false : "Should not be called on non-affinity node";

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean hasMovingPartitions() {
        lock.readLock().lock();

        try {
            assert node2part != null && node2part.valid() : "Invalid node2part [node2part: " + node2part +
                ", locNodeId=" + cctx.localNodeId() +
                ", gridName=" + cctx.gridName() + ']';

            for (GridDhtPartitionMap2 map : node2part.values()) {
                if (map.hasMovingPartitions())
                    return true;
            }

            return false;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats(int threshold) {
        X.println(">>>  Cache partition topology stats [grid=" + cctx.gridName() + ", cacheId=" + cacheId + ']');
    }

    /**
     * @param p Partition.
     * @param nodeId Node ID.
     * @param match State to match.
     * @param matches Additional states.
     * @return Filter for owners of this partition.
     */
    private boolean hasState(final int p, @Nullable UUID nodeId, final GridDhtPartitionState match,
        final GridDhtPartitionState... matches) {
        if (nodeId == null)
            return false;

        GridDhtPartitionMap2 parts = node2part.get(nodeId);

        // Set can be null if node has been removed.
        if (parts != null) {
            GridDhtPartitionState state = parts.get(p);

            if (state == match)
                return true;

            if (matches != null && matches.length > 0)
                for (GridDhtPartitionState s : matches)
                    if (state == s)
                        return true;
        }

        return false;
    }

    /**
     * Checks consistency after all operations.
     */
    private void consistencyCheck() {
        if (CONSISTENCY_CHECK) {
            assert lock.writeLock().isHeldByCurrentThread();

            if (node2part == null)
                return;

            for (Map.Entry<UUID, GridDhtPartitionMap2> e : node2part.entrySet()) {
                for (Integer p : e.getValue().keySet()) {
                    Set<UUID> nodeIds = part2node.get(p);

                    assert nodeIds != null : "Failed consistency check [part=" + p + ", nodeId=" + e.getKey() + ']';
                    assert nodeIds.contains(e.getKey()) : "Failed consistency check [part=" + p + ", nodeId=" +
                        e.getKey() + ", nodeIds=" + nodeIds + ']';
                }
            }

            for (Map.Entry<Integer, Set<UUID>> e : part2node.entrySet()) {
                for (UUID nodeId : e.getValue()) {
                    GridDhtPartitionMap2 map = node2part.get(nodeId);

                    assert map != null : "Failed consistency check [part=" + e.getKey() + ", nodeId=" + nodeId + ']';
                    assert map.containsKey(e.getKey()) : "Failed consistency check [part=" + e.getKey() +
                        ", nodeId=" + nodeId + ']';
                }
            }
        }
    }
}
