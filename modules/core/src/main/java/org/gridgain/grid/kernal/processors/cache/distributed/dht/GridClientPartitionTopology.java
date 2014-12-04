/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.apache.ignite.cluster.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.preloader.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.locks.*;

import static org.gridgain.grid.kernal.processors.cache.distributed.dht.GridDhtPartitionState.*;

/**
 * Partition topology for node which does not have any local partitions.
 */
@GridToStringExclude
public class GridClientPartitionTopology<K, V> implements GridDhtPartitionTopology<K, V> {
    /** If true, then check consistency. */
    private static final boolean CONSISTENCY_CHECK = false;

    /** Flag to control amount of output for full map. */
    private static final boolean FULL_MAP_DEBUG = false;

    /** Cache shared context. */
    private GridCacheSharedContext<K, V> cctx;

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
    private long topVer = -1;

    /** A future that will be completed when topology with version topVer will be ready to use. */
    private GridDhtTopologyFuture topReadyFut;

    /** */
    private final GridAtomicLong updateSeq = new GridAtomicLong(1);

    /** Lock. */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * @param cctx Context.
     * @param cacheId Cache ID.
     * @param exchId Exchange ID.
     */
    public GridClientPartitionTopology(GridCacheSharedContext<K, V> cctx, int cacheId,
        GridDhtPartitionExchangeId exchId) {
        this.cctx = cctx;
        this.cacheId = cacheId;

        topVer = exchId.topologyVersion();

        log = cctx.logger(getClass());

        beforeExchange(exchId);
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
    private String mapString(GridDhtPartitionMap map) {
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
    @Override public void updateTopologyVersion(GridDhtPartitionExchangeId exchId,
        GridDhtPartitionsExchangeFuture<K, V> exchFut) {
        lock.writeLock().lock();

        try {
            assert exchId.topologyVersion() > topVer : "Invalid topology version [topVer=" + topVer +
                ", exchId=" + exchId + ']';

            topVer = exchId.topologyVersion();

            topReadyFut = exchFut;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public long topologyVersion() {
        lock.readLock().lock();

        try {
            assert topVer > 0;

            return topVer;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public GridDhtTopologyFuture topologyVersionFuture() {
        lock.readLock().lock();

        try {
            assert topReadyFut != null;

            return topReadyFut;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void beforeExchange(GridDhtPartitionExchangeId exchId) {
        ClusterNode loc = cctx.localNode();

        lock.writeLock().lock();

        try {
            assert topVer == exchId.topologyVersion() : "Invalid topology version [topVer=" +
                topVer + ", exchId=" + exchId + ']';

            if (!exchId.isJoined())
                removeNode(exchId.nodeId());

            // In case if node joins, get topology at the time of joining node.
            ClusterNode oldest = CU.oldest(cctx, topVer);

            if (log.isDebugEnabled())
                log.debug("Partition map beforeExchange [exchId=" + exchId + ", fullMap=" + fullMapString() + ']');

            long updateSeq = this.updateSeq.incrementAndGet();

            // If this is the oldest node.
            if (oldest.id().equals(loc.id())) {
                if (node2part == null) {
                    node2part = new GridDhtPartitionFullMap(loc.id(), loc.order(), updateSeq);

                    if (log.isDebugEnabled())
                        log.debug("Created brand new full topology map on oldest node [exchId=" +
                            exchId + ", fullMap=" + fullMapString() + ']');
                }
                else if (!node2part.valid()) {
                    node2part = new GridDhtPartitionFullMap(loc.id(), loc.order(), updateSeq, node2part, false);

                    if (log.isDebugEnabled())
                        log.debug("Created new full topology map on oldest node [exchId=" + exchId + ", fullMap=" +
                            node2part + ']');
                }
                else if (!node2part.nodeId().equals(loc.id())) {
                    node2part = new GridDhtPartitionFullMap(loc.id(), loc.order(), updateSeq, node2part, false);

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
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean afterExchange(GridDhtPartitionExchangeId exchId) throws GridException {
        long topVer = exchId.topologyVersion();

        lock.writeLock().lock();

        try {
            assert topVer == exchId.topologyVersion() : "Invalid topology version [topVer=" +
                topVer + ", exchId=" + exchId + ']';

            if (log.isDebugEnabled())
                log.debug("Partition map before afterExchange [exchId=" + exchId + ", fullMap=" +
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
    @Nullable @Override public GridDhtLocalPartition<K, V> localPartition(int p, long topVer, boolean create)
        throws GridDhtInvalidPartitionException {
        if (!create)
            return null;

        throw new GridDhtInvalidPartitionException(p, "Adding entry to evicted partition [part=" + p +
            ", topVer=" + topVer + ", this.topVer=" + this.topVer + ']');
    }

    /** {@inheritDoc} */
    @Override public GridDhtLocalPartition<K, V> localPartition(K key, boolean create) {
        return localPartition(1, -1, create);
    }

    /** {@inheritDoc} */
    @Override public List<GridDhtLocalPartition<K, V>> localPartitions() {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public Collection<GridDhtLocalPartition<K, V>> currentLocalPartitions() {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public GridDhtLocalPartition<K, V> onAdded(long topVer, GridDhtCacheEntry<K, V> e) {
        assert false : "Entry should not be added to client topology: " + e;

        return null;
    }

    /** {@inheritDoc} */
    @Override public void onRemoved(GridDhtCacheEntry<K, V> e) {
        assert false : "Entry should not be removed from client topology: " + e;
    }

    /** {@inheritDoc} */
    @Override public GridDhtPartitionMap localPartitionMap() {
        lock.readLock().lock();

        try {
            return new GridDhtPartitionMap(cctx.localNodeId(), updateSeq.get(),
                Collections.<Integer, GridDhtPartitionState>emptyMap(), true);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> nodes(int p, long topVer) {
        lock.readLock().lock();

        try {
            assert node2part != null && node2part.valid() : "Invalid node-to-partitions map [topVer=" + topVer +
                ", node2part=" + node2part + ']';

            Collection<ClusterNode> nodes = null;

            Collection<UUID> nodeIds = part2node.get(p);

            if (!F.isEmpty(nodeIds)) {
                for (UUID nodeId : nodeIds) {
                    ClusterNode n = cctx.discovery().node(nodeId);

                    if (n != null && (topVer < 0 || n.order() <= topVer)) {
                        if (nodes == null)
                            nodes = new ArrayList<>();

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
    private List<ClusterNode> nodes(int p, long topVer, GridDhtPartitionState state, GridDhtPartitionState... states) {
        Collection<UUID> allIds = topVer > 0 ? F.nodeIds(CU.allNodes(cctx, topVer)) : null;

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
                if (topVer > 0 && !allIds.contains(id))
                    continue;

                if (hasState(p, id, state, states)) {
                    ClusterNode n = cctx.discovery().node(id);

                    if (n != null && (topVer < 0 || n.order() <= topVer))
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
    @Override public List<ClusterNode> owners(int p, long topVer) {
        return nodes(p, topVer, OWNING);
    }

    /** {@inheritDoc} */
    @Override public List<ClusterNode> owners(int p) {
        return owners(p, -1);
    }

    /** {@inheritDoc} */
    @Override public List<ClusterNode> moving(int p) {
        return nodes(p, -1, MOVING);
    }

    /**
     * @param p Partition.
     * @param topVer Topology version.
     * @return List of nodes in state OWNING or MOVING.
     */
    private List<ClusterNode> ownersAndMoving(int p, long topVer) {
        return nodes(p, topVer, OWNING, MOVING);
    }

    /** {@inheritDoc} */
    @Override public long updateSequence() {
        return updateSeq.get();
    }

    /** {@inheritDoc} */
    @Override public GridDhtPartitionFullMap partitionMap(boolean onlyActive) {
        lock.readLock().lock();

        try {
            assert node2part != null && node2part.valid() : "Invalid node2part [node2part: " + node2part +
                ", locNodeId=" + cctx.localNodeId() + ", gridName=" + cctx.gridName() + ']';

            GridDhtPartitionFullMap m = node2part;

            return new GridDhtPartitionFullMap(m.nodeId(), m.nodeOrder(), m.updateSequence(), m, onlyActive);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection"})
    @Nullable @Override public GridDhtPartitionMap update(@Nullable GridDhtPartitionExchangeId exchId,
        GridDhtPartitionFullMap partMap) {
        if (log.isDebugEnabled())
            log.debug("Updating full partition map [exchId=" + exchId + ", parts=" + fullMapString() + ']');

        lock.writeLock().lock();

        try {
            if (exchId != null && lastExchangeId != null && lastExchangeId.compareTo(exchId) >= 0) {
                if (log.isDebugEnabled())
                    log.debug("Stale exchange id for full partition map update (will ignore) [lastExchId=" +
                        lastExchangeId + ", exchId=" + exchId + ']');

                return null;
            }

            if (node2part != null && node2part.compareTo(partMap) >= 0) {
                if (log.isDebugEnabled())
                    log.debug("Stale partition map for full partition map update (will ignore) [lastExchId=" +
                        lastExchangeId + ", exchId=" + exchId + ", curMap=" + node2part + ", newMap=" + partMap + ']');

                return null;
            }

            updateSeq.incrementAndGet();

            if (exchId != null)
                lastExchangeId = exchId;

            if (node2part != null) {
                for (GridDhtPartitionMap part : node2part.values()) {
                    GridDhtPartitionMap newPart = partMap.get(part.nodeId());

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

            for (Map.Entry<UUID, GridDhtPartitionMap> e : partMap.entrySet()) {
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

            consistencyCheck();

            if (log.isDebugEnabled())
                log.debug("Partition map after full update: " + fullMapString());

            return null;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection"})
    @Nullable @Override public GridDhtPartitionMap update(@Nullable GridDhtPartitionExchangeId exchId,
        GridDhtPartitionMap parts) {
        if (log.isDebugEnabled())
            log.debug("Updating single partition map [exchId=" + exchId + ", parts=" + mapString(parts) + ']');

        if (!cctx.discovery().alive(parts.nodeId())) {
            if (log.isDebugEnabled())
                log.debug("Received partition update for non-existing node (will ignore) [exchId=" + exchId +
                    ", parts=" + parts + ']');

            return null;
        }

        lock.writeLock().lock();

        try {
            if (lastExchangeId != null && exchId != null && lastExchangeId.compareTo(exchId) > 0) {
                if (log.isDebugEnabled())
                    log.debug("Stale exchange id for single partition map update (will ignore) [lastExchId=" +
                        lastExchangeId + ", exchId=" + exchId + ']');

                return null;
            }

            if (exchId != null)
                lastExchangeId = exchId;

            if (node2part == null) {
                U.dumpStack(log, "Created invalid: " + node2part);

                // Create invalid partition map.
                node2part = new GridDhtPartitionFullMap();
            }

            GridDhtPartitionMap cur = node2part.get(parts.nodeId());

            if (cur != null && cur.updateSequence() >= parts.updateSequence()) {
                if (log.isDebugEnabled())
                    log.debug("Stale update sequence for single partition map update (will ignore) [exchId=" + exchId +
                        ", curSeq=" + cur.updateSequence() + ", newSeq=" + parts.updateSequence() + ']');

                return null;
            }

            long updateSeq = this.updateSeq.incrementAndGet();

            node2part = new GridDhtPartitionFullMap(node2part, updateSeq);

            boolean changed = false;

            if (cur == null || !cur.equals(parts))
                changed = true;

            node2part.put(parts.nodeId(), parts);

            part2node = new HashMap<>(part2node);

            // Add new mappings.
            for (Integer p : parts.keySet()) {
                Set<UUID> ids = part2node.get(p);

                if (ids == null)
                    // Initialize HashSet to size 3 in anticipation that there won't be
                    // more than 3 nodes per partition.
                    part2node.put(p, ids = U.newHashSet(3));

                changed |= ids.add(parts.nodeId());
            }

            // Remove obsolete mappings.
            if (cur != null) {
                for (Integer p : F.view(cur.keySet(), F0.notIn(parts.keySet()))) {
                    Set<UUID> ids = part2node.get(p);

                    if (ids != null)
                        changed |= ids.remove(parts.nodeId());
                }
            }

            consistencyCheck();

            if (log.isDebugEnabled())
                log.debug("Partition map after single update: " + fullMapString());

            return changed ? localPartitionMap() : null;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Updates value for single partition.
     *
     * @param p Partition.
     * @param nodeId Node ID.
     * @param state State.
     * @param updateSeq Update sequence.
     */
    @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection"})
    private void updateLocal(int p, UUID nodeId, GridDhtPartitionState state, long updateSeq) {
        assert lock.isWriteLockedByCurrentThread();
        assert nodeId.equals(cctx.localNodeId());

        // In case if node joins, get topology at the time of joining node.
        ClusterNode oldest = CU.oldest(cctx, topVer);

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

        GridDhtPartitionMap map = node2part.get(nodeId);

        if (map == null)
            node2part.put(nodeId, map = new GridDhtPartitionMap(nodeId, updateSeq,
                Collections.<Integer, GridDhtPartitionState>emptyMap(), false));

        map.updateSequence(updateSeq);

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

        ClusterNode oldest = CU.oldest(cctx, topVer);

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

            GridDhtPartitionMap parts = node2part.remove(nodeId);

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
    @Override public boolean own(GridDhtLocalPartition<K, V> part) {
        assert false : "Client topology should never own a partition: " + part;

        return false;
    }

    /** {@inheritDoc} */
    @Override public void onEvicted(GridDhtLocalPartition<K, V> part, boolean updateSeq) {
        assert updateSeq || lock.isWriteLockedByCurrentThread();

        lock.writeLock().lock();

        try {
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
    @Nullable @Override public GridDhtPartitionMap partitions(UUID nodeId) {
        lock.readLock().lock();

        try {
            return node2part.get(nodeId);
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

        GridDhtPartitionMap parts = node2part.get(nodeId);

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

            for (Map.Entry<UUID, GridDhtPartitionMap> e : node2part.entrySet()) {
                for (Integer p : e.getValue().keySet()) {
                    Set<UUID> nodeIds = part2node.get(p);

                    assert nodeIds != null : "Failed consistency check [part=" + p + ", nodeId=" + e.getKey() + ']';
                    assert nodeIds.contains(e.getKey()) : "Failed consistency check [part=" + p + ", nodeId=" +
                        e.getKey() + ", nodeIds=" + nodeIds + ']';
                }
            }

            for (Map.Entry<Integer, Set<UUID>> e : part2node.entrySet()) {
                for (UUID nodeId : e.getValue()) {
                    GridDhtPartitionMap map = node2part.get(nodeId);

                    assert map != null : "Failed consistency check [part=" + e.getKey() + ", nodeId=" + nodeId + ']';
                    assert map.containsKey(e.getKey()) : "Failed consistency check [part=" + e.getKey() +
                        ", nodeId=" + nodeId + ']';
                }
            }
        }
    }
}
