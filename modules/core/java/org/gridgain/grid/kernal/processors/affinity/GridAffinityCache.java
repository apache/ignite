/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.affinity;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.typedef.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Affinity cached function.
 */
public class GridAffinityCache {
    /** Node order comparator. */
    private static final Comparator<GridNode> nodeCmp = new GridNodeOrderComparator();

    /** Cache name. */
    private final String cacheName;

    /** Number of backups. */
    private int backups;

    /** Affinity function. */
    private final GridCacheAffinityFunction aff;

    /** Partitions count. */
    private final int partsCnt;

    /** Affinity mapper function. */
    private final GridCacheAffinityKeyMapper affMapper;

    /** Affinity calculation results cache: topology version => partition => nodes. */
    private final ConcurrentMap<Long, CachedAffinity> affCache;

    /** Cache item corresponding to the head topology version. */
    private final AtomicReference<CachedAffinity> head;

    /** Discovery manager. */
    private final GridKernalContext ctx;

    /** Ready futures. */
    private ConcurrentMap<Long, AffinityReadyFuture> readyFuts = new ConcurrentHashMap8<>();

    /**
     * Constructs affinity cached calculations.
     *
     * @param ctx Kernal context.
     * @param cacheName Cache name.
     * @param aff Affinity function.
     * @param affMapper Affinity key mapper.
     */
    public GridAffinityCache(GridKernalContext ctx, String cacheName, GridCacheAffinityFunction aff,
        GridCacheAffinityKeyMapper affMapper, int backups) {
        this.ctx = ctx;
        this.aff = aff;
        this.affMapper = affMapper;
        this.cacheName = cacheName;
        this.backups = backups;

        partsCnt = aff.partitions();
        affCache = new ConcurrentLinkedHashMap<>();
        head = new AtomicReference<>(new CachedAffinity(-1));
    }

    /**
     * Initializes affinity with given topology version and assignment. The assignment is calculated on remote nodes
     * and brought to local node on partition map exchange.
     *
     * @param topVer Topology version.
     * @param affAssignment Affinity assignment for topology version.
     */
    public void initialize(long topVer, List<List<GridNode>> affAssignment) {
        CachedAffinity assignment = new CachedAffinity(topVer, affAssignment);

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
    public List<List<GridNode>> calculate(long topVer, GridDiscoveryEvent discoEvt) {
        CachedAffinity updated = new CachedAffinity(topVer);

        updated.calculate(discoEvt);

        updated = F.addIfAbsent(affCache, topVer, updated);

        // Update top version, if required.
        while (true) {
            CachedAffinity headItem = head.get();

            if (headItem.topologyVersion() >= topVer)
                break;

            if (head.compareAndSet(headItem, updated))
                break;
        }

        for (Map.Entry<Long, AffinityReadyFuture> entry : readyFuts.entrySet()) {
            if (entry.getKey() >= topVer)
                entry.getValue().onDone(topVer);
        }

        return updated.assignment;
    }

    /**
     * Clean up outdated cache items.
     *
     * @param topVer Actual topology version, older versions will be removed.
     */
    public void cleanUpCache(long topVer) {
        for (Iterator<Long> it = affCache.keySet().iterator(); it.hasNext(); )
            if (it.next() < topVer)
                it.remove();
    }

    /**
     * @param topVer Topology version.
     * @return Affinity assignment.
     */
    public List<List<GridNode>> assignments(long topVer) {
        CachedAffinity aff = cachedAffinity(topVer);

        return aff.assignment;
    }

    /**
     * Gets future that will be completed after topology with version {@code topVer} is calculated.
     *
     * @param topVer Topology version to await for.
     * @return Future that will be completed after affinity for topology version {@code topVer} is calculated.
     */
    public GridFuture<Long> readyFuture(long topVer) {
        CachedAffinity aff = head.get();

        if (aff.topologyVersion() >= topVer)
            return new GridFinishedFutureEx<>(topVer);

        GridFutureAdapter<Long> fut = F.addIfAbsent(readyFuts, topVer,
            new AffinityReadyFuture(ctx));

        if (head.get().topologyVersion() >= topVer)
            fut.onDone(topVer);

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
        return aff.partition(affMapper.affinityKey(key));
    }

    /**
     * Gets affinity key from cache key.
     *
     * @param key Cache key.
     * @return Affinity key.
     */
    public Object affinityKey(Object key) {
        return affMapper.affinityKey(key);
    }

    /**
     * Gets affinity nodes for specified partition.
     *
     * @param part Partition.
     * @param topVer Topology version.
     * @return Affinity nodes.
     */
    public Collection<GridNode> nodes(int part, long topVer) {
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
    private CachedAffinity cachedAffinity(long topVer) {
        if (topVer == -1)
            topVer = ctx.discovery().topologyVersion();

        assert topVer >= 0;

        CachedAffinity cache = head.get();

        if (cache.topologyVersion() != topVer) {
            cache = affCache.get(topVer);

            if (cache == null) {
                throw new IllegalStateException("Getting affinity for topology version earlier than affinity is " +
                    "calculated [locNodeId=" + ctx.localNodeId() + ", topVer=" + topVer +
                    ", head=" + head.get().topologyVersion() + ']');
            }
        }

        assert cache != null && cache.topologyVersion() == topVer : "Invalid cached affinity: " + cache;

        return cache;
    }

    /**
     * Sorts nodes according to order.
     *
     * @param nodes Nodes to sort.
     * @return Sorted list of nodes.
     */
    private List<GridNode> sort(Collection<GridNode> nodes) {
        List<GridNode> sorted = new ArrayList<>(nodes.size());

        sorted.addAll(nodes);

        Collections.sort(sorted, nodeCmp);

        return sorted;
    }

    /**
     * Affinity ready future. Will remove itself from ready futures map.
     */
    private class AffinityReadyFuture extends GridFutureAdapter<Long> {
        /**
         * Empty constructor required by {@link Externalizable}.
         */
        public AffinityReadyFuture() {
        }

        /**
         * @param ctx Kernal context.
         */
        private AffinityReadyFuture(GridKernalContext ctx) {
            super(ctx);
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable Long res, @Nullable Throwable err) {
            boolean done = super.onDone(res, err);

            if (done)
                readyFuts.remove(res, this);

            return done;
        }
    }

    /**
     * Cached affinity calculations.
     */
    private final class CachedAffinity {
        /** Topology version. */
        private final long topVer;

        /** Collection of calculated affinity nodes. */
        private List<List<GridNode>> assignment;

        /** Map of primary node partitions. */
        private final Map<UUID, Set<Integer>> primary;

        /** Map of backup node partitions. */
        private final Map<UUID, Set<Integer>> backup;

        /**
         * Constructs cached affinity calculations item.
         *
         * @param topVer Topology version.
         */
        private CachedAffinity(long topVer) {
            this.topVer = topVer;
            primary = new HashMap<>();
            backup = new HashMap<>();
        }

        private CachedAffinity(long topVer, List<List<GridNode>> assignment) {
            this.topVer = topVer;
            this.assignment = assignment;

            primary = new HashMap<>();
            backup = new HashMap<>();

            initPrimaryBackupMaps();
        }

        /**
         * @return Topology version.
         */
        public long topologyVersion() {
            return topVer;
        }

        /**
         * Get affinity nodes for partition.
         *
         * @param part Partition.
         * @return Affinity nodes.
         */
        public Collection<GridNode> get(int part) {
            assert part >= 0 && part < assignment.size() : "Affinity partition is out of range" +
                " [part=" + part + ", partitions=" + assignment.size() + ']';

            return assignment.get(part);
        }

        /**
         * Get primary partitions for specified node ID.
         *
         * @param nodeId Node ID to get primary partitions for.
         * @return Primary partitions for specified node ID.
         */
        public Set<Integer> primaryPartitions(UUID nodeId) {
            Set<Integer> set = primary.get(nodeId);

            return set == null ? Collections.<Integer>emptySet() : Collections.unmodifiableSet(set);
        }

        /**
         * Get backup partitions for specified node ID.
         *
         * @param nodeId Node ID to get backup partitions for.
         * @return Backup partitions for specified node ID.
         */
        public Set<Integer> backupPartitions(UUID nodeId) {
            Set<Integer> set = backup.get(nodeId);

            return set == null ? Collections.<Integer>emptySet() : Collections.unmodifiableSet(set);
        }

        /**
         * Calculates affinity cache.
         *
         * @param discoEvt Discovery event.
         */
        public void calculate(GridDiscoveryEvent discoEvt) {
            CachedAffinity prev = affCache.get(topVer - 1);

            // Resolve nodes snapshot for specified topology version.
            Collection<GridNode> nodes = ctx.discovery().cacheAffinityNodes(cacheName, topVer);

            List<GridNode> sorted = sort(nodes);

            assert prev != null || (sorted.size() == 1 && sorted.get(0).equals(ctx.grid().localNode()));

            List<List<GridNode>> prevAssignment = prev == null ? null : prev.assignment;

            assignment = aff.assignPartitions(
                new GridCacheAffinityFunctionContextImpl(sorted, prevAssignment, discoEvt, topVer, backups));

            initPrimaryBackupMaps();
        }

        /**
         * Initializes primary and backup maps.
         */
        private void initPrimaryBackupMaps() {
            // Temporary mirrors with modifiable partition's collections.
            Map<UUID, Set<Integer>> tmpPrm = new HashMap<>();
            Map<UUID, Set<Integer>> tmpBkp = new HashMap<>();

            for (int partsCnt = assignment.size(), p = 0; p < partsCnt; p++) {
                // Use the first node as primary, other - backups.
                Map<UUID, Set<Integer>> tmp = tmpPrm;
                Map<UUID, Set<Integer>> map = primary;

                for (GridNode node : assignment.get(p)) {
                    UUID id = node.id();

                    Set<Integer> set = tmp.get(id);

                    if (set == null) {
                        tmp.put(id, set = new HashSet<>());
                        map.put(id, Collections.unmodifiableSet(set));
                    }

                    set.add(p);

                    // Use the first node as primary, other - backups.
                    tmp = tmpBkp;
                    map = backup;
                }
            }
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return (int)(topVer ^ (topVer >>> 32));
        }

        /** {@inheritDoc} */
        @SuppressWarnings("SimplifiableIfStatement")
        @Override public boolean equals(Object o) {
            if (o == this)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            return topVer == ((CachedAffinity)o).topVer;
        }
    }
}
