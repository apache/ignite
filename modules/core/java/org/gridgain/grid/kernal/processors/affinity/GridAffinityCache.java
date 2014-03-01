// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.affinity;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Affinity cached function.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridAffinityCache {
    /** Node order comparator. */
    private static final Comparator<GridNode> nodeCmp = new NodeOrderComparator();

    /** Cache name. */
    private final String cacheName;

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

    /**
     * Constructs affinity cached calculations.
     *
     * @param ctx Kernal context.
     * @param cacheName Cache name.
     * @param aff Affinity function.
     * @param affMapper Affinity key mapper.
     */
    public GridAffinityCache(GridKernalContext ctx, String cacheName, GridCacheAffinityFunction aff,
        GridCacheAffinityKeyMapper affMapper) {
        this.ctx = ctx;
        this.aff = aff;
        this.affMapper = affMapper;
        this.cacheName = cacheName;

        partsCnt = aff.partitions();
        affCache = new ConcurrentLinkedHashMap<>();
        head = new AtomicReference<>(new CachedAffinity(-1));
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
                CachedAffinity old = affCache.putIfAbsent(topVer, cache = new CachedAffinity(topVer));

                if (old == null) {
                    cache.calculate(); // Calculate cached affinity.

                    // Update top version, if required.
                    while (true) {
                        CachedAffinity headItem = head.get();

                        if (headItem.topologyVersion() >= topVer)
                            break;

                        if (head.compareAndSet(headItem, cache))
                            break;
                    }
                }
                else
                    cache = old;
            }

            cache.await(); // Waits for cached affinity calculations complete.
        }

        assert cache != null && cache.topologyVersion() == topVer : "Invalid cached affinity: " + cache;
        assert cache.latch.getCount() == 0 : "Expects cache calculations complete: " + cache;

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
     * Cached affinity calculations.
     */
    private final class CachedAffinity {
        /** Topology version. */
        private final long topVer;

        /** Collection of calculated affinity nodes. */
        private List<List<GridNode>> arr;

        /** Map of primary node partitions. */
        private final Map<UUID, Set<Integer>> primary;

        /** Map of backup node partitions. */
        private final Map<UUID, Set<Integer>> backup;

        /** Calculations latch. */
        private final CountDownLatch latch = new CountDownLatch(1);

        /** Flag to check cache is calculated. */
        private final AtomicBoolean calculated = new AtomicBoolean();

        /** Calculation's exception. */
        private volatile GridRuntimeException e;

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
            assert latch.getCount() == 0 && calculated.get() : "Affinity cache is not calculated yet.";

            assert part >= 0 && part < arr.size() : "Affinity partition is out of range" +
                " [part=" + part + ", partitions=" + arr.size() + ']';

            return arr.get(part);
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
         */
        public void calculate() {
            if (calculated.compareAndSet(false, true)) {
                try {
                    // Temporary mirrors with modifiable partition's collections.
                    Map<UUID, Set<Integer>> tmpPrm = new HashMap<>();
                    Map<UUID, Set<Integer>> tmpBkp = new HashMap<>();

                    // Resolve nodes snapshot for specified topology version.
                    Collection<GridNode> nodes = ctx.discovery().cacheAffinityNodes(cacheName, topVer);

                    arr = aff.assignPartitions(new GridCacheAffinityFunctionContextImpl(sort(nodes), topVer));

                    for (int partsCnt = arr.size(), p = 0; p < partsCnt; p++) {
                        // Use the first node as primary, other - backups.
                        Map<UUID, Set<Integer>> tmp = tmpPrm;
                        Map<UUID, Set<Integer>> map = primary;

                        for (GridNode node : arr.get(p)) {
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
                catch (RuntimeException | Error e) {
                    this.e = new GridRuntimeException("Failed to calculate affinity cache" +
                        " [topVer=" + topVer + ", partitions=" + partitions() + ']', e);

                    throw e;
                } finally {
                    latch.countDown();
                }
            }
            else
                await();
        }

        /**
         * Waits for affinity calculations complete.
         */
        public void await() {
            try {
                if (latch.getCount() > 0)
                    latch.await();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new GridRuntimeException("Failed to wait for affinity calculations.", e);
            }

            if (e != null)
                throw e;
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

    /**
     *
     */
    private static class NodeOrderComparator implements Comparator<GridNode>, Serializable {
        /** {@inheritDoc} */
        @Override public int compare(GridNode n1, GridNode n2) {
            return n1.order() < n2.order() ? -1 : n1.order() > n2.order() ? 1 : 0;
        }
    }
}
