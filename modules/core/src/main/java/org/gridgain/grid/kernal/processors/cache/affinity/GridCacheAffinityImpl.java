/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.affinity;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.portables.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Affinity interface implementation.
 */
public class GridCacheAffinityImpl<K, V> implements GridCacheAffinity<K> {
    /** Cache context. */
    private GridCacheContext<K, V> cctx;

    /** Logger. */
    private IgniteLogger log;

    /**
     * @param cctx Context.
     */
    public GridCacheAffinityImpl(GridCacheContext<K, V> cctx) {
        this.cctx = cctx;

        log = cctx.logger(getClass());
    }

    /** {@inheritDoc} */
    @Override public int partitions() {
        return cctx.config().getAffinity().partitions();
    }

    /** {@inheritDoc} */
    @Override public int partition(K key) {
        A.notNull(key, "key");

        return cctx.affinity().partition(key);
    }

    /** {@inheritDoc} */
    @Override public boolean isPrimary(ClusterNode n, K key) {
        A.notNull(n, "n", key, "key");

        return cctx.affinity().primary(n, key, topologyVersion());
    }

    /** {@inheritDoc} */
    @Override public boolean isBackup(ClusterNode n, K key) {
        A.notNull(n, "n", key, "key");

        return cctx.affinity().backups(key, topologyVersion()).contains(n);
    }

    /** {@inheritDoc} */
    @Override public boolean isPrimaryOrBackup(ClusterNode n, K key) {
        A.notNull(n, "n", key, "key");

        return cctx.affinity().belongs(n, key, topologyVersion());
    }

    /** {@inheritDoc} */
    @Override public int[] primaryPartitions(ClusterNode n) {
        A.notNull(n, "n");

        long topVer = cctx.discovery().topologyVersion();

        Set<Integer> parts = cctx.affinity().primaryPartitions(n.id(), topVer);

        return U.toIntArray(parts);
    }

    /** {@inheritDoc} */
    @Override public int[] backupPartitions(ClusterNode n) {
        A.notNull(n, "n");

        long topVer = cctx.discovery().topologyVersion();

        Set<Integer> parts = cctx.affinity().backupPartitions(n.id(), topVer);

        return U.toIntArray(parts);
    }

    /** {@inheritDoc} */
    @Override public int[] allPartitions(ClusterNode n) {
        A.notNull(n, "p");

        Collection<Integer> parts = new HashSet<>();

        long topVer = cctx.discovery().topologyVersion();

        for (int partsCnt = partitions(), part = 0; part < partsCnt; part++) {
            for (ClusterNode affNode : cctx.affinity().nodes(part, topVer)) {
                if (n.id().equals(affNode.id())) {
                    parts.add(part);

                    break;
                }
            }
        }

        return U.toIntArray(parts);
    }

    /** {@inheritDoc} */
    @Override public ClusterNode mapPartitionToNode(int part) {
        A.ensure(part >= 0 && part < partitions(), "part >= 0 && part < total partitions");

        return F.first(cctx.affinity().nodes(part, topologyVersion()));
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, ClusterNode> mapPartitionsToNodes(Collection<Integer> parts) {
        A.notNull(parts, "parts");

        Map<Integer, ClusterNode> map = new HashMap<>();

        if (!F.isEmpty(parts)) {
            for (int p : parts)
                map.put(p, mapPartitionToNode(p));
        }

        return map;
    }

    /** {@inheritDoc} */
    @Override public Object affinityKey(K key) {
        A.notNull(key, "key");

        if (cctx.portableEnabled()) {
            try {
                key = (K)cctx.marshalToPortable(key);
            }
            catch (PortableException e) {
                U.error(log, "Failed to marshal key to portable: " + key, e);
            }
        }

        return cctx.config().getAffinityMapper().affinityKey(key);
    }

    /** {@inheritDoc} */
    @Override @Nullable public ClusterNode mapKeyToNode(K key) {
        A.notNull(key, "key");

        return F.first(mapKeysToNodes(F.asList(key)).keySet());
    }

    /** {@inheritDoc} */
    @Override public Map<ClusterNode, Collection<K>> mapKeysToNodes(@Nullable Collection<? extends K> keys) {
        A.notNull(keys, "keys");

        long topVer = topologyVersion();

        int nodesCnt = cctx.discovery().cacheAffinityNodes(cctx.name(), topVer).size();

        // Must return empty map if no alive nodes present or keys is empty.
        Map<ClusterNode, Collection<K>> res = new HashMap<>(nodesCnt, 1.0f);

        for (K key : keys) {
            ClusterNode primary = cctx.affinity().primary(key, topVer);

            if (primary != null) {
                Collection<K> mapped = res.get(primary);

                if (mapped == null) {
                    mapped = new ArrayList<>(Math.max(keys.size() / nodesCnt, 16));

                    res.put(primary, mapped);
                }

                mapped.add(key);
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> mapKeyToPrimaryAndBackups(K key) {
        A.notNull(key, "key");

        return cctx.affinity().nodes(partition(key), topologyVersion());
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> mapPartitionToPrimaryAndBackups(int part) {
        A.ensure(part >= 0 && part < partitions(), "part >= 0 && part < total partitions");

        return cctx.affinity().nodes(part, topologyVersion());
    }

    /**
     * Gets current topology version.
     *
     * @return Topology version.
     */
    private long topologyVersion() {
        return cctx.affinity().affinityTopologyVersion();
    }
}
