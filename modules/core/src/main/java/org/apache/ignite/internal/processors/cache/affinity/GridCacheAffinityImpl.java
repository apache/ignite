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

package org.apache.ignite.internal.processors.cache.affinity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Affinity interface implementation.
 */
public class GridCacheAffinityImpl<K, V> implements Affinity<K> {
    /** */
    public static final String FAILED_TO_FIND_CACHE_ERR_MSG = "Failed to find cache (cache was not started " +
        "yet or cache was already stopped): ";

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
        CacheConfiguration ccfg = cctx.config();

        if (ccfg == null)
            throw new IgniteException(FAILED_TO_FIND_CACHE_ERR_MSG + cctx.name());

        return ccfg.getAffinity().partitions();
    }

    /** {@inheritDoc} */
    @Override public int partition(K key) {
        A.notNull(key, "key");

        return cctx.affinity().partition(key);
    }

    /** {@inheritDoc} */
    @Override public boolean isPrimary(ClusterNode n, K key) {
        A.notNull(n, "n", key, "key");

        return cctx.affinity().primaryByKey(n, key, topologyVersion());
    }

    /** {@inheritDoc} */
    @Override public boolean isBackup(ClusterNode n, K key) {
        A.notNull(n, "n", key, "key");

        return cctx.affinity().backupsByKey(key, topologyVersion()).contains(n);
    }

    /** {@inheritDoc} */
    @Override public boolean isPrimaryOrBackup(ClusterNode n, K key) {
        A.notNull(n, "n", key, "key");

        return cctx.affinity().partitionBelongs(n, cctx.affinity().partition(key), topologyVersion());
    }

    /** {@inheritDoc} */
    @Override public int[] primaryPartitions(ClusterNode n) {
        A.notNull(n, "n");

        Set<Integer> parts = cctx.affinity().primaryPartitions(n.id(), topologyVersion());

        return U.toIntArray(parts);
    }

    /** {@inheritDoc} */
    @Override public int[] backupPartitions(ClusterNode n) {
        A.notNull(n, "n");

        Set<Integer> parts = cctx.affinity().backupPartitions(n.id(), topologyVersion());

        return U.toIntArray(parts);
    }

    /** {@inheritDoc} */
    @Override public int[] allPartitions(ClusterNode n) {
        A.notNull(n, "p");

        Collection<Integer> parts = new HashSet<>();

        AffinityTopologyVersion topVer = topologyVersion();

        for (int partsCnt = partitions(), part = 0; part < partsCnt; part++) {
            for (ClusterNode affNode : cctx.affinity().nodesByPartition(part, topVer)) {
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

        return F.first(cctx.affinity().nodesByPartition(part, topologyVersion()));
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

        if (key instanceof CacheObject && !(key instanceof BinaryObject)) {
            CacheObjectContext ctx = cctx.cacheObjectContext();

            if (ctx == null)
                throw new IgniteException(FAILED_TO_FIND_CACHE_ERR_MSG + cctx.name());

            key = ((CacheObject)key).value(ctx, false);
        }

        CacheConfiguration ccfg = cctx.config();

        if (ccfg == null)
            throw new IgniteException(FAILED_TO_FIND_CACHE_ERR_MSG + cctx.name());

        return ccfg.getAffinityMapper().affinityKey(key);
    }

    /** {@inheritDoc} */
    @Override @Nullable public ClusterNode mapKeyToNode(K key) {
        A.notNull(key, "key");

        return F.first(mapKeysToNodes(F.asList(key)).keySet());
    }

    /** {@inheritDoc} */
    @Override public Map<ClusterNode, Collection<K>> mapKeysToNodes(@Nullable Collection<? extends K> keys) {
        A.notNull(keys, "keys");

        AffinityTopologyVersion topVer = topologyVersion();

        int nodesCnt;

        if (!cctx.isLocal())
            nodesCnt = cctx.discovery().cacheAffinityNodes(cctx.cacheId(), topVer).size();
        else
            nodesCnt = 1;

        // Must return empty map if no alive nodes present or keys is empty.
        Map<ClusterNode, Collection<K>> res = new HashMap<>(nodesCnt, 1.0f);

        for (K key : keys) {
            ClusterNode primary = cctx.affinity().primaryByKey(key, topVer);

            if (primary == null)
                throw new IgniteException("Failed to get primary node [topVer=" + topVer + ", key=" + key + ']');

            Collection<K> mapped = res.get(primary);

            if (mapped == null) {
                mapped = new ArrayList<>(Math.max(keys.size() / nodesCnt, 16));

                res.put(primary, mapped);
            }

            mapped.add(key);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> mapKeyToPrimaryAndBackups(K key) {
        A.notNull(key, "key");

        return cctx.affinity().nodesByPartition(partition(key), topologyVersion());
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> mapPartitionToPrimaryAndBackups(int part) {
        A.ensure(part >= 0 && part < partitions(), "part >= 0 && part < total partitions");

        return cctx.affinity().nodesByPartition(part, topologyVersion());
    }

    /**
     * Gets current topology version.
     *
     * @return Topology version.
     */
    private AffinityTopologyVersion topologyVersion() {
        return cctx.affinity().affinityTopologyVersion();
    }
}
