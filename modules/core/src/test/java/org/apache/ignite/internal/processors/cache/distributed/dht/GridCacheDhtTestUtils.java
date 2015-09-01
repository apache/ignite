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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheConcurrentMap;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.internal.CU;

import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.OWNING;

/**
 * Utility methods for dht preloader testing.
 */
public class GridCacheDhtTestUtils {
    /**
     * Ensure singleton.
     */
    private GridCacheDhtTestUtils() {
        // No-op.
    }

    /**
     * @param dht Cache.
     * @param keyCnt Number of test keys to put into cache.
     * @throws IgniteCheckedException If failed to prepare.
     */
    @SuppressWarnings({"UnusedAssignment", "unchecked"})
    static void prepareKeys(GridDhtCache<Integer, String> dht, int keyCnt) throws IgniteCheckedException {
        AffinityFunction aff = dht.context().config().getAffinity();

        GridCacheConcurrentMap cacheMap;

        try {
            Field field = GridCacheAdapter.class.getDeclaredField("map");

            field.setAccessible(true);

            cacheMap = (GridCacheConcurrentMap)field.get(dht);
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Failed to get cache map.", e);
        }

        GridDhtPartitionTopology top = dht.topology();

        GridCacheContext ctx = dht.context();

        for (int i = 0; i < keyCnt; i++) {
            KeyCacheObject cacheKey = ctx.toCacheKeyObject(i);

            cacheMap.putEntry(AffinityTopologyVersion.NONE, cacheKey, ctx.toCacheKeyObject("value" + i));

            dht.preloader().request(Collections.singleton(cacheKey), AffinityTopologyVersion.NONE);

            GridDhtLocalPartition part = top.localPartition(aff.partition(i), false);

            assert part != null;

            part.own();
        }
    }

    /**
     * @param dht Dht cache.
     * @param idx Cache index
     */
    static void printDhtTopology(GridDhtCache<Integer, String> dht, int idx) {
        final Affinity<Integer> aff = dht.affinity();

        Ignite ignite = dht.context().grid();
        ClusterNode locNode = ignite.cluster().localNode();

        GridDhtPartitionTopology top = dht.topology();

        System.out.println("\nTopology of cache #" + idx + " (" + locNode.id() + ")" + ":");
        System.out.println("----------------------------------");

        List<Integer> affParts = new LinkedList<>();

        GridDhtPartitionMap map = dht.topology().partitions(locNode.id());

        if (map != null)
            for (int p : map.keySet())
                affParts.add(p);

        Collections.sort(affParts);

        System.out.println("Affinity partitions: " + affParts + "\n");

        List<GridDhtLocalPartition> locals = new ArrayList<GridDhtLocalPartition>(top.localPartitions());

        Collections.sort(locals);

        for (final GridDhtLocalPartition part : locals) {
            Collection<ClusterNode> partNodes = aff.mapKeyToPrimaryAndBackups(part.id());

            String ownStr = !partNodes.contains(dht.context().localNode()) ? "NOT AN OWNER" :
                F.eqNodes(CU.primary(partNodes), locNode) ? "PRIMARY" : "BACKUP";

            Collection<Integer> keys = F.viewReadOnly(dht.keySet(), F.<Integer>identity(), new P1<Integer>() {
                @Override public boolean apply(Integer k) {
                    return aff.partition(k) == part.id();
                }
            });

            System.out.println("Local partition: [" + part + "], [owning=" + ownStr + ", keyCnt=" + keys.size() +
                ", keys=" + keys + "]");
        }

        System.out.println("\nNode map:");

        for (Map.Entry<UUID, GridDhtPartitionMap> e : top.partitionMap(false).entrySet()) {
            List<Integer> list = new ArrayList<>(e.getValue().keySet());

            Collections.sort(list);

            System.out.println("[node=" + e.getKey() + ", parts=" + list + "]");
        }

        System.out.println("");
    }

    /**
     * Checks consistency of partitioned cache.
     * Any preload processes must be finished before this method call().
     *
     * @param dht Dht cache.
     * @param idx Cache index.
     * @param log Logger.
     */
    @SuppressWarnings("unchecked")
    static void checkDhtTopology(GridDhtCache<Integer, String> dht, int idx, IgniteLogger log) {
        assert dht != null;
        assert idx >= 0;
        assert log != null;

        log.info("Checking balanced state of cache #" + idx);

        Affinity<Object> aff = (Affinity)dht.affinity();

        Ignite ignite = dht.context().grid();
        ClusterNode locNode = ignite.cluster().localNode();

        GridDhtPartitionTopology top = dht.topology();

        // Expected partitions calculated with affinity function.
        // They should be in topology in OWNING state.
        Collection<Integer> affParts = new HashSet<>();

        GridDhtPartitionMap map = dht.topology().partitions(locNode.id());

        if (map != null)
            for (int p : map.keySet())
                affParts.add(p);

        if (F.isEmpty(affParts))
            return;

        for (int p : affParts)
            assert top.localPartition(p, false) != null :
                "Partition does not exist in topology: [cache=" + idx + ", part=" + p + "]";

        for (GridDhtLocalPartition p : top.localPartitions()) {
            assert affParts.contains(p.id()) :
                "Invalid local partition: [cache=" + idx + ", part=" + p + ", node partitions=" + affParts + "]";

            assert p.state() == OWNING : "Invalid partition state [cache=" + idx + ", part=" + p + "]";

            Collection<ClusterNode> partNodes = aff.mapPartitionToPrimaryAndBackups(p.id());

            assert partNodes.contains(locNode) :
                "Partition affinity nodes does not contain local node: [cache=" + idx + "]";
        }

        // Check keys.
        for (GridCacheEntryEx e : dht.entries()) {
            GridDhtCacheEntry entry = (GridDhtCacheEntry)e;

            if (!affParts.contains(entry.partition()))
                log.warning("Partition of stored entry is obsolete for node: [cache=" + idx + ", entry=" + entry +
                    ", node partitions=" + affParts + "]");

            int p = aff.partition(entry.key());

            if (!affParts.contains(p))
                log.warning("Calculated entry partition is not in node partitions: [cache=" + idx + ", part=" + p +
                    ", entry=" + entry + ", node partitions=" + affParts + "]");
        }
    }
}