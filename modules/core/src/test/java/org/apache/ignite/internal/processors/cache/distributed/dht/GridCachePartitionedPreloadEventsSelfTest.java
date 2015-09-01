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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.processors.cache.distributed.GridCachePreloadEventsAbstractSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtForceKeysFuture;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_OBJECT_LOADED;

/**
 *
 */
public class GridCachePartitionedPreloadEventsSelfTest extends GridCachePreloadEventsAbstractSelfTest {
    /** */
    private boolean replicatedAffinity = true;

    /** */
    private long rebalanceDelay;

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration cacheCfg = super.cacheConfiguration();

        if (replicatedAffinity)
            // replicate entries to all nodes
            cacheCfg.setAffinity(notSerializableProxy(new AffinityFunction() {
                /** {@inheritDoc} */
                @Override public void reset() {
                }

                /** {@inheritDoc} */
                @Override public int partitions() {
                    return 1;
                }

                /** {@inheritDoc} */
                @Override public int partition(Object key) {
                    return 0;
                }

                /** {@inheritDoc} */
                @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
                    List<ClusterNode> nodes = new ArrayList<>(affCtx.currentTopologySnapshot());

                    return Collections.singletonList(nodes);
                }

                /** {@inheritDoc} */
                @Override public void removeNode(UUID nodeId) {
                }
            }, AffinityFunction.class));

        cacheCfg.setRebalanceDelay(rebalanceDelay);

        return cacheCfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode getCacheMode() {
        return PARTITIONED;
    }

    /**
     * Test events fired from
     * {@link GridDhtForceKeysFuture}
     *
     * @throws Exception if failed.
     */
    public void testForcePreload() throws Exception {
        replicatedAffinity = false;
        rebalanceDelay = -1;

        Ignite g1 = startGrid("g1");

        Collection<Integer> keys = new HashSet<>();

        IgniteCache<Integer, String> cache = g1.cache(null);

        for (int i = 0; i < 100; i++) {
            keys.add(i);
            cache.put(i, "val");
        }

        Ignite g2 = startGrid("g2");

        Map<ClusterNode, Collection<Object>> keysMap = g1.affinity(null).mapKeysToNodes(keys);
        Collection<Object> g2Keys = keysMap.get(g2.cluster().localNode());

        assertNotNull(g2Keys);
        assertFalse("There are no keys assigned to g2", g2Keys.isEmpty());

        for (Object key : g2Keys)
            // Need to force keys loading.
            assertEquals("val", g2.cache(null).getAndPut(key, "changed val"));

        Collection<Event> evts = g2.events().localQuery(F.<Event>alwaysTrue(), EVT_CACHE_REBALANCE_OBJECT_LOADED);

        checkPreloadEvents(evts, g2, g2Keys);
    }
}