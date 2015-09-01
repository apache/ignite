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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CachePeekMode.ONHEAP;

/**
 *
 */
public abstract class CacheNearUpdateTopologyChangeAbstractTest extends IgniteCacheAbstractTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return new NearCacheConfiguration();
    }

    /**
     * @throws Exception If failed.
     */
    public void testNearUpdateTopologyChange() throws Exception {
        final Affinity<Integer> aff = grid(0).affinity(null);

        final Integer key = 9;

        IgniteCache<Integer, Integer> primaryCache = primaryCache(key, null);

        final Ignite primaryIgnite = primaryCache.unwrap(Ignite.class);

        log.info("Primary node: " + primaryIgnite.name());

        primaryCache.put(key, 1);

        IgniteCache<Integer, Integer> nearCache = nearCache(key);

        log.info("Near node: " + nearCache.unwrap(Ignite.class).name());

        assertEquals((Object)1, nearCache.get(key));

        assertEquals((Object)1, nearCache.localPeek(key, ONHEAP));

        boolean gotNewPrimary = false;

        List<Ignite> newNodes = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            int idx = gridCount() + i;

            log.info("Start new node: " + i);

            Ignite ignite = startGrid(idx);

            awaitPartitionMapExchange();

            newNodes.add(ignite);

            ClusterNode primaryNode = aff.mapKeyToNode(key);

            Ignite primary = grid(primaryNode);

            log.info("Primary node on new topology: " + primary.name());

            if (!primaryNode.equals(primaryIgnite.cluster().localNode())) {
                log.info("Update from new primary: " + primary.name());

                primary = grid(primaryNode);

                gotNewPrimary = true;

                primary.cache(null).put(key, 2);

                break;
            }
            else
                assertEquals((Object)1, nearCache.localPeek(key, ONHEAP));
        }

        assertTrue(gotNewPrimary);

        for (Ignite ignite : newNodes) {
            log.info("Stop started node: " + ignite.name());

            ignite.close();
        }

        awaitPartitionMapExchange();

        boolean wait = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return aff.isPrimary(primaryIgnite.cluster().localNode(), key);
            }
        }, 10_000);

        assertTrue(wait);

        log.info("Primary node: " + primaryNode(key, null).name());

        assertTrue(aff.isPrimary(primaryIgnite.cluster().localNode(), key));

        assertFalse(aff.isPrimaryOrBackup(nearCache.unwrap(Ignite.class).cluster().localNode(), key));

        assertEquals((Object)2, nearCache.get(key));

        assertEquals((Object)2, nearCache.localPeek(key, ONHEAP));
    }
}