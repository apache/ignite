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

package org.apache.ignite.internal.processors.cache.checker.tasks;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.checker.objects.RecheckRequest;
import org.apache.ignite.internal.processors.cache.checker.objects.VersionedValue;
import org.apache.ignite.internal.processors.diagnostic.ReconciliationExecutionContext;
import org.junit.Test;

/**
 * Tests that collects actual value by keys from nodes for recheck.
 */
public class CollectPartitionKeysByRecheckRequestTaskTest extends CollectPartitionInfoAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
            .setBackups(2)
            .setAffinity(new RendezvousAffinityFunction(1, null))
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Checks that all keys returned.
     */
    @Test
    public void testShouldReturnAllRequiredKeys() throws Exception {
        IgniteEx node = startGrids(2);

        node.cluster().active(true);

        IgniteInternalCache<Object, Object> cache = node.cachex(DEFAULT_CACHE_NAME);
        cache.put(1, 1);
        cache.put(2, 2);

        CacheObjectContext ctx = cache.context().cacheObjectContext();

        Collection<ClusterNode> nodes = node.affinity(DEFAULT_CACHE_NAME)
            .mapPartitionToPrimaryAndBackups(FIRST_PARTITION);

        List<KeyCacheObject> recheckKeys = new ArrayList<>();
        recheckKeys.add(key(1, ctx));
        recheckKeys.add(key(2, ctx));

        Map<KeyCacheObject, Map<UUID, VersionedValue>> res = node.compute(group(node, nodes)).execute(
            CollectPartitionKeysByRecheckRequestTask.class,
            new RecheckRequest(ReconciliationExecutionContext.IGNORE_JOB_PERMITS_SESSION_ID, UUID.randomUUID(),
                recheckKeys, DEFAULT_CACHE_NAME, FIRST_PARTITION, lastTopologyVersion(node))
        ).getResult();

        assertEquals(2, res.size());
        assertTrue(res.keySet().containsAll(recheckKeys));
    }

    /**
     * Checks that empty result returned.
     */
    @Test
    public void testEmptyKeysSelectEmptyResult() throws Exception {
        IgniteEx node = startGrids(2);

        node.cluster().active(true);

        Collection<ClusterNode> nodes = node.affinity(DEFAULT_CACHE_NAME)
            .mapPartitionToPrimaryAndBackups(FIRST_PARTITION);

        List<KeyCacheObject> recheckKeys = new ArrayList<>();

        Map<KeyCacheObject, Map<UUID, VersionedValue>> res = node.compute(group(node, nodes)).execute(
            CollectPartitionKeysByRecheckRequestTask.class,
            new RecheckRequest(ReconciliationExecutionContext.IGNORE_JOB_PERMITS_SESSION_ID, UUID.randomUUID(),
                recheckKeys, DEFAULT_CACHE_NAME, FIRST_PARTITION, lastTopologyVersion(node))
        ).getResult();

        assertTrue(res.isEmpty());
    }

    /**
     * If a key was removed, it should return empty result.
     */
    @Test
    public void testRemovedKeyShouldReturnEmptyResult() throws Exception {
        IgniteEx node = startGrids(2);

        node.cluster().active(true);

        Collection<ClusterNode> nodes = node.affinity(DEFAULT_CACHE_NAME)
            .mapPartitionToPrimaryAndBackups(FIRST_PARTITION);

        List<KeyCacheObject> recheckKeys = new ArrayList<>();

        String keyRow = "test-key";
        KeyCacheObjectImpl key = key(keyRow, node.cachex(DEFAULT_CACHE_NAME).context().cacheObjectContext());

        recheckKeys.add(key);

        Map<KeyCacheObject, Map<UUID, VersionedValue>> res = node.compute(group(node, nodes)).execute(
            CollectPartitionKeysByRecheckRequestTask.class,
            new RecheckRequest(ReconciliationExecutionContext.IGNORE_JOB_PERMITS_SESSION_ID, UUID.randomUUID(),
                recheckKeys, DEFAULT_CACHE_NAME, FIRST_PARTITION, lastTopologyVersion(node))
        ).getResult();

        assertTrue(res.isEmpty());
    }
}
