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

package org.apache.ignite.internal.client.thin;

import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.junit.Test;

/**
 * Test partition awareness of thin client on stable topology.
 */
public class ThinClientPartitionAwarenessStableTopologyTest extends ThinClientAbstractPartitionAwarenessTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(3);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        // Add one extra node address to the list, skip 0 node.
        initClient(getClientConfiguration(1, 2, 3), 1, 2);
    }

    /**
     * Test that partition awareness is not applicable for replicated cache.
     */
    @Test
    public void testReplicatedCache() {
        testNotApplicableCache(REPL_CACHE_NAME);
    }

    /**
     * Test that partition awareness is not applicable for partitioned cache with custom affinity function.
     */
    @Test
    public void testPartitionedCustomAffinityCache() {
        testNotApplicableCache(PART_CUSTOM_AFFINITY_CACHE_NAME);
    }

    /**
     * Test partition awareness for all applicable operation types for partitioned cache with primitive key.
     */
    @Test
    public void testPartitionedCachePrimitiveKey() {
        testApplicableCache(PART_CACHE_NAME, i -> i);
    }

    /**
     * Test partition awareness for all applicable operation types for partitioned cache with complex key.
     */
    @Test
    public void testPartitionedCacheComplexKey() {
        testApplicableCache(PART_CACHE_NAME, i -> new TestComplexKey(i, i));
    }

    /**
     * Test partition awareness for all applicable operation types for partitioned cache with annotated affinity
     * mapped key.
     */
    @Test
    public void testPartitionedCacheAnnotatedAffinityKey() {
        testApplicableCache(PART_CACHE_NAME, i -> new TestAnnotatedAffinityKey(i, i));
    }

    /**
     * Test partition awareness for all applicable operation types for partitioned cache with not annotated affinity
     * mapped key.
     */
    @Test
    public void testPartitionedCacheNotAnnotatedAffinityKey() {
        testApplicableCache(PART_CACHE_NAME, i -> new TestNotAnnotatedAffinityKey(new TestComplexKey(i, i), i));
    }

    /**
     * Test request to partition mapped to unknown for client node.
     */
    @Test
    public void testPartitionedCacheUnknownNode() throws IgniteCheckedException {
        ClientCache<Object, Object> clientCache = client.cache(PART_CACHE_NAME);

        // We don't included grid(0) address to list of addresses known for the client, so client don't have connection
        // with grid(0)
        Integer keyForUnknownNode = primaryKey(grid(0).cache(PART_CACHE_NAME));

        assertNotNull("Not found key for node " + grid(0).localNode().id(), keyForUnknownNode);

        clientCache.put(keyForUnknownNode, 0);

        assertOpOnChannel(dfltCh, ClientOperation.CACHE_PARTITIONS);
        assertOpOnChannel(dfltCh, ClientOperation.CACHE_PUT);
    }

    /**
     * @param cacheName Cache name.
     */
    private void testNotApplicableCache(String cacheName) {
        ClientCache<Object, Object> cache = client.cache(cacheName);

        // After first response we should send partitions request on default channel together with next request.
        cache.put(0, 0);

        assertOpOnChannel(dfltCh, ClientOperation.CACHE_PARTITIONS);
        assertOpOnChannel(dfltCh, ClientOperation.CACHE_PUT);

        for (int i = 1; i < KEY_CNT; i++) {
            cache.put(i, i);

            assertOpOnChannel(dfltCh, ClientOperation.CACHE_PUT);

            cache.get(i);

            assertOpOnChannel(dfltCh, ClientOperation.CACHE_GET);
        }
    }

    /**
     * @param cacheName Cache name.
     * @param keyFactory Key factory function.
     */
    private void testApplicableCache(String cacheName, Function<Integer, Object> keyFactory) {
        ClientCache<Object, Object> clientCache = client.cache(cacheName);
        IgniteInternalCache<Object, Object> igniteCache = grid(0).context().cache().cache(cacheName);

        clientCache.put(keyFactory.apply(0), 0);

        TestTcpClientChannel opCh = affinityChannel(keyFactory.apply(0), igniteCache);

        // Default channel is the first who detects topology change, so next partition request will go through
        // the default channel.
        assertOpOnChannel(dfltCh, ClientOperation.CACHE_PARTITIONS);
        assertOpOnChannel(opCh, ClientOperation.CACHE_PUT);

        for (int i = 1; i < KEY_CNT; i++) {
            Object key = keyFactory.apply(i);

            opCh = affinityChannel(key, igniteCache);

            clientCache.put(key, key);

            assertOpOnChannel(opCh, ClientOperation.CACHE_PUT);

            clientCache.get(key);

            assertOpOnChannel(opCh, ClientOperation.CACHE_GET);

            clientCache.containsKey(key);

            assertOpOnChannel(opCh, ClientOperation.CACHE_CONTAINS_KEY);

            clientCache.replace(key, i);

            assertOpOnChannel(opCh, ClientOperation.CACHE_REPLACE);

            clientCache.replace(key, i, i);

            assertOpOnChannel(opCh, ClientOperation.CACHE_REPLACE_IF_EQUALS);

            clientCache.remove(key);

            assertOpOnChannel(opCh, ClientOperation.CACHE_REMOVE_KEY);

            clientCache.remove(key, i);

            assertOpOnChannel(opCh, ClientOperation.CACHE_REMOVE_IF_EQUALS);

            clientCache.getAndPut(key, i);

            assertOpOnChannel(opCh, ClientOperation.CACHE_GET_AND_PUT);

            clientCache.getAndRemove(key);

            assertOpOnChannel(opCh, ClientOperation.CACHE_GET_AND_REMOVE);

            clientCache.getAndReplace(key, i);

            assertOpOnChannel(opCh, ClientOperation.CACHE_GET_AND_REPLACE);

            clientCache.putIfAbsent(key, i);

            assertOpOnChannel(opCh, ClientOperation.CACHE_PUT_IF_ABSENT);
        }
    }
}
