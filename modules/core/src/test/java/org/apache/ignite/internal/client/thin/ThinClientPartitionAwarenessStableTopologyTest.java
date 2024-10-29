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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.client.ClientAtomicConfiguration;
import org.apache.ignite.client.ClientAtomicLong;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCollectionConfiguration;
import org.apache.ignite.client.ClientIgniteSet;
import org.apache.ignite.client.ClientPartitionAwarenessMapper;
import org.apache.ignite.client.ClientPartitionAwarenessMapperFactory;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.datastructures.GridCacheAtomicLongEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.Arrays.asList;

/**
 * Test partition awareness of thin client on stable topology.
 */
public class ThinClientPartitionAwarenessStableTopologyTest extends ThinClientAbstractPartitionAwarenessTest {
    /** Grids count. */
    private static final int GRIDS_CNT = 3;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(GRIDS_CNT);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        // Add one extra node address to the list, skip 0 node.
        initClient(getClientConfiguration(1, 2, 3), 1, 2);
    }

    /** {@inheritDoc} */
    @Override protected ClientConfiguration getClientConfiguration(int... nodeIdxs) {
        ClientConfiguration cfg = super.getClientConfiguration(nodeIdxs);

        // To cover more cases, we need undiscovered nodes for this test, so disable endpoints discovery
        // by setting addresses finder.
        return cfg.setAddressesFinder(cfg::getAddresses);
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
     * Test that partition awareness is applicable for partitioned cache with custom affinity function
     * and key to partition mapping function is set on the client side.
     */
    @Test
    public void testPartitionedCustomAffinityCacheWithMapper() throws Exception {
        client.close();

        Arrays.fill(channels, null);

        initClient(getClientConfiguration(1, 2, 3)
            .setPartitionAwarenessMapperFactory(new ClientPartitionAwarenessMapperFactory() {
                /** {@inheritDoc} */
                @Override public ClientPartitionAwarenessMapper create(String cacheName, int partitions) {
                    assertEquals(cacheName, PART_CUSTOM_AFFINITY_CACHE_NAME);

                    AffinityFunction aff = new RendezvousAffinityFunction(false, partitions);

                    return aff::partition;
                }
            }), 1, 2);

        testApplicableCache(PART_CUSTOM_AFFINITY_CACHE_NAME, i -> i);
    }

    /**
     * Test partition awareness for all applicable operation types for partitioned cache with primitive key.
     */
    @Test
    public void testPartitionedCachePrimitiveKey() throws Exception {
        testApplicableCache(PART_CACHE_NAME, i -> i);
    }

    /**
     * Test partition awareness for all applicable operation types for partitioned cache with complex key.
     */
    @Test
    public void testPartitionedCacheComplexKey() throws Exception {
        testApplicableCache(PART_CACHE_NAME, i -> new TestComplexKey(i, i));
    }

    /**
     * Test partition awareness for all applicable operation types for partitioned cache with annotated affinity
     * mapped key.
     */
    @Test
    public void testPartitionedCacheAnnotatedAffinityKey() throws Exception {
        testApplicableCache(PART_CACHE_NAME, i -> new TestAnnotatedAffinityKey(i, i));
    }

    /**
     * Test partition awareness for all applicable operation types for partitioned cache with not annotated affinity
     * mapped key.
     */
    @Test
    public void testPartitionedCacheNotAnnotatedAffinityKey() throws Exception {
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

        assertOpOnChannel(null, ClientOperation.CACHE_PARTITIONS);
        assertOpOnChannel(null, ClientOperation.CACHE_PUT);
    }

    /**
     * Test affinity awareness for all applicable operation types for partitioned cache with 0 backups.
     */
    @Test
    public void testPartitionedCache0Backups() throws Exception {
        testApplicableCache(PART_CACHE_0_BACKUPS_NAME, i -> i);
    }

    /**
     * Test affinity awareness for all applicable operation types for partitioned cache with 1 backups.
     */
    @Test
    public void testPartitionedCache1Backups() throws Exception {
        testApplicableCache(PART_CACHE_1_BACKUPS_NAME, i -> i);
    }

    /**
     * Test partition awareness for cache with a node filter.
     */
    @Test
    public void testPartitionedWithNodeFilter() throws Exception {
        doTestCachesWithEqualAffinities(Arrays.asList(PART_CACHE_1_BACKUPS_NF_NAME), null);
    }

    /**
     * Test partition awareness for caches with the same cache group, the same and equal node filters.
     * Tests that equal partitions mappings are grouped in single ClientCachePartitionAwarenessGroup.
     * Expects only one {@link ClientOperation#CACHE_PARTITIONS}.
     */
    @Test
    public void testCacheGroupWithNodeFilter() throws Exception {
        CacheConfiguration<?, ?> c1 = new CacheConfiguration<>(grid(0).cachex(PART_CACHE_1_BACKUPS_NF_NAME).configuration())
            .setName("groupWithNodeFilter1")
            .setGroupName("filteredGrp");

        CacheConfiguration<?, ?> c2 = new CacheConfiguration<>(c1).setName("groupWithNodeFilter2");

        // Cache with node filter equal to the filter of cache PART_CACHE_1_BACKUPS_NF_NAME.
        CacheConfiguration<?, ?> c3 = new CacheConfiguration<>(c1)
            .setName("cacheWithEqualNodeFilter")
            .setGroupName(null)
            .setNodeFilter(new ConsistentIdNodeFilter1());

        doTestCachesWithEqualAffinities(Collections.singletonList(PART_CACHE_1_BACKUPS_NF_NAME), Arrays.asList(c1, c2, c3));
    }

    /**
     * Test partition awareness for caches with equal affinity.
     * Tests equal partitions mappings are grouped in single ClientCachePartitionAwarenessGroup.
     */
    @Test
    public void testPartitionedWithNodeFilter1() throws Exception {
        doTestCachesWithEqualAffinities(Arrays.asList(PART_CACHE_NAME, PART_CACHE_1_BACKUPS_NAME, PART_CACHE_3_BACKUPS_NAME), null);
    }

    /**
     * Test affinity awareness for the caches {@code cacheToUse} and {@code cachesToCreate}. Requires equal affinities.
     * Expects strictly single {@link ClientOperation#CACHE_PARTITIONS} is invoked once for all the caches.
     *
     * @param cacheToUse Existing caches to test.
     * @param cachesToCreate Caches to create for the test. Are removed after.
     */
    private void doTestCachesWithEqualAffinities(
        @Nullable Collection<String> cacheToUse,
        @Nullable Collection<CacheConfiguration<?, ?>> cachesToCreate
    ) throws Exception {
        assert !F.isEmpty(cacheToUse) || !F.isEmpty(cachesToCreate);

        ArrayList<CacheConfiguration<?, ?>> cachesToTest = new ArrayList<>();

        if (!F.isEmpty(cachesToCreate)) {
            for (CacheConfiguration<?, ?> ccfg : cachesToCreate)
                grid(0).createCache(ccfg);

            cachesToTest.addAll(cachesToCreate);

            awaitPartitionMapExchange();
        }

        try {
            reinitClientToAllServers();

            if (!F.isEmpty(cacheToUse)) {
                for (String cacheName : cacheToUse)
                    cachesToTest.add(grid(0).context().cache().cacheConfiguration(cacheName));
            }

            AtomicBoolean firstCall = new AtomicBoolean(true);

            for (CacheConfiguration<?, ?> ccfg : cachesToTest) {
                log.info("Testing cache '" + ccfg.getName() + "'...");

                testApplicableCache(ccfg.getName(), i -> i, firstCall.getAndSet(false));
            }
        }
        finally {
            if (!F.isEmpty(cachesToCreate)) {
                for (CacheConfiguration<?, ?> ccfg : cachesToCreate)
                    grid(0).destroyCache(ccfg.getName());

                awaitPartitionMapExchange();
            }
        }
    }

    /**
     * Tests grouping of caches affinities mappings in ClientCachePartitionAwarenessGroup.
     */
    @Test
    public void testGroupingOfVariousAffinities() throws Exception {
        reinitClientToAllServers();

        client.cache(PART_CACHE_1_BACKUPS_NF_NAME).get(0);

        assertOpOnChannel(null, ClientOperation.CACHE_PARTITIONS);
        assertOpOnChannel(null, ClientOperation.CACHE_GET);

        // New operation with differend affinity.
        client.cache(PART_CACHE_NAME).get(0);

        assertOpOnChannel(null, ClientOperation.CACHE_PARTITIONS);
        assertOpOnChannel(null, ClientOperation.CACHE_GET);

        // New operation with the same affinity.
        client.cache(PART_CACHE_3_BACKUPS_NAME).get(0);

        assertOpOnChannel(null, ClientOperation.CACHE_GET);

        // New operation with the same affinity.
        client.cache(PART_CACHE_1_BACKUPS_NAME).get(0);

        assertOpOnChannel(null, ClientOperation.CACHE_GET);

        // New operation with differend affinity.
        client.cache(PART_CUSTOM_AFFINITY_CACHE_NAME).get(0);

        assertOpOnChannel(null, ClientOperation.CACHE_PARTITIONS);
        assertOpOnChannel(null, ClientOperation.CACHE_GET);
    }

    /**
     * Tests that equal affinities mappings with node filters are included in single ClientCachePartitionAwarenessGroup
     * and no additional {@link ClientOperation#CACHE_PARTITIONS} request is required.
     */
    @Test
    public void testGroupingOfEqualAffinities() throws Exception {
        // Cache with the same node filter as of cache PART_CACHE_1_BACKUPS_NF_NAME.
        CacheConfiguration<?, ?> ccfg1 = new CacheConfiguration<>(grid(0).cachex(PART_CACHE_1_BACKUPS_NF_NAME).configuration())
            .setName("cacheWithTheSameNodeFilter");

        // Cache with node filter equal to the filter of cache PART_CACHE_1_BACKUPS_NF_NAME.
        CacheConfiguration<?, ?> ccfg2 = new CacheConfiguration<>(ccfg1)
            .setName("cacheWithEqualNodeFilter")
            .setNodeFilter(new ConsistentIdNodeFilter1());

        // Cache with different node filter.
        CacheConfiguration<?, ?> ccfg3 = new CacheConfiguration<>(ccfg1)
            .setName("cacheWithOtherNodeFilter")
            .setNodeFilter(new ConsistentIdNodeFilter2());

        try {
            ignite(0).createCache(ccfg1);
            ignite(0).createCache(ccfg2);
            ignite(0).createCache(ccfg3);

            awaitPartitionMapExchange();

            reinitClientToAllServers();

            client.cache(PART_CACHE_1_BACKUPS_NF_NAME).get(0);

            assertOpOnChannel(null, ClientOperation.CACHE_PARTITIONS);
            assertOpOnChannel(null, ClientOperation.CACHE_GET);

            // The same affinity mapping.
            client.cache(ccfg1.getName()).get(0);

            assertOpOnChannel(null, ClientOperation.CACHE_GET);

            // Other affinity mapping but equal to the previous.
            client.cache(ccfg2.getName()).get(0);

            assertOpOnChannel(null, ClientOperation.CACHE_GET);

            // Different node filter, Different affinity.
            client.cache(ccfg3.getName()).get(0);

            assertOpOnChannel(null, ClientOperation.CACHE_PARTITIONS);
            assertOpOnChannel(null, ClientOperation.CACHE_GET);
        }
        finally {
            ignite(0).destroyCache(ccfg1.getName());
            ignite(0).destroyCache(ccfg2.getName());
            ignite(0).destroyCache(ccfg3.getName());

            awaitPartitionMapExchange();
        }
    }

    /**
     * Test affinity awareness for all applicable operation types for partitioned cache with 3 backups.
     */
    @Test
    public void testPartitionedCache3Backups() throws Exception {
        testApplicableCache(PART_CACHE_3_BACKUPS_NAME, i -> i);
    }

    /**
     * Test scan query.
     */
    @Test
    public void testScanQuery() throws IgniteCheckedException {
        ClientCache<Object, Object> clientCache = client.cache(PART_CACHE_NAME);

        // Make any operation to request partitions.
        clientCache.get(0);

        opsQueue.clear(); // Clear partitions request and get operation.

        for (int i = 0; i < GRIDS_CNT; i++) {
            int part = grid(i).affinity(PART_CACHE_NAME).primaryPartitions(grid(i).localNode())[0];

            TestTcpClientChannel ch = nodeChannel(grid(i).localNode().id());

            // Test scan query with specified partition.
            clientCache.query(new ScanQuery<>().setPartition(part)).getAll();

            // Client doesn't have connection with grid(0), ch will be null for this grid
            // and operation on any channel is acceptable.
            assertOpOnChannel(ch, ClientOperation.QUERY_SCAN);
        }
    }

    /**
     * Tests {@link ClientIgniteSet} partition awareness.
     * Other client set tests are in {@link IgniteSetTest}.
     */
    @Test
    public void testIgniteSet() {
        testIgniteSet("testIgniteSet", null, CacheAtomicityMode.ATOMIC);
        testIgniteSet("testIgniteSet2", null, CacheAtomicityMode.TRANSACTIONAL);
        testIgniteSet("testIgniteSet3", "grp-testIgniteSet3", CacheAtomicityMode.ATOMIC);
        testIgniteSet("testIgniteSet4", "grp-testIgniteSet4", CacheAtomicityMode.TRANSACTIONAL);
    }

    /** */
    @Test
    public void testMultipleCacheGroupAffinityMappingRequest() throws Exception {
        ClientCacheAffinityContext affCtx = ((TcpIgniteClient)client).reliableChannel().affinityContext();

        IgniteInternalFuture<Object> replCacheOpFut;
        IgniteInternalFuture<Object> partCacheOpFut;

        synchronized (affCtx.cacheKeyMapperFactoryMap) {
            partCacheOpFut = GridTestUtils.runAsync(() -> client.cache(PART_CACHE_NAME).get(0));
            replCacheOpFut = GridTestUtils.runAsync(() -> client.cache(REPL_CACHE_NAME).get(0));

            GridTestUtils.waitForCondition(
                () -> affCtx.pendingCacheIds.containsAll(F.transform(asList(REPL_CACHE_NAME, PART_CACHE_NAME), CU::cacheId)),
                getTestTimeout()
            );
        }

        partCacheOpFut.get();
        replCacheOpFut.get();

        Map<ClientOperation, Integer> ops = opsQueue.stream().map(T2::get2).collect(Collectors.toMap(v -> v, v -> 1, Integer::sum));

        assertEquals(2, (int)ops.get(ClientOperation.CACHE_GET));
        assertEquals(1, (int)ops.get(ClientOperation.CACHE_PARTITIONS));
    }

    /**
     * Tests {@link ClientIgniteSet} partition awareness.
     */
    private void testIgniteSet(String name, String groupName, CacheAtomicityMode mode) {
        ClientCollectionConfiguration cfg = new ClientCollectionConfiguration()
                .setGroupName(groupName)
                .setAtomicityMode(mode)
                .setBackups(1);

        ClientIgniteSet<String> clientSet = client.set(name, cfg);

        if (groupName == null)
            groupName = "default-ds-group";

        String cacheName = "datastructures_" + mode + "_PARTITIONED_1@" + groupName + "#SET_" + clientSet.name();
        IgniteInternalCache<Object, Object> cache = grid(0).context().cache().cache(cacheName);

        // Warm up.
        clientSet.add("a");
        opsQueue.clear();

        // Test.
        for (int i = 0; i < 10; i++) {
            String key = "b" + i;
            clientSet.add(key);

            TestTcpClientChannel opCh = affinityChannel(key, cache);
            assertOpOnChannel(opCh, ClientOperation.OP_SET_VALUE_ADD);
        }
    }

    /**
     * Tests {@link ClientIgniteSet} partition awareness in colocated mode.
     */
    @Test
    public void testIgniteSetCollocated() {
        testIgniteSetCollocated("testIgniteSetCollocated", null, CacheAtomicityMode.ATOMIC);
        testIgniteSetCollocated("testIgniteSetCollocated2", null, CacheAtomicityMode.TRANSACTIONAL);
        testIgniteSetCollocated("testIgniteSetCollocated3", "grp-testIgniteSetCollocated3", CacheAtomicityMode.ATOMIC);
        testIgniteSetCollocated("testIgniteSetCollocated4", "grp-testIgniteSetCollocated4",
                CacheAtomicityMode.TRANSACTIONAL);
    }

    /**
     * Tests {@link ClientIgniteSet} partition awareness in colocated mode.
     */
    public void testIgniteSetCollocated(String name, String groupName, CacheAtomicityMode mode) {
        ClientCollectionConfiguration cfg = new ClientCollectionConfiguration()
                .setColocated(true)
                .setGroupName(groupName)
                .setAtomicityMode(mode)
                .setBackups(1);

        ClientIgniteSet<String> clientSet = client.set(name, cfg);

        if (groupName == null)
            groupName = "default-ds-group";

        String cacheName = "datastructures_" + mode + "_PARTITIONED_1@" + groupName;
        IgniteInternalCache<Object, Object> cache = grid(0).context().cache().cache(cacheName);

        // Warm up.
        clientSet.add("a");
        opsQueue.clear();

        // Test.
        for (int i = 0; i < 10; i++) {
            String key = "b" + i;
            clientSet.add(key);

            TestTcpClientChannel opCh = affinityChannel(clientSet.name().hashCode(), cache);
            assertOpOnChannel(opCh, ClientOperation.OP_SET_VALUE_ADD);
        }

        // Test iterator.
        clientSet.toArray();

        TestTcpClientChannel opCh = affinityChannel(clientSet.name().hashCode(), cache);
        assertOpOnChannel(opCh, ClientOperation.OP_SET_ITERATOR_START);
    }

    /**
     * Test atomic long.
     */
    @Test
    public void testAtomicLong() {
        testAtomicLong("default-grp-partitioned", null, CacheMode.PARTITIONED);
        testAtomicLong("default-grp-replicated", null, CacheMode.REPLICATED);
        testAtomicLong("custom-grp-partitioned", "testAtomicLong", CacheMode.PARTITIONED);
        testAtomicLong("custom-grp-replicated", "testAtomicLong", CacheMode.REPLICATED);
    }

    /**
     * Test atomic long.
     */
    private void testAtomicLong(String name, String grpName, CacheMode cacheMode) {
        ClientAtomicConfiguration cfg = new ClientAtomicConfiguration()
                .setGroupName(grpName)
                .setCacheMode(cacheMode);

        ClientAtomicLong clientAtomicLong = client.atomicLong(name, cfg, 1, true);
        GridCacheAtomicLongEx serverAtomicLong = (GridCacheAtomicLongEx)grid(0).atomicLong(
                name, new AtomicConfiguration().setGroupName(grpName), 0, false);

        String cacheName = "ignite-sys-atomic-cache@" + (grpName == null ? "default-ds-group" : grpName);
        IgniteInternalCache<Object, Object> cache = grid(0).context().cache().cache(cacheName);

        // Warm up.
        clientAtomicLong.get();
        opsQueue.clear();

        // Test.
        clientAtomicLong.get();
        TestTcpClientChannel opCh = affinityChannel(serverAtomicLong.key(), cache);

        assertOpOnChannel(opCh, ClientOperation.ATOMIC_LONG_VALUE_GET);
    }

    /**
     * @param cacheName Cache name.
     */
    private void testNotApplicableCache(String cacheName) {
        ClientCache<Object, Object> cache = client.cache(cacheName);

        // After first response we should send partitions request on default channel together with next request.
        cache.put(0, 0);

        assertOpOnChannel(null, ClientOperation.CACHE_PARTITIONS);
        assertOpOnChannel(null, ClientOperation.CACHE_PUT);

        for (int i = 1; i < KEY_CNT; i++) {
            cache.put(i, i);

            assertOpOnChannel(null, ClientOperation.CACHE_PUT);

            cache.get(i);

            assertOpOnChannel(null, ClientOperation.CACHE_GET);
        }
    }

    /**
     * @param cacheName Cache name.
     * @param keyFactory Key factory function.
     */
    private void testApplicableCache(String cacheName, Function<Integer, Object> keyFactory) throws Exception {
        testApplicableCache(cacheName, keyFactory, true);
    }

    /**
     * @param cacheName Cache name.
     * @param keyFactory Key factory function.
     * @param partitionsRequestExpected If {@code true}, awaits {@link ClientOperation#CACHE_PARTITIONS} first.
     */
    private void testApplicableCache(
        String cacheName,
        Function<Integer, Object> keyFactory,
        boolean partitionsRequestExpected
    ) throws Exception {
        ClientCache<Object, Object> clientCache = client.cache(cacheName);
        IgniteInternalCache<Object, Object> igniteCache = grid(0).context().cache().cache(cacheName);

        clientCache.put(keyFactory.apply(0), 0);

        TestTcpClientChannel opCh = affinityChannel(keyFactory.apply(0), igniteCache);

        if (partitionsRequestExpected)
            assertOpOnChannel(null, ClientOperation.CACHE_PARTITIONS);

        assertOpOnChannel(opCh, ClientOperation.CACHE_PUT);

        for (int i = 1; i < KEY_CNT; i++) {
            Object key = keyFactory.apply(i);

            opCh = affinityChannel(key, igniteCache);

            clientCache.put(key, key);
            assertOpOnChannel(opCh, ClientOperation.CACHE_PUT);

            clientCache.putAsync(key, key).get();
            assertOpOnChannel(opCh, ClientOperation.CACHE_PUT);

            clientCache.get(key);
            assertOpOnChannel(opCh, ClientOperation.CACHE_GET);

            clientCache.getAsync(key).get();
            assertOpOnChannel(opCh, ClientOperation.CACHE_GET);

            clientCache.containsKey(key);
            assertOpOnChannel(opCh, ClientOperation.CACHE_CONTAINS_KEY);

            clientCache.containsKeyAsync(key).get();
            assertOpOnChannel(opCh, ClientOperation.CACHE_CONTAINS_KEY);

            clientCache.replace(key, i);
            assertOpOnChannel(opCh, ClientOperation.CACHE_REPLACE);

            clientCache.replaceAsync(key, i).get();
            assertOpOnChannel(opCh, ClientOperation.CACHE_REPLACE);

            clientCache.replace(key, i, i);
            assertOpOnChannel(opCh, ClientOperation.CACHE_REPLACE_IF_EQUALS);

            clientCache.replaceAsync(key, i, i).get();
            assertOpOnChannel(opCh, ClientOperation.CACHE_REPLACE_IF_EQUALS);

            clientCache.remove(key);
            assertOpOnChannel(opCh, ClientOperation.CACHE_REMOVE_KEY);

            clientCache.removeAsync(key).get();
            assertOpOnChannel(opCh, ClientOperation.CACHE_REMOVE_KEY);

            clientCache.remove(key, i);
            assertOpOnChannel(opCh, ClientOperation.CACHE_REMOVE_IF_EQUALS);

            clientCache.removeAsync(key, i).get();
            assertOpOnChannel(opCh, ClientOperation.CACHE_REMOVE_IF_EQUALS);

            clientCache.getAndPut(key, i);
            assertOpOnChannel(opCh, ClientOperation.CACHE_GET_AND_PUT);

            clientCache.getAndPutAsync(key, i).get();
            assertOpOnChannel(opCh, ClientOperation.CACHE_GET_AND_PUT);

            clientCache.getAndRemove(key);
            assertOpOnChannel(opCh, ClientOperation.CACHE_GET_AND_REMOVE);

            clientCache.getAndRemoveAsync(key).get();
            assertOpOnChannel(opCh, ClientOperation.CACHE_GET_AND_REMOVE);

            clientCache.getAndReplace(key, i);
            assertOpOnChannel(opCh, ClientOperation.CACHE_GET_AND_REPLACE);

            clientCache.getAndReplaceAsync(key, i).get();
            assertOpOnChannel(opCh, ClientOperation.CACHE_GET_AND_REPLACE);

            clientCache.putIfAbsent(key, i);
            assertOpOnChannel(opCh, ClientOperation.CACHE_PUT_IF_ABSENT);

            clientCache.putIfAbsentAsync(key, i).get();
            assertOpOnChannel(opCh, ClientOperation.CACHE_PUT_IF_ABSENT);

            clientCache.getAndPutIfAbsent(key, i);
            assertOpOnChannel(opCh, ClientOperation.CACHE_GET_AND_PUT_IF_ABSENT);

            clientCache.getAndPutIfAbsentAsync(key, i).get();
            assertOpOnChannel(opCh, ClientOperation.CACHE_GET_AND_PUT_IF_ABSENT);

            clientCache.clear(key);
            assertOpOnChannel(opCh, ClientOperation.CACHE_CLEAR_KEY);

            clientCache.clearAsync(key);
            assertOpOnChannel(opCh, ClientOperation.CACHE_CLEAR_KEY);
        }
    }

    /** */
    private void reinitClientToAllServers() throws Exception {
        client.close();

        Arrays.fill(channels, null);

        initClient(getClientConfiguration(0, 1, 2), 0, 1, 2);

        awaitChannelsInit(0, 1, 2);
    }

    /**
     * Excludes node if its consistent id ends with 'Test1'. The same as {@link ConsistentIdNodeFilter}. We do not
     * use one parametrized filter because we validate node filters equality for a cache group only by their
     * classes.
     */
    protected static final class ConsistentIdNodeFilter1 implements IgnitePredicate<ClusterNode> {
        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return !node.consistentId().toString().endsWith("Test1");
        }
    }

    /**
     * Excludes node if its consistent id ends with 'Test2'. Similar to {@link ConsistentIdNodeFilter} and
     * {@link ConsistentIdNodeFilter1}. We do not use one parametrized filter because we validate node filters
     * equality for a cache group only by their classes.
     */
    protected static final class ConsistentIdNodeFilter2 implements IgnitePredicate<ClusterNode> {
        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return !node.consistentId().toString().endsWith("Test2");
        }
    }
}
