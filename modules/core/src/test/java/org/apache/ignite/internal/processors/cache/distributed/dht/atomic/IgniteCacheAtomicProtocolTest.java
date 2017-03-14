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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessageV2;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;

/**
 *
 */
public class IgniteCacheAtomicProtocolTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String TEST_CACHE = "testCache";

    /** */
    private boolean client;

    /** */
    private CacheConfiguration ccfg;

    /** */
    private boolean blockRebalance;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);
        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setMaxMissedClientHeartbeats(1000);

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

        cfg.setCommunicationSpi(commSpi);

        cfg.setClientMode(client);

        if (ccfg != null)
            cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     *
     */
    private void blockRebalance() {
        for (Ignite node : G.allGrids()) {
            testSpi(node).blockMessages(new IgnitePredicate<GridIoMessage>() {
                @Override public boolean apply(GridIoMessage msg) {
                    Object msg0 = msg.message();

                    return (msg0 instanceof GridDhtPartitionSupplyMessage || msg0 instanceof GridDhtPartitionSupplyMessageV2)
                        && ((GridCacheMessage)msg0).cacheId() == CU.cacheId(TEST_CACHE);
                }
            });
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllPrimaryFailure1() throws Exception {
        putAllPrimaryFailure(true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllPrimaryFailure1_UnstableTopology() throws Exception {
        blockRebalance = true;

        putAllPrimaryFailure(true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllPrimaryFailure2() throws Exception {
        putAllPrimaryFailure(true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllPrimaryFailure2_UnstableTopology() throws Exception {
        blockRebalance = true;

        putAllPrimaryFailure(true, true);
    }

    /**
     * @param fail0 Fail node 0 flag.
     * @param fail1 Fail node 1 flag.
     * @throws Exception If failed.
     */
    private void putAllPrimaryFailure(boolean fail0, boolean fail1) throws Exception {
        ccfg = cacheConfiguration(1, FULL_SYNC);

        startServers(4);

        client = true;

        Ignite client = startGrid(4);

        IgniteCache<Integer, Integer> nearCache = client.cache(TEST_CACHE);
        IgniteCache<Integer, Integer> nearAsyncCache = nearCache.withAsync();

        if (!blockRebalance)
            awaitPartitionMapExchange();

        Ignite srv0 = ignite(0);
        Ignite srv1 = ignite(1);

        Integer key1 = primaryKey(srv0.cache(TEST_CACHE));
        Integer key2 = primaryKey(srv1.cache(TEST_CACHE));

        Map<Integer, Integer> map = new HashMap<>();
        map.put(key1, key1);
        map.put(key2, key2);

        assertEquals(2, map.size());

        if (fail0) {
            testSpi(client).blockMessages(GridNearAtomicFullUpdateRequest.class, srv0.name());
            testSpi(client).blockMessages(GridNearAtomicCheckUpdateRequest.class, srv0.name());
        }

        if (fail1) {
            testSpi(client).blockMessages(GridNearAtomicFullUpdateRequest.class, srv1.name());
            testSpi(client).blockMessages(GridNearAtomicCheckUpdateRequest.class, srv1.name());
        }

        log.info("Start put [key1=" + key1 + ", key2=" + key2 + ']');

        nearAsyncCache.putAll(map);

        IgniteFuture<?> fut = nearAsyncCache.future();

        U.sleep(500);

        assertFalse(fut.isDone());

        if (fail0)
            stopGrid(0);

        if (fail1)
            stopGrid(1);

        fut.get();

        checkData(map);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllBackupFailure1() throws Exception {
        putAllBackupFailure1();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllBackupFailure1_UnstableTopology() throws Exception {
        blockRebalance = true;

        putAllBackupFailure1();
    }

    /**
     * @throws Exception If failed.
     */
    private void putAllBackupFailure1() throws Exception {
        ccfg = cacheConfiguration(1, FULL_SYNC);

        startServers(4);

        client = true;

        Ignite client = startGrid(4);

        IgniteCache<Integer, Integer> nearCache = client.cache(TEST_CACHE);
        IgniteCache<Integer, Integer> nearAsyncCache = nearCache.withAsync();

        if (!blockRebalance)
            awaitPartitionMapExchange();

        Ignite srv0 = ignite(0);

        List<Integer> keys = primaryKeys(srv0.cache(TEST_CACHE), 3);

        Ignite backup = backup(client.affinity(TEST_CACHE), keys.get(0));

        testSpi(backup).blockMessages(GridDhtAtomicNearResponse.class, client.name());

        Map<Integer, Integer> map = new HashMap<>();

        for (Integer key : keys)
            map.put(key, key);

        log.info("Start put [map=" + map + ']');

        nearAsyncCache.putAll(map);

        IgniteFuture<?> fut = nearAsyncCache.future();

        U.sleep(500);

        assertFalse(fut.isDone());

        stopGrid(backup.name());

        fut.get();

        checkData(map);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutBackupFailure1() throws Exception {
        putBackupFailure1();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutBackupFailure1_UnstableTopology() throws Exception {
        blockRebalance = true;

        putBackupFailure1();
    }

    /**
     * @throws Exception If failed.
     */
    private void putBackupFailure1() throws Exception {
        ccfg = cacheConfiguration(1, FULL_SYNC);

        startServers(4);

        client = true;

        Ignite client = startGrid(4);

        IgniteCache<Integer, Integer> nearCache = client.cache(TEST_CACHE);
        IgniteCache<Integer, Integer> nearAsyncCache = nearCache.withAsync();

        if (!blockRebalance)
            awaitPartitionMapExchange();

        Ignite srv0 = ignite(0);

        Integer key = primaryKey(srv0.cache(TEST_CACHE));

        Ignite backup = backup(client.affinity(TEST_CACHE), key);

        testSpi(backup).blockMessages(GridDhtAtomicNearResponse.class, client.name());

        log.info("Start put [key=" + key + ']');

        nearAsyncCache.put(key, key);

        IgniteFuture<?> fut = nearAsyncCache.future();

        U.sleep(500);

        assertFalse(fut.isDone());

        stopGrid(backup.name());

        fut.get();

        checkData(F.asMap(key, key));
    }

    /**
     * @throws Exception If failed.
     */
    public void testFullAsyncPutRemap() throws Exception {
        fullAsyncRemap(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testFullAsyncPutAllRemap() throws Exception {
        fullAsyncRemap(true);
    }

    /**
     * @param putAll Test putAll flag.
     * @throws Exception If failed.
     */
    private void fullAsyncRemap(boolean putAll) throws Exception {
        Ignite srv0 = startGrid(0);

        client = true;

        Ignite clientNode = startGrid(1);

        client = false;

        final IgniteCache<Integer, Integer> nearCache = clientNode.createCache(cacheConfiguration(1, FULL_ASYNC));

        List<Integer> keys = primaryKeys(srv0.cache(TEST_CACHE), putAll ? 3 : 1);

        testSpi(clientNode).blockMessages(GridNearAtomicSingleUpdateRequest.class, srv0.name());
        testSpi(clientNode).blockMessages(GridNearAtomicFullUpdateRequest.class, srv0.name());

        final Map<Integer, Integer> map = new HashMap<>();

        for (Integer key : keys)
            map.put(key, -key);

        if (putAll)
            nearCache.putAll(map);
        else
            nearCache.put(keys.get(0), map.get(keys.get(0)));

        int nodeIdx = 2;

        Affinity<Object> aff = clientNode.affinity(TEST_CACHE);

        int keysMoved;

        do {
            startGrid(nodeIdx);

            awaitPartitionMapExchange();

            keysMoved = 0;

            for (Integer key : keys) {
                if (!aff.isPrimary(srv0.cluster().localNode(), key))
                    keysMoved++;
            }

            if (keysMoved == keys.size())
                break;

            nodeIdx++;
        }
        while (nodeIdx < 10);

        assertEquals(keys.size(), keysMoved);

        testSpi(clientNode).stopBlock(true);

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (Integer key : map.keySet()) {
                    if (nearCache.get(key) == null)
                        return false;
                }

                return true;
            }
        }, 5000);

        checkData(map);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutPrimarySync() throws Exception {
        startGrids(2);

        client = true;

        Ignite clientNode = startGrid(2);

        client = false;

        final IgniteCache<Integer, Integer> nearCache = clientNode.createCache(cacheConfiguration(1, PRIMARY_SYNC));
        IgniteCache<Integer, Integer> nearAsyncCache = nearCache.withAsync();

        awaitPartitionMapExchange();

        Ignite srv0 = grid(0);
        final Ignite srv1 = grid(1);

        final Integer key = primaryKey(srv0.cache(TEST_CACHE));

        testSpi(srv0).blockMessages(GridDhtAtomicSingleUpdateRequest.class, srv1.name());

        nearAsyncCache.put(key, key);

        IgniteFuture<?> fut = nearAsyncCache.future();

        fut.get(5, TimeUnit.SECONDS);

        assertEquals(key, srv0.cache(TEST_CACHE).get(key));

        assertNull(srv1.cache(TEST_CACHE).localPeek(key));

        testSpi(srv0).stopBlock(true);

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return srv1.cache(TEST_CACHE).localPeek(key) != null;
            }
        }, 5000);

        checkData(F.asMap(key, key));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutNearNodeFailure() throws Exception {
        startGrids(2);

        client = true;

        Ignite clientNode = startGrid(2);

        final IgniteCache<Integer, Integer> nearCache = clientNode.createCache(cacheConfiguration(1, FULL_SYNC));
        IgniteCache<Integer, Integer> nearAsyncCache = nearCache.withAsync();

        awaitPartitionMapExchange();

        final Ignite srv0 = grid(0);
        final Ignite srv1 = grid(1);

        final Integer key = primaryKey(srv0.cache(TEST_CACHE));

        nearAsyncCache.put(key, key);

        testSpi(srv1).blockMessages(GridDhtAtomicNearResponse.class, clientNode.name());

        stopGrid(2);

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return ((IgniteKernal)srv0).context().cache().context().mvcc().atomicFuturesCount() == 0;
            }
        }, 5000);

        assertEquals(0, ((IgniteKernal)srv0).context().cache().context().mvcc().atomicFuturesCount());
        assertEquals(0, ((IgniteKernal)srv1).context().cache().context().mvcc().atomicFuturesCount());

        checkData(F.asMap(key, key));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllNearNodeFailure() throws Exception {
        final int SRVS = 4;

        startGrids(SRVS);

        client = true;

        Ignite clientNode = startGrid(SRVS);

        final IgniteCache<Integer, Integer> nearCache = clientNode.createCache(cacheConfiguration(1, FULL_SYNC));
        IgniteCache<Integer, Integer> nearAsyncCache = nearCache.withAsync();

        awaitPartitionMapExchange();

        for (int i = 0; i < SRVS; i++)
            testSpi(grid(i)).blockMessages(GridDhtAtomicNearResponse.class, clientNode.name());

        final Map<Integer, Integer> map = new HashMap<>();

        for (int i = 0; i < 100; i++)
            map.put(i, i);

        nearAsyncCache.putAll(map);

        boolean wait = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                IgniteCache cache = ignite(0).cache(TEST_CACHE);

                for (Integer key : map.keySet()) {
                    if (cache.get(key) == null)
                        return false;
                }

                return true;
            }
        }, 5000);

        assertTrue(wait);

        stopGrid(SRVS);

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                for (int i = 0; i < SRVS; i++) {
                    if (grid(i).context().cache().context().mvcc().atomicFuturesCount() != 0)
                        return false;
                }

                return true;
            }
        }, 5000);

        for (int i = 0; i < SRVS; i++)
            assertEquals(0, grid(i).context().cache().context().mvcc().atomicFuturesCount());

        checkData(map);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheOperations0() throws Exception {
        cacheOperations(0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheOperations_UnstableTopology0() throws Exception {
        blockRebalance = true;

        cacheOperations(0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheOperations1() throws Exception {
        cacheOperations(1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheOperations_UnstableTopology1() throws Exception {
        blockRebalance = true;

        cacheOperations(1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheOperations2() throws Exception {
        cacheOperations(2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheOperations_UnstableTopology2() throws Exception {
        blockRebalance = true;

        cacheOperations(2);
    }

    /**
     * @param backups Number of backups.
     * @throws Exception If failed.
     */
    private void cacheOperations(int backups) throws Exception {
        ccfg = cacheConfiguration(backups, FULL_SYNC);

        final int SRVS = 4;

        startServers(SRVS);

        client = true;

        Ignite clientNode = startGrid(SRVS);

        final IgniteCache<Integer, Integer> nearCache = clientNode.cache(TEST_CACHE);

        Integer key = primaryKey(ignite(0).cache(TEST_CACHE));

        nearCache.replace(key, 1);

        nearCache.remove(key);

        nearCache.invoke(key, new SetValueEntryProcessor(null));

        Map<Integer, SetValueEntryProcessor> map = new HashMap<>();

        List<Integer> keys = primaryKeys(ignite(0).cache(TEST_CACHE), 2);

        map.put(keys.get(0), new SetValueEntryProcessor(1));
        map.put(keys.get(1), new SetValueEntryProcessor(null));

        nearCache.invokeAll(map);

        Set<Integer> rmvAllKeys = new HashSet<>();

        for (int i = 0; i < 100; i++) {
            nearCache.put(i, i);

            if (i % 2 == 0)
                rmvAllKeys.add(i);
        }

        nearCache.removeAll(rmvAllKeys);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutMissedDhtRequest_UnstableTopology() throws Exception {
        blockRebalance = true;

        ccfg = cacheConfiguration(1, FULL_SYNC);

        startServers(4);

        client = true;

        Ignite client = startGrid(4);

        IgniteCache<Integer, Integer> nearCache = client.cache(TEST_CACHE);
        IgniteCache<Integer, Integer> nearAsyncCache = nearCache.withAsync();

        testSpi(ignite(0)).blockMessages(new IgnitePredicate<GridIoMessage>() {
            @Override public boolean apply(GridIoMessage msg) {
                return msg.message() instanceof GridDhtAtomicAbstractUpdateRequest;
            }
        });

        Integer key = primaryKey(ignite(0).cache(TEST_CACHE));

        log.info("Start put [key=" + key + ']');

        nearAsyncCache.put(key, key);
        IgniteFuture<?> fut = nearAsyncCache.future();

        U.sleep(500);

        assertFalse(fut.isDone());

        stopGrid(0);

        fut.get();

        checkData(F.asMap(key, key));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllMissedDhtRequest_UnstableTopology1() throws Exception {
        putAllMissedDhtRequest_UnstableTopology(true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllMissedDhtRequest_UnstableTopology2() throws Exception {
        putAllMissedDhtRequest_UnstableTopology(true, true);
    }

    /**
     * @param fail0 Fail node 0 flag.
     * @param fail1 Fail node 1 flag.
     * @throws Exception If failed.
     */
    private void putAllMissedDhtRequest_UnstableTopology(boolean fail0, boolean fail1) throws Exception {
        blockRebalance = true;

        ccfg = cacheConfiguration(1, FULL_SYNC);

        startServers(4);

        client = true;

        Ignite client = startGrid(4);

        IgniteCache<Integer, Integer> nearCache = client.cache(TEST_CACHE);
        IgniteCache<Integer, Integer> nearAsyncCache = nearCache.withAsync();

        if (fail0) {
            testSpi(ignite(0)).blockMessages(new IgnitePredicate<GridIoMessage>() {
                @Override public boolean apply(GridIoMessage msg) {
                    return msg.message() instanceof GridDhtAtomicAbstractUpdateRequest;
                }
            });
        }
        if (fail1) {
            testSpi(ignite(2)).blockMessages(new IgnitePredicate<GridIoMessage>() {
                @Override public boolean apply(GridIoMessage msg) {
                    return msg.message() instanceof GridDhtAtomicAbstractUpdateRequest;
                }
            });
        }

        Integer key1 = primaryKey(ignite(0).cache(TEST_CACHE));
        Integer key2 = primaryKey(ignite(2).cache(TEST_CACHE));

        log.info("Start put [key1=" + key1 + ", key2=" + key1 + ']');

        Map<Integer, Integer> map = new HashMap<>();
        map.put(key1, 10);
        map.put(key2, 20);

        nearAsyncCache.putAll(map);
        IgniteFuture<?> fut = nearAsyncCache.future();

        U.sleep(500);

        assertFalse(fut.isDone());

        if (fail0)
            stopGrid(0);
        if (fail1)
            stopGrid(2);

        fut.get();

        checkData(map);
    }

    /**
     * @param expData Expected cache data.
     */
    private void checkData(Map<Integer, Integer> expData) {
        assert !expData.isEmpty();

        List<Ignite> nodes = G.allGrids();

        assertFalse(nodes.isEmpty());

        for (Ignite node : nodes) {
            IgniteCache<Integer, Integer> cache = node.cache(TEST_CACHE);

            for (Map.Entry<Integer, Integer> e : expData.entrySet()) {
                assertEquals("Invalid value [key=" + e.getKey() + ", node=" + node.name() + ']',
                    e.getValue(),
                    cache.get(e.getKey()));
            }
        }
    }

    /**
     * @param aff Affinity.
     * @param key Key.
     * @return Backup node for given key.
     */
    private Ignite backup(Affinity<Object> aff, Object key) {
        for (Ignite ignite : G.allGrids()) {
            ClusterNode node = ignite.cluster().localNode();

            if (aff.isPrimaryOrBackup(node, key) && !aff.isPrimary(node, key))
                return ignite;
        }

        fail("Failed to find backup for key: " + key);

        return null;
    }

    /**
     * @param node Node.
     * @return Node communication SPI.
     */
    private TestRecordingCommunicationSpi testSpi(Ignite node) {
        return (TestRecordingCommunicationSpi)node.configuration().getCommunicationSpi();
    }

    /**
     * @param backups Number of backups.
     * @param writeSync Cache write synchronization mode.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Integer> cacheConfiguration(int backups,
        CacheWriteSynchronizationMode writeSync) {
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>();

        ccfg.setName(TEST_CACHE);
        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setWriteSynchronizationMode(writeSync);
        ccfg.setBackups(backups);
        ccfg.setRebalanceMode(ASYNC);

        return ccfg;
    }

    /**
     * @param cnt Number of server nodes.
     * @throws Exception If failed.
     */
    private void startServers(int cnt) throws Exception {
        startGrids(cnt - 1);

        awaitPartitionMapExchange();

        if (blockRebalance)
            blockRebalance();

        startGrid(cnt - 1);
    }

    /**
     *
     */
    public static class SetValueEntryProcessor implements CacheEntryProcessor<Integer, Integer, Object> {
        /** */
        private Integer val;

        /**
         * @param val Value.
         */
        SetValueEntryProcessor(Integer val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Integer, Integer> entry, Object... args) {
            if (val != null)
                entry.setValue(val);

            return null;
        }
    }
}
