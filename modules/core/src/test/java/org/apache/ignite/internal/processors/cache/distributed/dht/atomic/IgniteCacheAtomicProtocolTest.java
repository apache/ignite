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
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.GridCacheGroupIdMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.plugin.extensions.communication.Message;
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
        cfg.setClientFailureDetectionTimeout(Integer.MAX_VALUE);

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
        final int grpId = groupIdForCache(ignite(0), TEST_CACHE);

        for (Ignite node : G.allGrids()) {
            testSpi(node).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                @Override public boolean apply(ClusterNode node, Message msg) {
                    return (msg instanceof GridDhtPartitionSupplyMessage)
                        && ((GridCacheGroupIdMessage)msg).groupId() == grpId;
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
    public void testPutReaderUpdate1() throws Exception {
        readerUpdateDhtFails(false, false, false);

        stopAllGrids();

        readerUpdateDhtFails(false, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutReaderUpdate2() throws Exception {
        readerUpdateDhtFails(true, false, false);

        stopAllGrids();

        readerUpdateDhtFails(true, true, false);
    }

    /**
     * @param updateNearEnabled {@code True} if enable near cache for second put.
     * @param delayReader If {@code true} delay reader response, otherwise delay backup response.
     * @param putAll If {@code true} use putAll, otherwise put.
     * @throws Exception If failed.
     */
    private void readerUpdateDhtFails(boolean updateNearEnabled,
        boolean delayReader,
        boolean putAll) throws Exception {
        ccfg = cacheConfiguration(1, FULL_SYNC);

        client = false;

        startServers(2);

        Ignite srv0 = ignite(0);
        Ignite srv1 = ignite(1);

        IgniteCache<Object, Object> cache = srv0.cache(TEST_CACHE);

        // Waiting for minor topology changing because of late affinity assignment.
        awaitPartitionMapExchange();

        List<Integer> keys = primaryKeys(cache, putAll ? 3 : 1);

        ccfg = null;

        client = true;

        Ignite client1 = startGrid(2);

        IgniteCache<Object, Object> cache1 = client1.createNearCache(TEST_CACHE, new NearCacheConfiguration<>());

        Ignite client2 = startGrid(3);

        IgniteCache<Object, Object> cache2 = updateNearEnabled ?
            client2.createNearCache(TEST_CACHE, new NearCacheConfiguration<>()) : client2.cache(TEST_CACHE);

        if (putAll) {
            Map<Integer, Integer> map = new HashMap<>();

            for (Integer key : keys)
                map.put(key, 1);

            cache1.putAll(map);
        }
        else
            cache1.put(keys.get(0), 1);

        if (delayReader)
            testSpi(client1).blockMessages(GridDhtAtomicNearResponse.class, client2.name());
        else
            testSpi(srv1).blockMessages(GridDhtAtomicNearResponse.class, client2.name());

        Map<Integer, Integer> map;

        IgniteFuture<?> fut;

        if (putAll) {
            map = new HashMap<>();

            for (Integer key : keys)
                map.put(key, 1);

            fut = cache2.putAllAsync(map);
        }
        else {
            map = F.asMap(keys.get(0), 2);

            fut = cache2.putAsync(keys.get(0), 2);
        }

        U.sleep(2000);

        assertFalse(fut.isDone());

        if (delayReader)
            testSpi(client1).stopBlock();
        else
            testSpi(srv1).stopBlock();

        fut.get();

        checkData(map);
    }

    /**
     * @param expData Expected cache data.
     */
    private void checkData(Map<Integer, Integer> expData) {
        checkCacheData(expData, TEST_CACHE);
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
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

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
