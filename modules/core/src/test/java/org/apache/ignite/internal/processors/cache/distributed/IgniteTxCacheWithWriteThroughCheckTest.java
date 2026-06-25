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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.managers.eventstorage.HighPriorityListener;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.testframework.GridTestUtils.cartesianProduct;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/** */
@RunWith(Parameterized.class)
public class IgniteTxCacheWithWriteThroughCheckTest extends GridCommonAbstractTest {
    /** Node kill trigger. */
    private static CountDownLatch nodeKillLatch;

    /** Node left on backup. */
    private static CountDownLatch nodeLeftRegisteredOnBackup;

    /** */
    @Parameterized.Parameter(0)
    public Boolean withPersistence;

    /** */
    @Parameterized.Parameter(1)
    public TransactionConcurrency conc;

    /** */
    @Parameterized.Parameters(name = "withPersistence={0}, concMode={1}")
    public static Collection<Object[]> parameters() {
        return cartesianProduct(
            List.of(true, false),
            List.of(OPTIMISTIC, PESSIMISTIC)
        );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        if (withPersistence)
            cleanPersistenceDir();

        nodeKillLatch = new CountDownLatch(1);
        nodeLeftRegisteredOnBackup = new CountDownLatch(1);

        MapCacheStore.salvagedLatch = new CountDownLatch(1);
        MapCacheStore.txCoordStoreLatch = new CountDownLatch(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        try {
            for (Ignite node : G.allGrids()) {
                Collection<IgniteInternalTx> txs = ((IgniteEx)node).context().cache().context().tm().activeTransactions();

                assertTrue("Unfinished txs [node=" + node.name() + ", txs=" + txs + ']', txs.isEmpty());
            }
        }
        finally {
            stopAllGrids();

            super.afterTest();
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(withPersistence)))
            .setConsistentId(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi());
    }

    /** Test scenario:
     * <ul>
     *   <li>Start 3 nodes [node0, node1, node2].</li>
     *   <li>Initialize put operation into transactional cache where [node1] holds primary partition for such insertion.</li>
     *   <li>Kill [node1] right after tx PREPARE stage is completed (it triggers tx recovery procedure).</li>
     * </ul>
     *
     * @see IgniteTxManager#salvageTx(IgniteInternalTx)
     */
    @Test
    public void testTxCoordinatorLeftClusterWithEnabledReadWriteThrough() throws Exception {
        // Sequential start is important here.
        IgniteEx nodeCoord = startGrid(0);
        // Near node.
        IgniteEx nodePrimary = startGrid(1);
        // Backup node.
        IgniteEx nodeBackup = startGrid(2);

        int firstVal = 1;
        int secondVal = 2;

        nodeCoord.cluster().state(ClusterState.ACTIVE);

        CacheConfiguration<Object, Object> ccfgWithWriteThrough = configureCache(DEFAULT_CACHE_NAME);
        IgniteCache<Object, Object> cache = nodeCoord.createCache(ccfgWithWriteThrough);

        Integer primaryKey = primaryKey(nodePrimary.cache(DEFAULT_CACHE_NAME));

        try (Transaction tx = nodeCoord.transactions().txStart()) {
            cache.put(primaryKey, firstVal);

            tx.commit();
        }

        nodeCoord.cluster().state(ClusterState.INACTIVE);

        GridMessageListener lsnr = new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                if (msg instanceof GridNearTxPrepareResponse) {
                    IgniteTxManager txMgr = nodeBackup.context().cache().context().tm();
                    Collection<IgniteInternalTx> txs = txMgr.activeTransactions();

                    assertEquals(1, txs.size());
                    IgniteInternalTx idleTx = txs.iterator().next();
                    assertFalse(idleTx.local());

                    Map<GridCacheVersion, IgniteInternalTx> activeTx = GridTestUtils.getFieldValue(txMgr, "idMap");
                    assertEquals(1, activeTx.size());

                    nodeKillLatch.countDown();

                    U.awaitQuiet(nodeLeftRegisteredOnBackup);

                    // let`s wait until all discovery events have been processed on backup node.
                    doSleep(1000);

                    MapCacheStore.txCoordStoreLatch.countDown();
                }
            }
        };

        nodeCoord.context().io().removeMessageListener(GridTopic.TOPIC_CACHE); // Remove old cache listener.
        nodeCoord.context().io().addMessageListener(GridTopic.TOPIC_CACHE, lsnr); // Register as first listener.
        nodeCoord.context().cache().context().io().start0(); // Register cache listener again.

        nodeCoord.cluster().state(ClusterState.ACTIVE);
        awaitPartitionMapExchange(true, true, null);

        nodeCoord.context().event().addDiscoveryEventListener(new BeforeRecoveryListener(), EVT_NODE_FAILED, EVT_NODE_LEFT);
        nodeBackup.context().event().addDiscoveryEventListener(new BeforeBackupRecoveryListener(), EVT_NODE_FAILED, EVT_NODE_LEFT);

        IgniteInternalFuture<Object> stopFut = GridTestUtils.runAsync(() -> {
            nodeKillLatch.await();
            nodePrimary.close();
        });

        try (Transaction tx = nodeCoord.transactions().txStart(conc, READ_COMMITTED)) {
            cache.put(primaryKey, secondVal);

            tx.commit();
        }
        catch (Throwable th) {
            fail("Unexpected exception: " + th);
        }

        stopFut.get(getTestTimeout());

        awaitPartitionMapExchange();

        assertEquals(secondVal, nodeCoord.cache(DEFAULT_CACHE_NAME).get(primaryKey));
        assertEquals(secondVal, nodeBackup.cache(DEFAULT_CACHE_NAME).get(primaryKey));

        if (withPersistence) {
            for (Ignite ignite : G.allGrids())
                forceCheckpoint(ignite);

            // Check value after restart.
            stopAllGrids();
            startGridsMultiThreaded(3);

            awaitPartitionMapExchange(true, true, null);

            for (Ignite ignite : G.allGrids())
                assertEquals(secondVal, ignite.cache(ccfgWithWriteThrough.getName()).get(primaryKey));
        }
    }

    /** */
    private CacheConfiguration<Object, Object> configureCache(String cacheName) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(cacheName);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setCacheMode(CacheMode.REPLICATED);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        ccfg.setReadThrough(true);
        ccfg.setWriteThrough(true);
        ccfg.setCacheStoreFactory(MapCacheStore::new);

        return ccfg;
    }

    /** */
    public static class MapCacheStore extends CacheStoreAdapter<Object, Object> {
        /** Store map. */
        private static final Map<Object, Object> map = new ConcurrentHashMap<>();

        /** */
        static CountDownLatch salvagedLatch;

        /** */
        static CountDownLatch txCoordStoreLatch;

        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Object, Object> clo, Object... args) {
            for (Map.Entry<Object, Object> e : map.entrySet())
                clo.apply(e.getKey(), e.getValue());
        }

        /** {@inheritDoc} */
        @Override public Object load(Object key) {
            Object val = map.get(key);

            salvagedLatch.countDown();

            return val;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<?, ?> e) {
            map.put(e.getKey(), e.getValue());

            txCoordStoreLatch.countDown();
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            map.remove(key);
        }
    }

    /** */
    private static class BeforeRecoveryListener implements DiscoveryEventListener, HighPriorityListener {
        /** {@inheritDoc} */
        @Override public void onEvent(DiscoveryEvent evt, DiscoCache discoCache) {
            U.awaitQuiet(MapCacheStore.txCoordStoreLatch);
        }

        /** {@inheritDoc} */
        @Override public int order() {
            return -1;
        }
    }

    /** */
    private static class BeforeBackupRecoveryListener implements DiscoveryEventListener, HighPriorityListener {
        /** {@inheritDoc} */
        @Override public void onEvent(DiscoveryEvent evt, DiscoCache discoCache) {
            nodeLeftRegisteredOnBackup.countDown();
        }

        /** {@inheritDoc} */
        @Override public int order() {
            return -1;
        }
    }
}
