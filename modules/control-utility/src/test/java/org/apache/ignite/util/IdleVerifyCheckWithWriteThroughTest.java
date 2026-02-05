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

package org.apache.ignite.util;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.cache.store.CacheStoreSession;
import org.apache.ignite.cache.store.CacheStoreSessionListener;
import org.apache.ignite.cache.store.jdbc.CacheJdbcStoreSessionListener;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.failure.AbstractFailureHandler;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.managers.eventstorage.HighPriorityListener;
import org.apache.ignite.internal.processors.cache.MapCacheStoreStrategy;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;
import org.junit.runners.Parameterized;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/** */
public class IdleVerifyCheckWithWriteThroughTest extends GridCommandHandlerClusterPerMethodAbstractTest {
    /** Node kill trigger. */
    private static CountDownLatch nodeKillLatch;

    /** */
    @Parameterized.Parameter(1)
    public Boolean withPersistence;

    /** */
    @Parameterized.Parameters(name = "cmdHnd={0}, withPersistence={1}")
    public static Collection<Object[]> parameters() {
        return List.of(
            new Object[] {CLI_CMD_HND, false},
            new Object[] {CLI_CMD_HND, true}
        );
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        persistenceEnable(withPersistence);

        if (withPersistence)
            cleanPersistenceDir();

        nodeKillLatch = new CountDownLatch(1);
        MapCacheStore.salvagedLatch = new CountDownLatch(1);
        MapCacheStore.txCoordStoreLatch = new CountDownLatch(1);
    }

    /** {@inheritDoc} */
    @Override protected boolean persistenceEnable() {
        return withPersistence;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi());
    }

    /** */
    @Test
    public void testTxCoordinatorLeftClusterWithEnabledReadWriteThrough() throws Exception {
        // sequential start is important here
        IgniteEx nodeCoord = startGrid(0);
        IgniteEx nodePrimary = startGrid(1);
        startGrid(2);

        nodeCoord.cluster().state(ClusterState.ACTIVE);

        GridMessageListener lsnr = new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                if (msg instanceof GridNearTxPrepareResponse) {
                    nodeKillLatch.countDown();
                    U.awaitQuiet(MapCacheStore.salvagedLatch);
                }
            }
        };

        nodeCoord.context().io().removeMessageListener(GridTopic.TOPIC_CACHE); // Remove old cache listener.
        nodeCoord.context().io().addMessageListener(GridTopic.TOPIC_CACHE, lsnr); // Register as first listener.
        nodeCoord.context().cache().context().io().start0(); // Register cache listener again.

        nodeCoord.context().event().addDiscoveryEventListener(new BeforeRecoveryListener(), EVT_NODE_FAILED, EVT_NODE_LEFT);

        IgniteInternalFuture<Object> stopFut = GridTestUtils.runAsync(() -> {
            nodeKillLatch.await();
            nodePrimary.close();
        });

        injectTestSystemOut();

        CacheConfiguration<Integer, Object> ccfgWithWriteThrough = createCache(DEFAULT_CACHE_NAME, true);
        IgniteCache<Integer, Object> cache = nodeCoord.createCache(ccfgWithWriteThrough);

        CacheConfiguration<Integer, Object> ccfgWithoutWriteThrough = createCache("noWriteThrough", false);
        IgniteCache<Integer, Object> cacheWithoutWriteThrough = nodeCoord.createCache(ccfgWithoutWriteThrough);

        awaitPartitionMapExchange();

        Integer primaryKey = primaryKey(nodePrimary.cache(DEFAULT_CACHE_NAME));
        Integer primaryKeyWithoutWriteThrough = primaryKey(nodePrimary.cache("noWriteThrough"));

        try (Transaction tx = nodeCoord.transactions().txStart(OPTIMISTIC, READ_COMMITTED)) {
            cache.put(primaryKey, new Object());
            cacheWithoutWriteThrough.put(primaryKeyWithoutWriteThrough, new Object());
            tx.commit();
        }
        catch (Throwable th) {
            fail("Unexpected exception: " + th);
        }

        stopFut.get(getTestTimeout());

        doSleep(1_000L);

        awaitPartitionMapExchange();

        assertEquals(EXIT_CODE_OK, execute("--port", connectorPort(grid(2)), "--cache", "idle_verify"));

        String out = testOut.toString();

        assertContains(log, out, "The check procedure has failed");
        // Update counters are equal but size is different
        if (withPersistence) {
            assertContains(log, out, "updateCntr=[lwm=1, missed=[], hwm=1], partitionState=OWNING, size=0");
            assertContains(log, out, "updateCntr=[lwm=1, missed=[], hwm=1], partitionState=OWNING, size=1");
        }
        else {
            assertContains(log, out, "updateCntr=1, partitionState=OWNING, size=0");
            assertContains(log, out, "updateCntr=1, partitionState=OWNING, size=1");
        }
        testOut.reset();

        if (withPersistence) {
            stopAllGrids();
            startGridsMultiThreaded(3);

            awaitPartitionMapExchange(true, true, null);

            assertEquals(EXIT_CODE_OK, execute("--port", connectorPort(grid(2)), "--cache", "idle_verify"));
            out = testOut.toString();
            // partVerHash are different, thus only regex check here
            Pattern primaryPattern = Pattern.compile("Partition instances: " +
                "\\[PartitionHashRecord" +
                ".*?hwm=1\\], partitionState=OWNING, size=1" +
                ".*?hwm=1\\], partitionState=OWNING, size=1" +
                ".*?hwm=1\\], partitionState=OWNING, size=1");

            boolean matches = primaryPattern.matcher(out).find();
            System.err.println("!!!! " + out);
            assertTrue(matches);
        }
    }

    private CacheConfiguration<Integer, Object> createCache(String cacheName, boolean writeThrough) {
        CacheConfiguration<Integer, Object> ccfg = new CacheConfiguration<>(cacheName);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setCacheMode(CacheMode.REPLICATED);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        if (writeThrough) {
            ccfg.setReadThrough(true);
            ccfg.setWriteThrough(true);
            ccfg.setCacheStoreFactory(MapCacheStore::new);
        }

        return ccfg;
    }

    /** {@link CacheStore} backed by {@link #map} */
    public static class MapCacheStore extends CacheStoreAdapter<Object, Object> {
        /** Store map. */
        private static final Map<Object, Object> map = new ConcurrentHashMap<>();

        /** */
        private static CountDownLatch salvagedLatch;

        /** */
        private static CountDownLatch txCoordStoreLatch;

        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Object, Object> clo, Object... args) {
            for (Map.Entry<Object, Object> e : map.entrySet())
                clo.apply(e.getKey(), e.getValue());
        }

        /** {@inheritDoc} */
        @Override public Object load(Object key) {
            Object val = map.get(key);

            if (salvagedLatch != null)
                salvagedLatch.countDown();

            return val;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<?, ?> e) {
            map.put(e.getKey(), e.getValue());

            if (txCoordStoreLatch != null)
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
}
