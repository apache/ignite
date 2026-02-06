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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.eventstorage.DiscoveryEventListener;
import org.apache.ignite.internal.managers.eventstorage.HighPriorityListener;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.util.typedef.G;
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

/** */
public class IdleVerifyCheckWithWriteThroughTest extends GridCommandHandlerClusterPerMethodAbstractTest {
    /** Node kill trigger. */
    private static CountDownLatch nodeKillLatch;

    /** */
    @Parameterized.Parameter(1)
    public Boolean withPersistence;

    /** */
    @Parameterized.Parameter(2)
    public Boolean multiCache;

    /** */
    private static final String CORRECT_VERIFY_MSG = "The check procedure has finished, no conflicts have been found.";

    /** */
    private static final String WITHOUT_WRITE_THROUGH_CACHE = "withoutWriteThrough";

    /** */
    @Parameterized.Parameters(name = "cmdHnd={0}, withPersistence={1}, multiCache={2}")
    public static Collection<Object[]> parameters() {
        return List.of(
            new Object[] {CLI_CMD_HND, false, false},
            new Object[] {CLI_CMD_HND, false, true},
            new Object[] {CLI_CMD_HND, true, false},
            new Object[] {CLI_CMD_HND, true, true}
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
        MapCacheStore.txCoordStoreLatch = new CountDownLatch(2);
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
    public void testTxCoordinatorLeftClusterWithEnabledReadWriteThrough0() throws Exception {
        // sequential start is important here
        IgniteEx nodeCoord = startGrid(0);
        // near node
        IgniteEx nodePrimary = startGrid(1);
        // backup node
        startGrid(2);

        nodeCoord.cluster().state(ClusterState.ACTIVE);

        CacheConfiguration<Integer, Object> ccfgWithWriteThrough = createCache(DEFAULT_CACHE_NAME, true);
        IgniteCache<Integer, Object> cache = nodeCoord.createCache(ccfgWithWriteThrough);

        IgniteCache<Integer, Object> cacheWithoutWriteThrough = null;

        if (multiCache) {
            CacheConfiguration<Integer, Object> ccfgWithoutWriteThrough = createCache(WITHOUT_WRITE_THROUGH_CACHE, false);
            cacheWithoutWriteThrough = nodeCoord.createCache(ccfgWithoutWriteThrough);
        }

        Integer primaryKey = primaryKey(nodePrimary.cache(DEFAULT_CACHE_NAME));

        try (Transaction tx = nodeCoord.transactions().txStart(OPTIMISTIC, READ_COMMITTED)) {
            cache.put(primaryKey, 0);

            if (multiCache) {
                Integer primaryKeyWithoutWriteThrough = primaryKey(nodePrimary.cache(WITHOUT_WRITE_THROUGH_CACHE));
                cacheWithoutWriteThrough.put(primaryKeyWithoutWriteThrough, 0);
            }

            tx.commit();
        }

        nodeCoord.cluster().state(ClusterState.INACTIVE);

        GridMessageListener lsnr = new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                if (msg instanceof GridNearTxPrepareResponse) {
                    nodeKillLatch.countDown();
                    try {
                        U.await(MapCacheStore.salvagedLatch, 2_000, TimeUnit.MILLISECONDS);
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        throw new RuntimeException(e);
                    }
                    long cnt = MapCacheStore.salvagedLatch.getCount();
                    // check latch not raised
                    //assertEquals(1L, cnt);
                }
            }
        };

        nodeCoord.context().io().removeMessageListener(GridTopic.TOPIC_CACHE); // Remove old cache listener.
        nodeCoord.context().io().addMessageListener(GridTopic.TOPIC_CACHE, lsnr); // Register as first listener.
        nodeCoord.context().cache().context().io().start0(); // Register cache listener again.

        nodeCoord.cluster().state(ClusterState.ACTIVE);

        nodeCoord.context().event().addDiscoveryEventListener(new BeforeRecoveryListener(), EVT_NODE_FAILED, EVT_NODE_LEFT);

        IgniteInternalFuture<Object> stopFut = GridTestUtils.runAsync(() -> {
            nodeKillLatch.await();
            nodePrimary.close();
        });

        injectTestSystemOut();

        try (Transaction tx = nodeCoord.transactions().txStart(OPTIMISTIC, READ_COMMITTED)) {
            cache.put(primaryKey, 0);

            if (multiCache) {
                Integer primaryKeyWithoutWriteThrough = primaryKey(nodePrimary.cache(WITHOUT_WRITE_THROUGH_CACHE));
                cacheWithoutWriteThrough.put(primaryKeyWithoutWriteThrough, 0);
            }

            tx.commit();
        }
        catch (Throwable th) {
            fail("Unexpected exception: " + th);
        }

        stopFut.get(getTestTimeout());

        awaitPartitionMapExchange();

        assertEquals(EXIT_CODE_OK, execute("--port", connectorPort(grid(2)), "--cache", "idle_verify"));

        String out = testOut.toString();

        int cacheSize = cache.size();
        assertEquals(1, cacheSize);

        for (Ignite g : G.allGrids()) {
            //assertEquals("grid instance: " + g.name(), 1, g.cache(DEFAULT_CACHE_NAME).localSize(CachePeekMode.PRIMARY,  CachePeekMode.BACKUP));
            //g.compute(g.compute().clusterGroup().forNodeId(g.cluster().localNode().id())).execute(() -> , null);
            assertNotNull("grid instance: " + g.name(), g.cache(DEFAULT_CACHE_NAME).get(primaryKey));
            
            Object peeked = g.cache(DEFAULT_CACHE_NAME).localPeek(primaryKey, CachePeekMode.PRIMARY);
            if (peeked == null)
                peeked = g.cache(DEFAULT_CACHE_NAME).localPeek(primaryKey, CachePeekMode.BACKUP);

            assertNotNull("grid instance: " + g.name(), peeked);
        }

        // partVerHash are different, thus only partial size and counters check here
        if (withPersistence) {
            assertContains(log, out, "updateCntr=[lwm=2, missed=[], hwm=2], partitionState=OWNING, size=1");
            assertContains(log, out, "updateCntr=[lwm=2, missed=[], hwm=2], partitionState=OWNING, size=1");
        }
        else {
            assertContains(log, out, "updateCntr=1, partitionState=OWNING, size=1");
            assertContains(log, out, "updateCntr=1, partitionState=OWNING, size=1");
        }
        testOut.reset();

        if (withPersistence) {
            stopAllGrids();
            startGridsMultiThreaded(3);

            awaitPartitionMapExchange(true, true, null);

            assertEquals(EXIT_CODE_OK, execute("--port", connectorPort(grid(2)), "--cache", "idle_verify"));
            out = testOut.toString();

            //Pattern regexCheck = Pattern.compile(CORRECT_VERIFY_MSG);
            //boolean matches = regexCheck.matcher(out).find();

            // partVerHash are different, thus only regex check here
            String regexCheck = "Partition instances: \\[PartitionHashRecord" +
                ".*?consistentId=%s, updateCntr=\\[lwm=2, missed=\\[\\], hwm=2\\], partitionState=OWNING, size=1";
            Pattern part0Pattern = Pattern.compile(String.format(regexCheck, "gridCommandHandlerTest0"));
            Pattern part1Pattern = Pattern.compile(String.format(regexCheck, "gridCommandHandlerTest1"));
            Pattern part2Pattern = Pattern.compile(String.format(regexCheck, "gridCommandHandlerTest2"));

            boolean matches =
                part0Pattern.matcher(out).find() &&
                part1Pattern.matcher(out).find() &&
                part2Pattern.matcher(out).find();

            assertTrue(out, matches);
        }
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
            //cacheWithoutWriteThrough.put(primaryKeyWithoutWriteThrough, new Object());
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
            Pattern part0Pattern = Pattern.compile("Partition instances: " +
                "\\[PartitionHashRecord" +
                ".*?consistentId=gridCommandHandlerTest0, updateCntr=\\[lwm=1, missed=\\[\\], hwm=1\\], partitionState=OWNING, size=1");
            Pattern part1Pattern = Pattern.compile("Partition instances: " +
                "\\[PartitionHashRecord" +
                ".*?consistentId=gridCommandHandlerTest1, updateCntr=\\[lwm=1, missed=\\[\\], hwm=1\\], partitionState=OWNING, size=1");
            Pattern part2Pattern = Pattern.compile("Partition instances: " +
                "\\[PartitionHashRecord" +
                ".*?consistentId=gridCommandHandlerTest2, updateCntr=\\[lwm=1, missed=\\[\\], hwm=1\\], partitionState=OWNING, size=0");

            boolean matches =
                part0Pattern.matcher(out).find() &&
                part1Pattern.matcher(out).find() &&
                part2Pattern.matcher(out).find();

            assertTrue(matches);
        }
    }

    /** */
    private CacheConfiguration<Integer, Object> createCache(String cacheName, boolean writeThrough) {
        CacheConfiguration<Integer, Object> ccfg = new CacheConfiguration<>(cacheName);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setCacheMode(CacheMode.REPLICATED);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        //ccfg.setNearConfiguration(new NearCacheConfiguration());

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
        private final Map<Object, Object> map = new ConcurrentHashMap<>();

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
