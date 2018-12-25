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

package org.apache.ignite.internal.processors.cache.distributed.replicated.preloader;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListener;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.binary.BinaryEnumObjectImpl;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P2;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.CachePluginConfiguration;
import org.apache.ignite.spi.eventstorage.memory.MemoryEventStorageSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.ASYNC;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.configuration.DeploymentMode.CONTINUOUS;
import static org.apache.ignite.events.EventType.EVTS_ALL;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_STARTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_STOPPED;


/**
 * Tests for replicated cache preloader.
 */
@SuppressWarnings("unchecked")
@RunWith(JUnit4.class)
public class GridCacheReplicatedPreloadSelfTest extends GridCommonAbstractTest {
    /** */
    private CacheRebalanceMode preloadMode = ASYNC;

    /** */
    private int batchSize = 4096;

    /** */
    private volatile boolean extClassloadingAtCfg = false;

    /** */
    private volatile boolean isClient = false;

    /** */
    private volatile boolean useExtClassLoader = false;

    /** Disable p2p. */
    private volatile boolean disableP2p = false;

    /** */
    private static volatile CountDownLatch latch;

    /** */
    private static boolean cutromEvt = false;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setRebalanceThreadPoolSize(2);

        cfg.setCacheConfiguration(cacheConfiguration(igniteInstanceName));

        cfg.setDeploymentMode(CONTINUOUS);

        cfg.setUserAttributes(F.asMap("EVEN", !igniteInstanceName.endsWith("0") && !igniteInstanceName.endsWith("2")));

        MemoryEventStorageSpi spi = new MemoryEventStorageSpi();

        spi.setExpireCount(50_000);

        cfg.setEventStorageSpi(spi);

        if (disableP2p)
            cfg.setPeerClassLoadingEnabled(false);

        if (getTestIgniteInstanceName(1).equals(igniteInstanceName) || useExtClassLoader ||
            cfg.getMarshaller() instanceof BinaryMarshaller)
            cfg.setClassLoader(getExternalClassLoader());

        if (isClient)
            cfg.setClientMode(true);

        if (cutromEvt) {
            int[] evts = new int[EVTS_ALL.length + 1];

            evts[0] = Integer.MAX_VALUE - 1;

            System.arraycopy(EVTS_ALL, 0, evts, 1, EVTS_ALL.length);

            cfg.setIncludeEventTypes(evts);
        }

        return cfg;
    }

    /**
     * Gets cache configuration for grid with specified name.
     *
     * @param igniteInstanceName Ignite instance name.
     * @return Cache configuration.
     */
    CacheConfiguration cacheConfiguration(String igniteInstanceName) {
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(REPLICATED);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setRebalanceMode(preloadMode);
        cacheCfg.setRebalanceBatchSize(batchSize);

        if (extClassloadingAtCfg)
            loadExternalClassesToCfg(cacheCfg);

        return cacheCfg;
    }

    /**
     *
     * @param cacheCfg Configuration.
     */
    private void loadExternalClassesToCfg(CacheConfiguration cacheCfg) {
        try {
            Object sf = getExternalClassLoader().
                loadClass("org.apache.ignite.tests.p2p.CacheDeploymentTestStoreFactory").newInstance();

            cacheCfg.setCacheStoreFactory((Factory)sf);

            Object sslf = getExternalClassLoader().
                loadClass("org.apache.ignite.tests.p2p.CacheDeploymentStoreSessionListenerFactory").newInstance();

            cacheCfg.setCacheStoreSessionListenerFactories((Factory)sslf);

            Object cpc = getExternalClassLoader().
                loadClass("org.apache.ignite.tests.p2p.CacheDeploymentCachePluginConfiguration").newInstance();

            cacheCfg.setPluginConfigurations((CachePluginConfiguration)cpc);

            Object akm = getExternalClassLoader().
                loadClass("org.apache.ignite.tests.p2p.CacheDeploymentAffinityKeyMapper").newInstance();

            cacheCfg.setAffinityMapper((AffinityKeyMapper)akm);

            Object pred = getExternalClassLoader().
                loadClass("org.apache.ignite.tests.p2p.CacheDeploymentAlwaysTruePredicate2").newInstance();

            cacheCfg.setNodeFilter((IgnitePredicate)pred);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSingleNode() throws Exception {
        preloadMode = SYNC;

        try {
            startGrid(1);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testIntegrity() throws Exception {
        preloadMode = SYNC;

        try {
            Ignite g1 = startGrid(1);

            GridCacheAdapter<Integer, String> cache1 = ((IgniteKernal)g1).internalCache(DEFAULT_CACHE_NAME);

            // Cache rebalancing events should not be fired for this cache.
            CacheConfiguration ccfg = cacheConfiguration(((IgniteKernal)g1).getInstanceName())
                .setName(DEFAULT_CACHE_NAME + "_evts_disabled")
                .setEventsDisabled(true);

            g1.getOrCreateCache(ccfg);

            cache1.getAndPut(1, "val1");
            cache1.getAndPut(2, "val2");

            GridCacheEntryEx e1 = cache1.entryEx(1);

            assertNotNull(e1);

            e1.unswap();

            Ignite g2 = startGrid(2);

            Collection<Event> evts = null;

            for (int i = 0; i < 3; i++) {
                evts = g2.events().localQuery(F.<Event>alwaysTrue(),
                    EVT_CACHE_REBALANCE_STARTED, EVT_CACHE_REBALANCE_STOPPED);

                if (evts.size() != 2) {
                    info("Wrong events collection size (will retry in 1000 ms): " + evts.size());

                    Thread.sleep(1000);
                }
                else
                    break;
            }

            assertNotNull(evts);
            assertEquals("Wrong events received: " + evts, 2, evts.size());

            Iterator<Event> iter = evts.iterator();

            assertEquals(EVT_CACHE_REBALANCE_STARTED, iter.next().type());
            assertEquals(EVT_CACHE_REBALANCE_STOPPED, iter.next().type());

            IgniteCache<Integer, String> cache2 = g2.cache(DEFAULT_CACHE_NAME);

            assertEquals("val1", cache2.localPeek(1));
            assertEquals("val2", cache2.localPeek(2));

            GridCacheAdapter<Integer, String> cacheAdapter2 = ((IgniteKernal)g2).internalCache(DEFAULT_CACHE_NAME);

            GridCacheEntryEx e2 = cacheAdapter2.entryEx(1);

            assertNotNull(e2);
            assertNotSame(e2, e1);

            e2.unswap();

            assertNotNull(e2.version());

            assertEquals(e1.version(), e2.version());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testDeployment() throws Exception {
        // TODO GG-11141.
        if (true)
            return;

        preloadMode = SYNC;

        try {
            Ignite g1 = startGrid(1);
            Ignite g2 = startGrid(2);

            IgniteCache<Integer, Object> cache1 = g1.cache(DEFAULT_CACHE_NAME);
            IgniteCache<Integer, Object> cache2 = g2.cache(DEFAULT_CACHE_NAME);

            ClassLoader ldr = grid(1).configuration().getClassLoader();

            Object v1 = ldr.loadClass("org.apache.ignite.tests.p2p.CacheDeploymentTestValue3").newInstance();

            cache1.put(1, v1);

            info("Stored value in cache1 [v=" + v1 + ", ldr=" + v1.getClass().getClassLoader() + ']');

            Object v2 = cache2.get(1);

            info("Read value from cache2 [v=" + v2 + ", ldr=" + v2.getClass().getClassLoader() + ']');

            assert v2 != null;
            assert v2.toString().equals(v1.toString());
            assert !v2.getClass().getClassLoader().equals(getClass().getClassLoader());
            assert v2.getClass().getClassLoader().getClass().getName().contains("GridDeploymentClassLoader") ||
                grid(2).configuration().getMarshaller() instanceof BinaryMarshaller;

            Object e1 = ldr.loadClass("org.apache.ignite.tests.p2p.CacheDeploymentTestEnumValue").getEnumConstants()[0];

            cache1.put(2, e1);

            Object e2 = cache2.get(2);

            if (g1.configuration().getMarshaller() instanceof BinaryMarshaller) {
                BinaryObject enumObj = (BinaryObject)cache2.withKeepBinary().get(2);

                assertEquals(0, enumObj.enumOrdinal());
                assertTrue(enumObj.type().isEnum());
                assertTrue(enumObj instanceof BinaryEnumObjectImpl);
            }

            assert e2 != null;
            assert e2.toString().equals(e1.toString());
            assert !e2.getClass().getClassLoader().equals(getClass().getClassLoader());
            assert e2.getClass().getClassLoader().getClass().getName().contains("GridDeploymentClassLoader") ||
                grid(2).configuration().getMarshaller() instanceof BinaryMarshaller;

            stopGrid(1);

            Ignite g3 = startGrid(3);

            IgniteCache<Integer, Object> cache3 = g3.cache(DEFAULT_CACHE_NAME);

            Object v3 = cache3.localPeek(1, CachePeekMode.ONHEAP);

            assert v3 != null;

            info("Read value from cache3 [v=" + v3 + ", ldr=" + v3.getClass().getClassLoader() + ']');

            assert v3 != null;
            assert v3.toString().equals(v1.toString());
            assert !v3.getClass().getClassLoader().equals(getClass().getClassLoader());
            assert v3.getClass().getClassLoader().getClass().getName().contains("GridDeploymentClassLoader") ||
                grid(3).configuration().getMarshaller() instanceof BinaryMarshaller;
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testExternalClassesAtConfiguration() throws Exception {
        try {
            extClassloadingAtCfg = true;
            useExtClassLoader = true;

            Ignite g1 = startGrid(1);

            Ignite g2 = startGrid(2);  // Checks deserialization at node join.

            isClient = true;

            Ignite g3 = startGrid(3);

            IgniteCache<Integer, Object> cache1 = g1.cache(DEFAULT_CACHE_NAME);
            IgniteCache<Integer, Object> cache2 = g2.cache(DEFAULT_CACHE_NAME);
            IgniteCache<Integer, Object> cache3 = g3.cache(DEFAULT_CACHE_NAME);

            final Class<CacheEntryListener> cls1 = (Class<CacheEntryListener>) getExternalClassLoader().
                loadClass("org.apache.ignite.tests.p2p.CacheDeploymentCacheEntryListener");
            final Class<CacheEntryEventSerializableFilter> cls2 = (Class<CacheEntryEventSerializableFilter>) getExternalClassLoader().
                loadClass("org.apache.ignite.tests.p2p.CacheDeploymentCacheEntryEventSerializableFilter");

            CacheEntryListenerConfiguration<Integer, Object> lsnrCfg = new MutableCacheEntryListenerConfiguration<>(
                new Factory<CacheEntryListener<Integer, Object>>() {
                    @Override public CacheEntryListener<Integer, Object> create() {
                        try {
                            return cls1.newInstance();
                        }
                        catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                },
                new ClassFilterFactory(cls2),
                true,
                true
            );

            cache1.registerCacheEntryListener(lsnrCfg);

            cache1.put(1, 1);

            assertEquals(1, cache2.get(1));
            assertEquals(1, cache3.get(1));
        }
        finally {
            extClassloadingAtCfg = false;
            isClient = false;
            useExtClassLoader = false;
        }
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testExternalClassesAtConfigurationDynamicStart() throws Exception {
        try {
            extClassloadingAtCfg = false;
            useExtClassLoader = true;

            Ignite g1 = startGrid(1);
            Ignite g2 = startGrid(2);

            isClient = true;

            Ignite g3 = startGrid(3);

            CacheConfiguration cfg = defaultCacheConfiguration();

            loadExternalClassesToCfg(cfg);

            cfg.setName("customStore");

            IgniteCache<Integer, Object> cache1 = g1.createCache(cfg);

            IgniteCache<Integer, Object> cache2 = g2.getOrCreateCache(cfg); // Checks deserialization at cache creation.
            IgniteCache<Integer, Object> cache3 = g3.getOrCreateCache(cfg); // Checks deserialization at cache creation.

            cache1.put(1, 1);

            assertEquals(1, cache2.get(1));
            assertEquals(1, cache3.get(1));
        }
        finally {
            extClassloadingAtCfg = false;
            isClient = false;
            useExtClassLoader = false;
        }
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testExternalClassesAtConfigurationDynamicStart2() throws Exception {
        try {
            extClassloadingAtCfg = false;
            useExtClassLoader = true;

            Ignite g1 = startGrid(1);
            Ignite g2 = startGrid(2);

            isClient = true;

            Ignite g3 = startGrid(3);

            CacheConfiguration cfg = defaultCacheConfiguration();

            loadExternalClassesToCfg(cfg);

            cfg.setName("customStore");

            IgniteCache<Integer, Object> cache1 = g1.getOrCreateCache(cfg);

            IgniteCache<Integer, Object> cache2 = g2.getOrCreateCache("customStore"); // Checks deserialization at cache creation.
            IgniteCache<Integer, Object> cache3 = g3.getOrCreateCache("customStore"); // Checks deserialization at cache creation.

            cache1.put(1, 1);

            assertEquals(1, cache2.get(1));
            assertEquals(1, cache3.get(1));
        }
        finally {
            extClassloadingAtCfg = false;
            isClient = false;
            useExtClassLoader = false;
        }
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testExternalClassesAtMessage() throws Exception {
        try {
            useExtClassLoader = true;
            disableP2p = true;

            final Class cls = (Class)getExternalClassLoader().
                loadClass("org.apache.ignite.tests.p2p.CacheDeploymentExternalizableTestValue");

            Ignite g1 = startGrid(1);
            startGrid(2);

            IgniteMessaging rmtMsg = g1.message();

            latch = new CountDownLatch(2);

            rmtMsg.remoteListen("MyOrderedTopic", new MessageListener());

            Object o = cls.newInstance();

            o.toString();

            rmtMsg.send("MyOrderedTopic", o);
            rmtMsg.sendOrdered("MyOrderedTopic", o, 0);

            latch.await();

            // Custom topic.

            final Class cls2 = (Class)getExternalClassLoader().
                loadClass("org.apache.ignite.tests.p2p.CacheDeploymentTestEnumValue");

            Object topic = cls2.getEnumConstants()[0];

            latch = new CountDownLatch(2);

            rmtMsg.remoteListen(topic, new MessageListener());

            rmtMsg.send(topic, topic);
            rmtMsg.sendOrdered(topic, topic, 0);

            latch.await();

        }
        finally {
            useExtClassLoader = false;
            disableP2p = false;
        }
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testExternalClassesAtEventP2pDisabled() throws Exception {
        MvccFeatureChecker.failIfNotSupported(MvccFeatureChecker.Feature.CACHE_EVENTS);

        testExternalClassesAtEvent0(true);
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testExternalClassesAtEvent() throws Exception {
        MvccFeatureChecker.failIfNotSupported(MvccFeatureChecker.Feature.CACHE_EVENTS);

        testExternalClassesAtEvent0(false);
    }

    /**
     * @throws Exception If test failed.
     */
    private void testExternalClassesAtEvent0(boolean p2p) throws Exception {
        try {
            useExtClassLoader = true;
            cutromEvt = true;

            if (p2p)
                disableP2p = true;

            final Class cls = (Class)getExternalClassLoader().
                loadClass("org.apache.ignite.tests.p2p.CacheDeploymentExternalizableTestValue");
            final Class cls2 = (Class)getExternalClassLoader().
                loadClass("org.apache.ignite.tests.p2p.GridEventConsumeFilter");

            Ignite g1 = startGrid(1);
            startGrid(2);

            latch = new CountDownLatch(3);

            g1.events().localListen((IgnitePredicate)cls2.newInstance(), EVT_CACHE_OBJECT_PUT);
            g1.events().localListen(new EventListener(), EVT_CACHE_OBJECT_PUT);

            g1.events().remoteListen(null, (IgnitePredicate)cls2.newInstance(), EVT_CACHE_OBJECT_PUT);
            g1.events().remoteListen(null, new EventListener(), EVT_CACHE_OBJECT_PUT);

            g1.cache(DEFAULT_CACHE_NAME).put("1", cls.newInstance());

            latch.await();
        }
        finally {
            useExtClassLoader = false;
            cutromEvt = false;

            if (p2p)
                disableP2p = false;
        }
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testSync() throws Exception {
        preloadMode = SYNC;
        batchSize = 512;

        try {
            IgniteCache<Integer, String> cache1 = startGrid(1).cache(DEFAULT_CACHE_NAME);

            int keyCnt = 1000;

            for (int i = 0; i < keyCnt; i++)
                cache1.put(i, "val" + i);

            IgniteCache<Integer, String> cache2 = startGrid(2).cache(DEFAULT_CACHE_NAME);

            assertEquals(keyCnt, cache2.localSize(CachePeekMode.ALL));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testAsync() throws Exception {
        preloadMode = ASYNC;
        batchSize = 256;

        try {
            IgniteCache<Integer, String> cache1 = startGrid(1).cache(DEFAULT_CACHE_NAME);

            final int keyCnt = 2000;

            for (int i = 0; i < keyCnt; i++)
                cache1.put(i, "val" + i);

            final IgniteCache<Integer, String> cache2 = startGrid(2).cache(DEFAULT_CACHE_NAME);

            int size = cache2.localSize(CachePeekMode.ALL);

            info("Size of cache2: " + size);

            boolean awaitSize = GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return cache2.localSize(CachePeekMode.ALL) >= keyCnt;
                }
            }, getTestTimeout());

            assertTrue("Actual cache size: " + cache2.localSize(CachePeekMode.ALL), awaitSize);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testBatchSize1() throws Exception {
        preloadMode = SYNC;
        batchSize = 1; // 1 byte but one entry should be in batch anyway.

        try {
            IgniteCache<Integer, String> cache1 = startGrid(1).cache(DEFAULT_CACHE_NAME);

            int cnt = 100;

            for (int i = 0; i < cnt; i++)
                cache1.put(i, "val" + i);

            IgniteCache<Integer, String> cache2 = startGrid(2).cache(DEFAULT_CACHE_NAME);

            assertEquals(cnt, cache2.localSize(CachePeekMode.ALL));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testBatchSize1000() throws Exception {
        preloadMode = SYNC;
        batchSize = 1000; // 1000 bytes.

        try {
            IgniteCache<Integer, String> cache1 = startGrid(1).cache(DEFAULT_CACHE_NAME);

            int cnt = 100;

            for (int i = 0; i < cnt; i++)
                cache1.put(i, "val" + i);

            IgniteCache<Integer, String> cache2 = startGrid(2).cache(DEFAULT_CACHE_NAME);

            assertEquals(cnt, cache2.localSize(CachePeekMode.ALL));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testBatchSize10000() throws Exception {
        preloadMode = SYNC;
        batchSize = 10000; // 10000 bytes.

        try {
            IgniteCache<Integer, String> cache1 = startGrid(1).cache(DEFAULT_CACHE_NAME);

            int cnt = 100;

            for (int i = 0; i < cnt; i++)
                cache1.put(i, "val" + i);

            IgniteCache<Integer, String> cache2 = startGrid(2).cache(DEFAULT_CACHE_NAME);

            assertEquals(cnt, cache2.localSize(CachePeekMode.ALL));
        }
        finally {
            stopGrid(1);
            stopGrid(2);
        }
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testMultipleNodes() throws Exception {
        preloadMode = ASYNC;
        batchSize = 256;

        try {
            int gridCnt = 4;

            startGridsMultiThreaded(gridCnt);

            info("Beginning data population...");

            final int cnt = 2500;

            Map<Integer, String> map = null;

            for (int i = 0; i < cnt; i++) {
                if (i % 100 == 0) {
                    if (map != null && !map.isEmpty()) {
                        grid(0).cache(DEFAULT_CACHE_NAME).putAll(map);

                        info("Put entries count: " + i);
                    }

                    map = new HashMap<>();
                }

                map.put(i, "val" + i);
            }

            if (map != null && !map.isEmpty())
                grid(0).cache(DEFAULT_CACHE_NAME).putAll(map);

            for (int gridIdx = 0; gridIdx < gridCnt; gridIdx++) {
                assert grid(gridIdx).cache(DEFAULT_CACHE_NAME).localSize(CachePeekMode.ALL) == cnt :
                    "Actual size: " + grid(gridIdx).cache(DEFAULT_CACHE_NAME).localSize(CachePeekMode.ALL);

                info("Cache size is OK for grid index: " + gridIdx);
            }

            final IgniteCache<Integer, String> lastCache = startGrid(gridCnt).cache(DEFAULT_CACHE_NAME);

            // Let preloading start.
            Thread.sleep(1000);

            // Stop random initial node while preloading is in progress.
            int idx = new Random().nextInt(gridCnt);

            info("Stopping node with index: " + idx);

            stopGrid(idx);

            awaitPartitionMapExchange(true, true, null);

            boolean awaitSize = GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return lastCache.localSize(CachePeekMode.ALL) >= cnt;
                }
            }, 20_000);

            assertTrue("Actual cache size: " + lastCache.localSize(CachePeekMode.ALL), awaitSize);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testConcurrentStartSync() throws Exception {
        preloadMode = SYNC;
        batchSize = 10000;

        try {
            startGridsMultiThreaded(4);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If test failed.
     */
    @Test
    public void testConcurrentStartAsync() throws Exception {
        preloadMode = ASYNC;
        batchSize = 10000;

        try {
            startGridsMultiThreaded(4);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    private static class MessageListener implements P2<UUID, Object> {
        /**
         * @param nodeId
         * @param msg
         * @return
         */
        @Override public boolean apply(UUID nodeId, Object msg) {
            System.out.println("Received message [msg=" + msg + ", from=" + nodeId + ']');

            latch.countDown();

            return true; // Return true to continue listening.
        }
    }

    private static class EventListener implements IgnitePredicate<Event> {
        @Override public boolean apply(Event evt) {
            System.out.println("Cache event: " + evt);

            latch.countDown();

            return true;
        }
    }

    /**
     *
     */
    private static class ClassFilterFactory implements Factory<CacheEntryEventFilter<Integer, Object>> {
        /** */
        private Class<CacheEntryEventSerializableFilter> cls;

        /**
         * @param cls Class.
         */
        public ClassFilterFactory(Class<CacheEntryEventSerializableFilter> cls) {
            this.cls = cls;
        }

        /** {@inheritDoc} */
        @Override public CacheEntryEventSerializableFilter<Integer, Object> create() {
            try {
                return cls.newInstance();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
