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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Assert;

/**
 *
 */
public class IgniteClusterActivateDeactivateTestWithPersistence extends IgniteClusterActivateDeactivateTest {
    /** {@inheritDoc} */
    @Override protected boolean persistenceEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setAutoActivationEnabled(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivateCachesRestore_SingleNode() throws Exception {
        activateCachesRestore(1, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivateCachesRestore_SingleNode_WithNewCaches() throws Exception {
        activateCachesRestore(1, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivateCachesRestore_5_Servers() throws Exception {
        activateCachesRestore(5, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivateCachesRestore_5_Servers_WithNewCaches() throws Exception {
        activateCachesRestore(5, true);
    }

    /** */
    private Map<Integer, Integer> startGridsAndLoadData(int srvs) throws Exception {
        Ignite srv = startGrids(srvs);

        srv.active(true);

        srv.createCaches(Arrays.asList(cacheConfigurations1()));

        Map<Integer, Integer> cacheData = new LinkedHashMap<>();

        for (int i = 1; i <= 100; i++) {
            for (CacheConfiguration ccfg : cacheConfigurations1()) {
                srv.cache(ccfg.getName()).put(-i, i);

                cacheData.put(-i, i);
            }
        }

        return cacheData;
    }

    /**
     * @param srvs Number of server nodes.
     * @param withNewCaches If {@code true} then after restart has new caches in configuration.
     * @throws Exception If failed.
     */
    private void activateCachesRestore(int srvs, boolean withNewCaches) throws Exception {
        Map<Integer, Integer> cacheData = startGridsAndLoadData(srvs);

        stopAllGrids();

        for (int i = 0; i < srvs; i++) {
            if (withNewCaches)
                ccfgs = cacheConfigurations2();

            startGrid(i);
        }

        Ignite srv = ignite(0);

        checkNoCaches(srvs);

        srv.cluster().active(true);

        final int CACHES = withNewCaches ? 4 : 2;

        for (int i = 0; i < srvs; i++) {
            for (int c = 0; c < CACHES; c++)
                checkCache(ignite(i), CACHE_NAME_PREFIX + c, true);
        }

        DataStorageConfiguration dsCfg = srv.configuration().getDataStorageConfiguration();

        checkCachesData(cacheData, dsCfg);

        checkCaches(srvs, CACHES);

        int nodes = srvs;

        client = false;

        startGrid(nodes++);

        for (int i = 0; i < nodes; i++) {
            for (int c = 0; c < CACHES; c++)
                checkCache(ignite(i), CACHE_NAME_PREFIX + c, true);
        }

        checkCaches(nodes, CACHES);

        client = true;

        startGrid(nodes++);

        for (int c = 0; c < CACHES; c++)
            checkCache(ignite(nodes - 1), CACHE_NAME_PREFIX + c, false);

        checkCaches(nodes, CACHES);

        for (int i = 0; i < nodes; i++) {
            for (int c = 0; c < CACHES; c++)
                checkCache(ignite(i), CACHE_NAME_PREFIX + c, true);
        }

        checkCachesData(cacheData, dsCfg);
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/IGNITE-7330">IGNITE-7330</a> for more information about context of the test
     */
    public void testClientJoinsWhenActivationIsInProgress() throws Exception {
        startGridsAndLoadData(5);

        stopAllGrids();

        Ignite srv = startGrids(5);

        final CountDownLatch clientStartLatch = new CountDownLatch(1);

        IgniteInternalFuture clStartFut = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try {
                    clientStartLatch.await();

                    Thread.sleep(10);

                    client = true;

                    Ignite cl = startGrid("client0");

                    IgniteCache<Object, Object> atomicCache = cl.cache(CACHE_NAME_PREFIX + '0');
                    IgniteCache<Object, Object> txCache = cl.cache(CACHE_NAME_PREFIX + '1');

                    assertEquals(100, atomicCache.size());
                    assertEquals(100, txCache.size());
                }
                catch (Exception e) {
                    log.error("Error occurred", e);
                }
            }
        }, "client-starter-thread");

        clientStartLatch.countDown();
        srv.cluster().active(true);

        clStartFut.get();
    }

    /**
     * Checks that persistent caches are present with actual data and volatile caches are missing.
     *
     * @param cacheData Cache data.
     * @param dsCfg DataStorageConfiguration.
     */
    private void checkCachesData(Map<Integer, Integer> cacheData, DataStorageConfiguration dsCfg) {
        for (CacheConfiguration ccfg : cacheConfigurations1()) {
            if (CU.isPersistentCache(ccfg, dsCfg))
                checkCacheData(cacheData, ccfg.getName());
            else {
                for (Ignite node : G.allGrids())
                    assertTrue(node.cache(ccfg.getName()) == null || node.cache(ccfg.getName()).size() == 0);
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testActivateCacheRestoreConfigurationConflict() throws Exception {
        final int SRVS = 3;

        Ignite srv = startGrids(SRVS);

        srv.cluster().active(true);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        srv.createCache(ccfg);

        stopAllGrids();

        ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME + 1);

        ccfg.setGroupName(DEFAULT_CACHE_NAME);

        ccfgs = new CacheConfiguration[] {ccfg};

        try {
            startGrids(SRVS);

            fail();
        }
        catch (Exception e) {
            assertTrue(
                X.cause(e, IgniteCheckedException.class).getMessage().contains("Failed to start configured cache."));
        }
    }

    /**
     * Test that after deactivation during eviction and rebalance and activation again after
     * all data in cache is consistent.
     *
     * @throws Exception If failed.
     */
    public void testDeactivateDuringEvictionAndRebalance() throws Exception {
        IgniteEx srv = (IgniteEx) startGrids(3);

        srv.cluster().active(true);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME)
            .setBackups(1)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setIndexedTypes(Integer.class, Integer.class)
            .setAffinity(new RendezvousAffinityFunction(false, 64));

        IgniteCache cache = srv.createCache(ccfg);

        // High number of keys triggers long partition eviction.
        final int keysCount = 100_000;

        try (IgniteDataStreamer ds = srv.dataStreamer(DEFAULT_CACHE_NAME)) {
            log.info("Writing initial data...");

            ds.allowOverwrite(true);
            for (int k = 1; k <= keysCount; k++) {
                ds.addData(k, k);

                if (k % 50_000 == 0)
                    log.info("Written " + k + " entities.");
            }

            log.info("Writing initial data finished.");
        }

        AtomicInteger keyCounter = new AtomicInteger(keysCount);
        AtomicBoolean stop = new AtomicBoolean(false);

        Set<Integer> addedKeys = new GridConcurrentHashSet<>();

        IgniteInternalFuture cacheLoadFuture = GridTestUtils.runMultiThreadedAsync(() -> {
            while (!stop.get()) {
                int key = keyCounter.incrementAndGet();
                try {
                    cache.put(key, key);

                    addedKeys.add(key);

                    Thread.sleep(10);
                }
                catch (Exception ignored) { }
            }
        }, 2, "cache-load");

        stopGrid(2);

        // Wait for some data.
        Thread.sleep(3000);

        startGrid(2);

        log.info("Stop load...");

        stop.set(true);

        cacheLoadFuture.get();

        // Deactivate and activate again.
        srv.cluster().active(false);

        srv.cluster().active(true);

        awaitPartitionMapExchange();

        log.info("Checking data...");

        for (Ignite ignite : G.allGrids()) {
            IgniteCache cache1 = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

            for (int k = 1; k <= keysCount; k++) {
                Object val = cache1.get(k);

                Assert.assertNotNull("node=" + ignite.name() + ", key=" + k, val);

                Assert.assertTrue("node=" + ignite.name() + ", key=" + k + ", val=" + val, (int) val == k);
            }

            for (int k : addedKeys) {
                Object val = cache1.get(k);

                Assert.assertNotNull("node=" + ignite.name() + ", key=" + k, val);

                Assert.assertTrue("node=" + ignite.name() + ", key=" + k + ", val=" + val, (int) val == k);
            }
        }
    }
}
