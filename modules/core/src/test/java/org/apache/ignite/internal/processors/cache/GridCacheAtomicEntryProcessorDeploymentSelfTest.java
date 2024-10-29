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

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Cache EntryProcessor + Deployment.
 */
public class GridCacheAtomicEntryProcessorDeploymentSelfTest extends GridCommonAbstractTest {
    /** Test value. */
    protected static String TEST_VALUE = "org.apache.ignite.tests.p2p.CacheDeploymentTestValue";

    /** */
    protected DeploymentMode depMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (cfg.isClientMode())
            cfg.setClassLoader(getExternalClassLoader());

        cfg.setDeploymentMode(depMode);
        cfg.setCacheConfiguration(cacheConfiguration());
        cfg.setConnectorConfiguration(null);

        return cfg;
    }

    /**
     * @return Cache configuration.
     * @throws Exception In case of error.
     */
    protected CacheConfiguration cacheConfiguration() throws Exception {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setCacheMode(PARTITIONED);
        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setRebalanceMode(SYNC);
        cfg.setAtomicityMode(atomicityMode());
        cfg.setNearConfiguration(new NearCacheConfiguration());
        cfg.setBackups(1);

        return cfg;
    }

    /**
     * @return Cache.
     */
    protected IgniteCache getCache() {
        return grid(1).cache(DEFAULT_CACHE_NAME);
    }

    /**
     * @return Entry Processor.
     */
    protected String getEntryProcessor() {
        return GridTestProperties.getProperty(GridTestProperties.ENTRY_PROCESSOR_CLASS_NAME) != null ?
            GridTestProperties.getProperty(GridTestProperties.ENTRY_PROCESSOR_CLASS_NAME) :
            "org.apache.ignite.tests.p2p.CacheDeploymentEntryProcessor";
    }

    /** */
    protected CacheAtomicityMode atomicityMode() {
        return ATOMIC;
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testInvokeDeployment() throws Exception {
        depMode = DeploymentMode.CONTINUOUS;

        doTestInvoke();
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testInvokeDeployment2() throws Exception {
        depMode = DeploymentMode.SHARED;

        doTestInvoke();
        doTestInvokeEx();
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testInvokeAllDeployment() throws Exception {
        depMode = DeploymentMode.CONTINUOUS;

        doTestInvokeAll();
    }

    /**
     * @throws Exception In case of error.
     */
    @Test
    public void testInvokeAllDeployment2() throws Exception {
        depMode = DeploymentMode.SHARED;

        doTestInvokeAll();
    }

    /**
     * @throws Exception In case of error.
     */
    private void doTestInvoke() throws Exception {
        try {
            startGrid(0);
            startClientGrid(1);

            Class procCls = grid(1).configuration().getClassLoader().loadClass(getEntryProcessor());
            Class valCls = grid(1).configuration().getClassLoader().loadClass(TEST_VALUE);

            assertTrue(grid(1).configuration().isClientMode());
            assertFalse(grid(0).configuration().isClientMode());

            IgniteCache cache = getCache();

            cache.put("key", valCls.newInstance());

            Boolean res = (Boolean)cache.invoke("key", (CacheEntryProcessor)procCls.newInstance());

            assertTrue(res);

            // Checks that get produces no exceptions.
            cache.get("key");
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Scenario: 2 different client nodes invoke entry processors on intersected collection of keys.
     * @throws Exception
     */
    private void doTestInvokeEx() throws Exception {
        String testCacheName = "dynamic_params";

        String prcClsName = "org.apache.ignite.tests.p2p.CacheDeploymentEntryProcessorMultipleEnts";

        String contClsName = "org.apache.ignite.tests.p2p.cache.Container";

        try {
            startGrid(0);
            IgniteEx cli1 = startClientGrid(1);
            IgniteEx cli2 = startClientGrid(2);

            Class procCls1 = cli1.configuration().getClassLoader().loadClass(prcClsName);
            Class procCls2 = cli2.configuration().getClassLoader().loadClass(prcClsName);

            Class contCls1 = cli1.configuration().getClassLoader().loadClass(contClsName);
            Class contCls2 = cli2.configuration().getClassLoader().loadClass(contClsName);

            // just one more additional class unavailability check.
            try {
                Class.forName(TEST_VALUE);
                fail();
            }
            catch (ClassNotFoundException e) {
                // No op.
            }

            Class<?> cacheValClazz = grid(2).configuration().getClassLoader().loadClass(TEST_VALUE);
            Object cacheVal = cacheValClazz.newInstance();

            CacheConfiguration<Long, Object> ccfg = new CacheConfiguration<>();
            ccfg.setCacheMode(REPLICATED);
            ccfg.setAtomicityMode(ATOMIC);
            ccfg.setName(testCacheName);

            IgniteCache<Long, Object> processedCache = cli1.createCache(ccfg);

            Map<Long, Object> map = new HashMap<>();
            for (long i = 0; i < 100; i++) {
                map.put(i, cacheVal);
            }

            processedCache.putAll(map);

            IgniteCache<Object, Object> cache1 = cli1.cache(testCacheName);
            IgniteCache<Object, Object> cache2 = cli2.cache(testCacheName);

            Object cont1 = contCls1.getDeclaredConstructor(Object.class).newInstance(map);
            Object cont2 = contCls2.getDeclaredConstructor(Object.class).newInstance(map);

            for (int i = 0; i < 10; ++i) {
                IgniteCache<Object, Object> procCache1 = cache1;
                IgniteInternalFuture<Object> f1 = GridTestUtils.runAsync(() -> {
                    for (long key = 0; key < 10; key++) {
                        procCache1.invoke(key,
                                (CacheEntryProcessor)procCls1.getDeclaredConstructor(Object.class).newInstance(cont1));
                    }
                });

                IgniteCache<Object, Object> procCache2 = cache2;
                IgniteInternalFuture<Object> f2 = GridTestUtils.runAsync(() -> {
                    for (long key = 10; key > 0; key--) {
                        procCache2.invoke(key,
                                (CacheEntryProcessor)procCls2.getDeclaredConstructor(Object.class).newInstance(cont2));
                    };
                });

                long duration = TimeUnit.SECONDS.toMillis(30);

                f1.get(duration);
                f2.get(duration);

                stopAllClients(true);

                cli1 = startClientGrid(1);
                cli2 = startClientGrid(2);

                cache1 = cli1.cache(testCacheName);
                cache2 = cli2.cache(testCacheName);
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception In case of error.
     */
    private void doTestInvokeAll() throws Exception {
        try {
            startGrid(0);
            startClientGrid(1);

            Class procCls = grid(1).configuration().getClassLoader().loadClass(getEntryProcessor());
            Class valCls = grid(1).configuration().getClassLoader().loadClass(TEST_VALUE);

            assertTrue(grid(1).configuration().isClientMode());
            assertFalse(grid(0).configuration().isClientMode());

            IgniteCache cache = getCache();

            TreeSet keys = new TreeSet();

            for (int i = 0; i < 3; i++) {
                String key = "key" + i;

                cache.put(key, valCls.newInstance());

                keys.add(key);
            }

            Map<String, EntryProcessorResult> res = (Map<String, EntryProcessorResult>)cache.invokeAll(keys,
                (CacheEntryProcessor)procCls.newInstance());

            assertEquals(3, res.size());

            for (EntryProcessorResult result : res.values())
                assertTrue((Boolean)result.get());

            // Checks that get produces no exceptions.
            for (int i = 0; i < 3; i++) {
                String key = "key" + i;

                cache.get(key);
            }
        }
        finally {
            stopAllGrids();
        }
    }
}
