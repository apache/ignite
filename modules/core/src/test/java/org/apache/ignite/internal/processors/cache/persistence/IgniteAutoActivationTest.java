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
package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class IgniteAutoActivationTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        MemoryConfiguration memCfg = new MemoryConfiguration();
        memCfg.setPageSize(1024);
        memCfg.setDefaultMemoryPolicySize(10 * 1024 * 1024);

        cfg.setMemoryConfiguration(memCfg);

        PersistentStoreConfiguration pCfg = new PersistentStoreConfiguration();
        pCfg.setWalMode(WALMode.LOG_ONLY);

        cfg.setPersistentStoreConfiguration(pCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        GridTestUtils.deleteDbFiles();
    }

    /**
     *
     */
    public void testAutoActivationSimple() throws Exception {
        startGrids(3);

        IgniteEx srv = grid(0);

        srv.active(true);

        createAndFillCache(srv);

        stopAllGrids();

        //note: no call for activation after grid restart
        startGrids(3);

        srv = grid(0);

        awaitActivation(srv);

        checkDataInCache(srv);
    }

    /**
     * Verifies scenario when user activates grid manually before reaching previously established BaselineTopology.
     */
    public void testBaselineTopologyReplacement() throws Exception {
        startGrids(3);

        IgniteEx srv = grid(0);

        srv.active(true);

        createAndFillCache(srv);

        stopAllGrids();

        startGrids(2);

        srv = grid(0);

        srv.active(true);

        checkDataInCache(srv);
    }

    private static final int ENTRIES_COUNT = 100;

    private void checkDataInCache(IgniteEx srv) {
        IgniteCache<Object, Object> cache = srv.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < ENTRIES_COUNT; i++) {
            TestValue testVal = (TestValue) cache.get(i);

            assertNotNull(testVal);

            assertEquals(i, testVal.id);
        }
    }

    private void createAndFillCache(Ignite srv) {
        IgniteCache cache = srv.getOrCreateCache(cacheConfiguration());

        for (int i = 0; i < ENTRIES_COUNT; i++)
            cache.put(i, new TestValue(i, "str" + i));
    }

    private void awaitActivation(Ignite srv) throws Exception {
        //TODO busy spinning for now, need to introduce an API to await for automatic activation
        int awaitCntr = 0;

        while (!srv.active()) {
            if (awaitCntr++ > 12)
                throw new Exception("Grid auto activation didn't happen in one minute");

            Thread.sleep(5_000);
        }
    }

    /**
     *
     */
    private CacheConfiguration cacheConfiguration() {
        return new CacheConfiguration()
            .setName(DEFAULT_CACHE_NAME)
            .setCacheMode(CacheMode.PARTITIONED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setBackups(2)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
    }

    /** */
    private static final class TestValue {
        /** */
        private final int id;

        /** */
        private final String strId;

        /** */
        private TestValue(int id, String strId) {
            this.id = id;
            this.strId = strId;
        }
    }

}
