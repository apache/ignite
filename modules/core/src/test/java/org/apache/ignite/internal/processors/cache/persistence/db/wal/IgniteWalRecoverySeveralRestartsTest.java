/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 *
 */
public class IgniteWalRecoverySeveralRestartsTest extends GridCommonAbstractTest {
    /** */
    public static final int PAGE_SIZE = 1024;

    /** */
    private static final int KEYS_COUNT = 100_000;

    /** */
    private static final int LARGE_KEYS_COUNT = 5_000;

    /** */
    private static final Random rnd = new Random(System.currentTimeMillis());

    /** */
    private String cacheName = "test";

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 3600_000;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration<Integer, IndexedObject> ccfg = new CacheConfiguration<>(cacheName);

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setRebalanceMode(CacheRebalanceMode.NONE);
        ccfg.setIndexedTypes(Integer.class, IndexedObject.class);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 64 * 4)); // 64 per node
        ccfg.setReadFromBackup(true);

        cfg.setCacheConfiguration(ccfg);

        MemoryConfiguration dbCfg = new MemoryConfiguration();

        dbCfg.setPageSize(PAGE_SIZE);

        MemoryPolicyConfiguration memPlcCfg = new MemoryPolicyConfiguration();

        memPlcCfg.setName("dfltMemPlc");
        memPlcCfg.setInitialSize(500 * 1024 * 1024);
        memPlcCfg.setMaxSize(500 * 1024 * 1024);

        dbCfg.setMemoryPolicies(memPlcCfg);
        dbCfg.setDefaultMemoryPolicyName("dfltMemPlc");

        cfg.setMemoryConfiguration(dbCfg);

        cfg.setPersistentStoreConfiguration(
            new PersistentStoreConfiguration()
                .setWalMode(WALMode.LOG_ONLY)
        );

        cfg.setMarshaller(null);

        BinaryConfiguration binCfg = new BinaryConfiguration();

        binCfg.setCompactFooter(false);

        cfg.setBinaryConfiguration(binCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
    }

    /**
     * @throws Exception if failed.
     */
    public void testWalRecoverySeveralRestarts() throws Exception {
        try {
            IgniteEx ignite = startGrid(1);

            ignite.active(true);

            Random locRandom = ThreadLocalRandom.current();

            try (IgniteDataStreamer<Integer, IndexedObject> dataLdr = ignite.dataStreamer(cacheName)) {
                for (int i = 0; i < KEYS_COUNT; ++i) {
                    if (i % (KEYS_COUNT / 100) == 0)
                        info("Loading " + i * 100 / KEYS_COUNT + "%");

                    dataLdr.addData(i, new IndexedObject(i));
                }
            }

            int size = ignite.cache(cacheName).size();

            for (int restartCnt = 0; restartCnt < 5; ++restartCnt) {
                stopGrid(1, true);

                info("Restart #" + restartCnt);
                U.sleep(500);

                ignite = startGrid(1);

                ignite.active(true);

                IgniteCache<Integer, IndexedObject> cache = ignite.cache(cacheName);

                assertEquals(size, cache.size());

                info("Restart #" + restartCnt);

                for (int i = 0; i < KEYS_COUNT / 100; ++i) {
                    assertNotNull(cache.get(locRandom.nextInt(KEYS_COUNT / 100)));

                    cache.put(locRandom.nextInt(KEYS_COUNT / 100), new IndexedObject(locRandom.nextInt(KEYS_COUNT / 100)));
                }

                cache.put(KEYS_COUNT + restartCnt, new IndexedObject(KEYS_COUNT + restartCnt));

                // Check recovery for partition meta pages.
                size = cache.size();
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testWalRecoveryWithDynamicCache() throws Exception {
        try {
            IgniteEx ignite = startGrid(1);

            ignite.active(true);

            CacheConfiguration<Integer, IndexedObject> dynCacheCfg = new CacheConfiguration<>();

            dynCacheCfg.setName("dyncache");
            dynCacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            dynCacheCfg.setRebalanceMode(CacheRebalanceMode.NONE);
            dynCacheCfg.setIndexedTypes(Integer.class, IndexedObject.class);
            dynCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
            dynCacheCfg.setAffinity(new RendezvousAffinityFunction(false, 64 * 4)); // 64 per node
            dynCacheCfg.setReadFromBackup(true);

            ignite.getOrCreateCache(dynCacheCfg);

            try (IgniteDataStreamer<Integer, IndexedObject> dataLdr = ignite.dataStreamer("dyncache")) {
                for (int i = 0; i < KEYS_COUNT; ++i) {
                    if (i % (KEYS_COUNT / 100) == 0)
                        info("Loading " + i * 100 / KEYS_COUNT + "%");

                    dataLdr.addData(i, new IndexedObject(i));
                }
            }

            for (int restartCnt = 0; restartCnt < 5; ++restartCnt) {
                stopGrid(1, true);

                info("Restart #" + restartCnt);
                U.sleep(500);

                ignite = startGrid(1);

                ignite.active(true);

                ThreadLocalRandom locRandom = ThreadLocalRandom.current();

                IgniteCache<Integer, IndexedObject> cache = ignite.getOrCreateCache(dynCacheCfg);

                for (int i = 0; i < KEYS_COUNT; ++i)
                    assertNotNull(cache.get(locRandom.nextInt(KEYS_COUNT)));
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testWalRecoveryWithDynamicCacheLargeObjects() throws Exception {
        try {
            IgniteEx ignite = startGrid(1);

            ignite.active(true);

            CacheConfiguration<Integer, IndexedObject> dynCacheCfg = new CacheConfiguration<>();

            dynCacheCfg.setName("dyncache");
            dynCacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            dynCacheCfg.setRebalanceMode(CacheRebalanceMode.NONE);
            dynCacheCfg.setIndexedTypes(Integer.class, IndexedObject.class);
            dynCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
            dynCacheCfg.setAffinity(new RendezvousAffinityFunction(false, 64 * 4)); // 64 per node
            dynCacheCfg.setReadFromBackup(true);

            ignite.getOrCreateCache(dynCacheCfg);

            try (IgniteDataStreamer<Integer, IndexedObject> dataLdr = ignite.dataStreamer("dyncache")) {
                for (int i = 0; i < LARGE_KEYS_COUNT; ++i) {
                    if (i % (LARGE_KEYS_COUNT / 100) == 0)
                        info("Loading " + i * 100 / LARGE_KEYS_COUNT + "%");

                    IndexedObject obj = new IndexedObject(i);

                    obj.payload = new byte[PAGE_SIZE + 2];

                    dataLdr.addData(i, obj);
                }
            }

            for (int restartCnt = 0; restartCnt < 5; ++restartCnt) {
                stopGrid(1, true);

                info("Restart #" + restartCnt);
                U.sleep(500);

                ignite = startGrid(1);

                ignite.active(true);

                ThreadLocalRandom locRandom = ThreadLocalRandom.current();

                IgniteCache<Integer, IndexedObject> cache = ignite.getOrCreateCache(dynCacheCfg);

                for (int i = 0; i < LARGE_KEYS_COUNT; ++i) {
                    IndexedObject val = cache.get(locRandom.nextInt(LARGE_KEYS_COUNT));

                    assertNotNull(val);

                    assertEquals(PAGE_SIZE + 2, val.payload.length);
                }
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    private static class IndexedObject {
        /** */
        @QuerySqlField(index = true)
        private int iVal;

        /** */
        @QuerySqlField(index = true)
        private String strVal0;

        /** */
        @QuerySqlField(index = true)
        private String strVal1;

        /** */
        private byte[] payload;

        /**
         * @param iVal Integer value.
         */
        private IndexedObject(int iVal) {
            this.iVal = iVal;

            strVal0 = "String value #0 " + iVal + " " + GridTestUtils.randomString(rnd, 256);
            strVal1 = GridTestUtils.randomString(rnd, 256);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            IndexedObject obj = (IndexedObject)o;

            if (iVal != obj.iVal)
                return false;

            if (strVal0 != null ? !strVal0.equals(obj.strVal0) : obj.strVal0 != null)
                return false;

            return strVal1 != null ? strVal1.equals(obj.strVal1) : obj.strVal1 == null;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = iVal;

            res = 31 * res + (strVal0 != null ? strVal0.hashCode() : 0);
            res = 31 * res + (strVal1 != null ? strVal1.hashCode() : 0);

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(IndexedObject.class, this);
        }
    }
}
