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

package org.apache.ignite.failure;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

/**
 * IgniteOutOfMemoryError failure handler test.
 */
public class IoomFailureHandlerTest extends AbstractFailureHandlerTest {
    /** Offheap size for memory policy. */
    private static final int SIZE = 10 * 1024 * 1024;

    /** Page size. */
    static final int PAGE_SIZE = 2048;

    /** Number of entries. */
    static final int ENTRIES = 10_000;

    /** PDS enabled. */
    private boolean pds;

    /** MVCC enabled. */
    private boolean mvcc;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        DataRegionConfiguration dfltPlcCfg = new DataRegionConfiguration();

        dfltPlcCfg.setName("dfltPlc");
        dfltPlcCfg.setInitialSize(SIZE);
        dfltPlcCfg.setMaxSize(SIZE);

        if (pds) {
            // We need longer failure detection timeout for PDS enabled mode or checkpoint write lock can block tx
            // checkpoint read lock for too long causing FH triggering on slow hardware.
            cfg.setFailureDetectionTimeout(30_000);

            dfltPlcCfg.setPersistenceEnabled(true);
        }

        dsCfg.setDefaultDataRegionConfiguration(dfltPlcCfg);
        dsCfg.setPageSize(PAGE_SIZE);

        cfg.setDataStorageConfiguration(dsCfg);

        CacheConfiguration<?, ?> ccfg = new CacheConfiguration<>()
            .setName(DEFAULT_CACHE_NAME)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(0)
            .setAtomicityMode(mvcc ? CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT : CacheAtomicityMode.TRANSACTIONAL);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanPersistenceDir();
    }

    /**
     * Test IgniteOutOfMemoryException handling with no store.
     */
    @Test
    public void testIoomErrorNoStoreHandling() throws Exception {
        testIoomErrorHandling(false, false);
    }

    /**
     * Test IgniteOutOfMemoryException handling with PDS.
     */
    @Test
    public void testIoomErrorPdsHandling() throws Exception {
        testIoomErrorHandling(true, false);
    }

    /**
     * Test IgniteOutOfMemoryException handling with no store.
     */
    @Test
    public void testIoomErrorMvccNoStoreHandling() throws Exception {
        testIoomErrorHandling(false, true);
    }

    /**
     * Test IgniteOutOfMemoryException handling with PDS.
     */
    @Test
    public void testIoomErrorMvccPdsHandling() throws Exception {
        testIoomErrorHandling(true, true);
    }

    /**
     * Test IOOME handling.
     */
    private void testIoomErrorHandling(boolean pds, boolean mvcc) throws Exception {
        this.pds = pds;
        this.mvcc = mvcc;

        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        try {
            if (pds)
                ignite0.cluster().active(true);

            IgniteCache<Integer, Object> cache0 = ignite0.getOrCreateCache(DEFAULT_CACHE_NAME);
            IgniteCache<Integer, Object> cache1 = ignite1.getOrCreateCache(DEFAULT_CACHE_NAME);

            awaitPartitionMapExchange();

            try (Transaction tx = ignite0.transactions().txStart()) {
                for (Integer i : primaryKeys(cache1, ENTRIES))
                    cache0.put(i, new byte[PAGE_SIZE / 3 * 2]);

                tx.commit();
            }
            catch (Throwable ignore) {
                // Expected.
            }

            assertFalse(dummyFailureHandler(ignite0).failure());

            if (mvcc && pds)
                assertFalse(dummyFailureHandler(ignite1).failure());
            else {
                assertTrue(dummyFailureHandler(ignite1).failure());
                assertTrue(X.hasCause(dummyFailureHandler(ignite1).failureContext().error(), IgniteOutOfMemoryException.class));
            }
        }
        finally {
            stopGrid(1);
            stopGrid(0);
        }
    }
}
