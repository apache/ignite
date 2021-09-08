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

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IgniteWalRecoveryPPCTest extends GridCommonAbstractTest {
    /** */
    private boolean fork;

    /** */
    public static final String CACHE_NAME_1 = "cache_1";

    /** */
    public static final String CACHE_NAME_2 = "cache_2";

    /** */
    public static final String MEM_PLC_NO_PDS = "mem_plc_2";

    /** */
    private int walSegmentSize;

    /** Logger only. */
    private boolean logOnly;

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return fork;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration<Integer, IndexedObject> ccfg = new CacheConfiguration<>(CACHE_NAME_1);

        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));

        cfg.setCacheConfiguration(ccfg);

        CacheConfiguration<Integer, IndexedObject> ccfg2 = new CacheConfiguration<>(CACHE_NAME_2);

        ccfg2.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg2.setRebalanceMode(CacheRebalanceMode.SYNC);
        ccfg2.setAffinity(new RendezvousAffinityFunction(false, 32));
        ccfg2.setDataRegionName(MEM_PLC_NO_PDS);

        cfg.setCacheConfiguration(ccfg, ccfg2);

        DataStorageConfiguration dbCfg = new DataStorageConfiguration();
        dbCfg.setPageSize(4 * 1024);

        DataRegionConfiguration memPlcCfg = new DataRegionConfiguration();
        memPlcCfg.setInitialSize(256L * 1024 * 1024);
        memPlcCfg.setMaxSize(256L * 1024 * 1024);
        memPlcCfg.setPersistenceEnabled(true);

        dbCfg.setDefaultDataRegionConfiguration(memPlcCfg);

        DataRegionConfiguration memPlcCfg2 = new DataRegionConfiguration();
        memPlcCfg2.setName(MEM_PLC_NO_PDS);
        memPlcCfg2.setInitialSize(256L * 1024 * 1024);
        memPlcCfg2.setMaxSize(256L * 1024 * 1024);
        memPlcCfg2.setPersistenceEnabled(false);

        dbCfg.setDataRegionConfigurations(memPlcCfg2);

        dbCfg.setWalRecordIteratorBufferSize(1024 * 1024);

        dbCfg.setWalHistorySize(2);

        dbCfg.setWalMode(WALMode.LOG_ONLY);

        if (walSegmentSize != 0)
            dbCfg.setWalSegmentSize(walSegmentSize);

        cfg.setDataStorageConfiguration(dbCfg);

        cfg.setMarshaller(null);

        BinaryConfiguration binCfg = new BinaryConfiguration();

        binCfg.setCompactFooter(false);

        cfg.setBinaryConfiguration(binCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testWalSimple() throws Exception {
        try {
            IgniteEx ignite = startGrid(1);

            ignite.active(true);

            IgniteCache<Object, Object> cache1 = ignite.cache(CACHE_NAME_1);
            IgniteCache<Object, Object> cache2 = ignite.cache(CACHE_NAME_2);

            info(" --> step1");

            for (int i = 0; i < 10_000; i += 2) {
                cache1.put(i, new IndexedObject(i));
                cache2.put(i, new IndexedObject(i + 1));
            }

            info(" --> step2");

            for (int i = 0; i < 10_000; i += 3) {
                cache1.put(i, new IndexedObject(i * 2));
                cache2.put(i, new IndexedObject(i * 2 + 1));
            }

            info(" --> step3");

            for (int i = 0; i < 10_000; i += 7) {
                cache1.put(i, new IndexedObject(i * 3));
                cache2.put(i, new IndexedObject(i * 3 + 1));
            }

            info(" --> check1");

            // Check.
            for (int i = 0; i < 10_000; i++) {
                IndexedObject o;
                IndexedObject o1;

                if (i % 7 == 0) {
                    o = new IndexedObject(i * 3);
                    o1 = new IndexedObject(i * 3 + 1);
                }
                else if (i % 3 == 0) {
                    o = new IndexedObject(i * 2);
                    o1 = new IndexedObject(i * 2 + 1);
                }
                else if (i % 2 == 0) {
                    o = new IndexedObject(i);
                    o1 = new IndexedObject(i + 1);
                }
                else {
                    o = null;
                    o1 = null;
                }

                assertEquals(o, cache1.get(i));
                assertEquals(o1, cache2.get(i));
            }

            stopGrid(1);

            ignite = startGrid(1);

            ignite.active(true);

            cache1 = ignite.cache(CACHE_NAME_1);
            cache2 = ignite.cache(CACHE_NAME_2);

            info(" --> check2");

            // Check.
            for (int i = 0; i < 10_000; i++) {
                IndexedObject o;

                if (i % 7 == 0)
                    o = new IndexedObject(i * 3);
                else if (i % 3 == 0)
                    o = new IndexedObject(i * 2);
                else if (i % 2 == 0)
                    o = new IndexedObject(i);
                else
                    o = null;

                assertEquals(o, cache1.get(i));
                assertEquals(null, cache2.get(i));
            }

            info(" --> ok");
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    @Test
    public void testDynamicallyStartedNonPersistentCache() throws Exception {
        try {
            IgniteEx ignite = startGrid(1);

            ignite.active(true);

            IgniteCache<Integer, Object> dynamicPersistent = ignite.getOrCreateCache(
                new CacheConfiguration<Integer, Object>()
                    .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                    .setRebalanceMode(CacheRebalanceMode.SYNC)
                    .setName("dynamicPersistent")
                    .setAffinity(new RendezvousAffinityFunction(false, 32)));

            IgniteCache<Integer, Object> dynamicVolatile = ignite.getOrCreateCache(
                new CacheConfiguration<Integer, Object>()
                    .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                    .setRebalanceMode(CacheRebalanceMode.SYNC)
                    .setDataRegionName(MEM_PLC_NO_PDS)
                    .setName("dynamicVolatile")
                    .setAffinity(new RendezvousAffinityFunction(false, 32)));

            for (int i = 0; i < 10_000; i++) {
                dynamicPersistent.put(i, new IndexedObject(i));
                dynamicVolatile.put(i, new IndexedObject(i + 1));
            }

            stopGrid(1);

            ignite = startGrid(1);

            ignite.active(true);

            dynamicPersistent = ignite.cache("dynamicPersistent");
            dynamicVolatile = ignite.cache("dynamicVolatile");

            for (int i = 0; i < 10_000; i++)
                assertEquals(new IndexedObject(i), dynamicPersistent.get(i));

            assertNull(dynamicVolatile);

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

        /**
         * @param iVal Integer value.
         */
        private IndexedObject(int iVal) {
            this.iVal = iVal;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof IndexedObject))
                return false;

            IndexedObject that = (IndexedObject)o;

            return iVal == that.iVal;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return iVal;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(IndexedObject.class, this);
        }
    }
}
