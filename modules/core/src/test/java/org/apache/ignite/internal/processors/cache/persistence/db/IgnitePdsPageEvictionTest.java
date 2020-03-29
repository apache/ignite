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

package org.apache.ignite.internal.processors.cache.persistence.db;

import java.io.Serializable;
import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.GridTestUtils.SF;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IgnitePdsPageEvictionTest extends GridCommonAbstractTest {
    /** Test entry count. */
    public static final int ENTRY_CNT = SF.applyLB(300_000, 100_000);

    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(50L * 1024 * 1024).setPersistenceEnabled(true))
            .setWalMode(WALMode.LOG_ONLY)
            .setPageSize(1024)
            .setConcurrencyLevel(Runtime.getRuntime().availableProcessors() * 4);

        cfg.setDataStorageConfiguration(memCfg);

        CacheConfiguration<DbKey, DbValue> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setRebalanceMode(CacheRebalanceMode.NONE);
        ccfg.setIndexedTypes(DbKey.class, DbValue.class);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));

        cfg.setCacheConfiguration(ccfg);

        cfg.setMarshaller(null);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();

        stopAllGrids();

        startGrids(1);
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
    public void testPageEvictionSql() throws Exception {
        IgniteEx ig = grid(0);

        ig.active(true);

        try (IgniteDataStreamer<DbKey, DbValue> streamer = ig.dataStreamer(CACHE_NAME)) {
            for (int i = 0; i < ENTRY_CNT; i++) {
                streamer.addData(new DbKey(i), new DbValue(i, "value-" + i, Long.MAX_VALUE - i));

                if (i > 0 && i % 10_000 == 0)
                    info("Done put: " + i);
            }
        }

        IgniteCache<DbKey, DbValue> cache = ignite(0).cache(CACHE_NAME);

        int i = 0;
        for (Cache.Entry<DbKey, DbValue> entry : cache.query(new ScanQuery<DbKey, DbValue>())) {
            assertEquals(Long.MAX_VALUE - entry.getKey().val, entry.getValue().lVal);

            if (i > 0 && i % 10_000 == 0)
                info("Done get: " + i);

            i++;
        }

        for (i = 0; i < ENTRY_CNT; i++) {
            List<List<?>> rows = cache.query(
                new SqlFieldsQuery("select lVal from DbValue where iVal=?").setArgs(i)
            ).getAll();

            assertEquals(1, rows.size());
            assertEquals(Long.MAX_VALUE - i, rows.get(0).get(0));

            if (i > 0 && i % 10_000 == 0)
                info("Done SQL query: " + i);
        }
    }

    /**
     *
     */
    private static class DbKey implements Serializable {
        /** */
        private int val;

        /**
         * @param val Value.
         */
        private DbKey(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || !(o instanceof DbKey))
                return false;

            DbKey key = (DbKey)o;

            return val == key.val;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val;
        }
    }

    /**
     *
     */
    private static class DbValue implements Serializable {
        /** */
        @QuerySqlField(index = true)
        private int iVal;

        /** */
        @QuerySqlField(index = true)
        private String sVal;

        /** */
        @QuerySqlField
        private long lVal;

        /**
         * @param iVal Integer value.
         * @param sVal String value.
         * @param lVal Long value.
         */
        private DbValue(int iVal, String sVal, long lVal) {
            this.iVal = iVal;
            this.sVal = sVal;
            this.lVal = lVal;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            DbValue dbVal = (DbValue)o;

            return iVal == dbVal.iVal && lVal == dbVal.lVal &&
                !(sVal != null ? !sVal.equals(dbVal.sVal) : dbVal.sVal != null);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = iVal;

            res = 31 * res + (sVal != null ? sVal.hashCode() : 0);
            res = 31 * res + (int)(lVal ^ (lVal >>> 32));

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(DbValue.class, this);
        }
    }
}
