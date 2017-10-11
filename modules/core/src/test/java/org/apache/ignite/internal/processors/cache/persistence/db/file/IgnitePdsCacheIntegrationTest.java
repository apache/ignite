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

package org.apache.ignite.internal.processors.cache.persistence.db.file;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 *
 */
public class IgnitePdsCacheIntegrationTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int GRID_CNT = 3;

    /** */
    private static final String CACHE_NAME = "cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        MemoryConfiguration dbCfg = new MemoryConfiguration();

        dbCfg.setConcurrencyLevel(Runtime.getRuntime().availableProcessors() * 4);

        MemoryPolicyConfiguration memPlcCfg = new MemoryPolicyConfiguration();

        memPlcCfg.setName("dfltMemPlc");
        memPlcCfg.setInitialSize(100 * 1024 * 1024);
        memPlcCfg.setMaxSize(100 * 1024 * 1024);

        dbCfg.setMemoryPolicies(memPlcCfg);
        dbCfg.setDefaultMemoryPolicyName("dfltMemPlc");

        cfg.setPersistentStoreConfiguration(
            new PersistentStoreConfiguration()
                .setWalMode(WALMode.LOG_ONLY)
        );

        cfg.setMemoryConfiguration(dbCfg);

        CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME);

        ccfg.setIndexedTypes(Integer.class, DbValue.class);

        ccfg.setRebalanceMode(CacheRebalanceMode.NONE);

        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));

        cfg.setCacheConfiguration(ccfg);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        cfg.setMarshaller(null);

        BinaryConfiguration bCfg = new BinaryConfiguration();

        bCfg.setCompactFooter(false);

        cfg.setBinaryConfiguration(bCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
    }

    /**
     * @throws Exception if failed.
     */
    public void testPutGetSimple() throws Exception {
        startGrids(GRID_CNT);

        try {
            IgniteEx ig = grid(0);

            ig.active(true);

            checkPutGetSql(ig, true);
        }
        finally {
            stopAllGrids();
        }

        info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");

        startGrids(GRID_CNT);

        try {
            IgniteEx ig = grid(0);

            ig.active(true);

            checkPutGetSql(ig, false);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception if failed.
     */
    public void testPutMultithreaded() throws Exception {
        startGrids(4);

        try {
            final IgniteEx grid = grid(0);

            grid.active(true);

            GridTestUtils.runMultiThreaded(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    for (int i = 0; i < 1000; i++)
                        grid.cache(CACHE_NAME).put(i, i);

                    return null;
                }
            }, 8, "updater");
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param ig Ignite instance.
     * @param write Write flag.
     */
    private void checkPutGetSql(Ignite ig, boolean write) {
        IgniteCache<Integer, DbValue> cache = ig.cache(CACHE_NAME);

        int entryCnt = 50_000;

        if (write) {
            try (IgniteDataStreamer<Object, Object> streamer = ig.dataStreamer(CACHE_NAME)) {
                streamer.allowOverwrite(true);

                for (int i = 0; i < entryCnt; i++)
                    streamer.addData(i, new DbValue(i, "value-" + i, i));
            }
        }

        for (int i = 0; i < GRID_CNT; i++) {
            IgniteEx ignite = grid(i);

            GridCacheAdapter<Object, Object> cache0 = ignite.context().cache().internalCache(CACHE_NAME);

            for (int k = 0; k < entryCnt; k++)
                assertNull(cache0.peekEx(i));

            assertEquals(entryCnt, ignite.cache(CACHE_NAME).size());
        }

        for (int i = 0; i < entryCnt; i++)
            assertEquals("i = " + i, new DbValue(i, "value-" + i, i), cache.get(i));

        List<List<?>> res = cache.query(new SqlFieldsQuery("select ival from dbvalue where ival < ? order by ival asc")
            .setArgs(10_000)).getAll();

        assertEquals(10_000, res.size());

        for (int i = 0; i < 10_000; i++) {
            assertEquals(1, res.get(i).size());
            assertEquals(i, res.get(i).get(0));
        }

        assertEquals(1, cache.query(new SqlFieldsQuery("select lval from dbvalue where ival = 7899")).getAll().size());
        assertEquals(5000, cache.query(new SqlFieldsQuery("select lval from dbvalue where ival >= 5000 and ival < 10000"))
            .getAll().size());

        for (int i = 0; i < 10_000; i++)
            assertEquals(new DbValue(i, "value-" + i, i), cache.get(i));
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
