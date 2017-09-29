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

package org.apache.ignite.internal.processors.database;

import java.io.Serializable;
import java.util.Arrays;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 *
 */
public abstract class IgniteDbAbstractTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /**
     * @return Node count.
     */
    protected abstract int gridCount();

    /**
     * @return {@code True} if indexing is enabled.
     */
    protected abstract boolean indexingEnabled();

    /** */
    protected boolean client;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        MemoryConfiguration dbCfg = new MemoryConfiguration();

        if (client)
            cfg.setClientMode(true);

        dbCfg.setConcurrencyLevel(Runtime.getRuntime().availableProcessors() * 4);

        if (isLargePage())
            dbCfg.setPageSize(16 * 1024);
        else
            dbCfg.setPageSize(1024);

        configure(dbCfg);

        cfg.setMemoryConfiguration(dbCfg);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        if (indexingEnabled())
            ccfg.setIndexedTypes(Integer.class, DbValue.class);

        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setRebalanceMode(SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));

        CacheConfiguration ccfg2 = new CacheConfiguration("non-primitive");

        if (indexingEnabled())
            ccfg2.setIndexedTypes(DbKey.class, DbValue.class);

        ccfg2.setAtomicityMode(TRANSACTIONAL);
        ccfg2.setWriteSynchronizationMode(FULL_SYNC);
        ccfg2.setRebalanceMode(SYNC);
        ccfg2.setAffinity(new RendezvousAffinityFunction(false, 32));

        CacheConfiguration ccfg3 = new CacheConfiguration("large");

        if (indexingEnabled())
            ccfg3.setIndexedTypes(Integer.class, LargeDbValue.class);

        ccfg3.setAtomicityMode(TRANSACTIONAL);
        ccfg3.setWriteSynchronizationMode(FULL_SYNC);
        ccfg3.setRebalanceMode(SYNC);
        ccfg3.setAffinity(new RendezvousAffinityFunction(false, 32));

        CacheConfiguration ccfg4 = new CacheConfiguration("tiny");

        ccfg4.setAtomicityMode(TRANSACTIONAL);
        ccfg4.setWriteSynchronizationMode(FULL_SYNC);
        ccfg4.setRebalanceMode(SYNC);
        ccfg4.setAffinity(new RendezvousAffinityFunction(1, null));

        CacheConfiguration ccfg5 = new CacheConfiguration("atomic");

        if (indexingEnabled())
            ccfg5.setIndexedTypes(DbKey.class, DbValue.class);

        ccfg5.setAtomicityMode(ATOMIC);
        ccfg5.setWriteSynchronizationMode(FULL_SYNC);
        ccfg5.setRebalanceMode(SYNC);
        ccfg5.setAffinity(new RendezvousAffinityFunction(false, 32));

        if (!client)
            cfg.setCacheConfiguration(ccfg, ccfg2, ccfg3, ccfg4, ccfg5);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);
        cfg.setMarshaller(null);

        configure(cfg);

        return cfg;
    }

    /**
     * @param cfg IgniteConfiguration.
     */
    protected void configure(IgniteConfiguration cfg){
        // No-op.
    }

    /**
     * @param mCfg MemoryConfiguration.
     */
    protected void configure(MemoryConfiguration mCfg){
        // No-op.
    }

    /**
     * @return {@code True} if cache operations should be called from client node with near cache.
     */
    protected boolean withClientNearCache() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));

        startGrids(gridCount());

        if (withClientNearCache()) {
            client = true;

            startGrid(gridCount());

            client = false;
        }

        assert gridCount() > 0;

        grid(0).active(true);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        BPlusTree.rnd = null;

        stopAllGrids();

        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
    }

    /**
     * @return {@code True} if use large page.
     */
    protected boolean isLargePage() {
        return false;
    }

    /**
     *
     */
    static class DbKey implements Serializable {
        /** */
        int val;

        /**
         * @param val Value.
         */
        DbKey(int val) {
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
    static class LargeDbKey implements Serializable {
        /** */
        int val;

        /** */
        byte[] data;

        /**
         * @param val Value.
         * @param size Key payload size.
         */
        LargeDbKey(int val, int size) {
            this.val = val;

            data = new byte[size];

            Arrays.fill(data, (byte)val);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || !(o instanceof LargeDbKey))
                return false;

            LargeDbKey key = (LargeDbKey)o;

            return val == key.val && Arrays.equals(data, key.data);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val + Arrays.hashCode(data);
        }
    }

    /**
     *
     */
    static class DbValue implements Serializable {
        /** */
        @QuerySqlField(index = true)
        int iVal;

        /** */
        @QuerySqlField(index = true)
        String sVal;

        /** */
        @QuerySqlField
        long lVal;

        /**
         * @param iVal Integer value.
         * @param sVal String value.
         * @param lVal Long value.
         */
        DbValue(int iVal, String sVal, long lVal) {
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

    /**
     *
     */
    static class LargeDbValue {
        /** */
        @QuerySqlField(index = true)
        String str1;

        /** */
        @QuerySqlField(index = true)
        String str2;

        /** */
        int[] arr;

        /**
         * @param str1 String 1.
         * @param str2 String 2.
         * @param arr Big array.
         */
        LargeDbValue(final String str1, final String str2, final int[] arr) {
            this.str1 = str1;
            this.str2 = str2;
            this.arr = arr;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final LargeDbValue that = (LargeDbValue) o;

            if (str1 != null ? !str1.equals(that.str1) : that.str1 != null) return false;
            if (str2 != null ? !str2.equals(that.str2) : that.str2 != null) return false;

            return Arrays.equals(arr, that.arr);

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = str1 != null ? str1.hashCode() : 0;

            res = 31 * res + (str2 != null ? str2.hashCode() : 0);
            res = 31 * res + Arrays.hashCode(arr);

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(LargeDbValue.class, this);
        }
    }
}
