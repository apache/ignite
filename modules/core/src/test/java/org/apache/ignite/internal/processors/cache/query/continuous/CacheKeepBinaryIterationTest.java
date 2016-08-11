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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.io.Serializable;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.PRIMARY;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_TIERED;
import static org.apache.ignite.cache.CacheMemoryMode.ONHEAP_TIERED;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class CacheKeepBinaryIterationTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES = 3;

    /** */
    private static final int KEYS = 1025;

    static {
        GridTestProperties.setProperty(GridTestProperties.MARSH_CLASS_NAME, BinaryMarshaller.class.getName());
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);
        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(getServerNodeCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicOnHeap() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            ATOMIC,
            ONHEAP_TIERED
        );

        doTestScanQuery(ccfg, true, true);
        doTestScanQuery(ccfg, true, false);
        doTestScanQuery(ccfg, false, true);
        doTestScanQuery(ccfg, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicOffHeap() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            ATOMIC,
            OFFHEAP_TIERED
        );

        doTestScanQuery(ccfg, true, true);
        doTestScanQuery(ccfg, true, false);
        doTestScanQuery(ccfg, false, true);
        doTestScanQuery(ccfg, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxOnHeap() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            TRANSACTIONAL,
            ONHEAP_TIERED
        );

        doTestScanQuery(ccfg, true, true);
        doTestScanQuery(ccfg, true, false);
        doTestScanQuery(ccfg, false, true);
        doTestScanQuery(ccfg, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxOffHeap() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            TRANSACTIONAL,
            OFFHEAP_TIERED
        );

        doTestScanQuery(ccfg, true, true);
        doTestScanQuery(ccfg, true, false);
        doTestScanQuery(ccfg, false, true);
        doTestScanQuery(ccfg, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicOnHeapLocalEntries() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            ATOMIC,
            ONHEAP_TIERED
        );

        doTestLocalEntries(ccfg, true, true);
        doTestLocalEntries(ccfg, true, false);
        doTestLocalEntries(ccfg, false, true);
        doTestLocalEntries(ccfg, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicOffHeapLocalEntries() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            ATOMIC,
            OFFHEAP_TIERED
        );

        doTestLocalEntries(ccfg, true, true);
        doTestLocalEntries(ccfg, true, false);
        doTestLocalEntries(ccfg, false, true);
        doTestLocalEntries(ccfg, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxOnHeapLocalEntries() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            TRANSACTIONAL,
            ONHEAP_TIERED
        );

        doTestLocalEntries(ccfg, true, true);
        doTestLocalEntries(ccfg, true, false);
        doTestLocalEntries(ccfg, false, true);
        doTestLocalEntries(ccfg, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxOffHeapLocalEntries() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            TRANSACTIONAL,
            OFFHEAP_TIERED
        );

        doTestLocalEntries(ccfg, true, true);
        doTestLocalEntries(ccfg, true, false);
        doTestLocalEntries(ccfg, false, true);
        doTestLocalEntries(ccfg, false, false);
    }

    /**
     * @param ccfg Cache configuration.
     */
    private void doTestScanQuery(final CacheConfiguration<Object, Object> ccfg, boolean keepBinary,
        boolean primitives) throws Exception {
        IgniteCache<Object, Object> cache = grid(0).createCache(ccfg);

        assertEquals(0, cache.size());

        try {
            for (int i = 0; i < KEYS; i++)
                if (primitives)
                    cache.put(i, i);
                else
                    cache.put(new QueryTestKey(i), new QueryTestValue(i));

            for (int i = 0; i < getServerNodeCount(); i++) {
                IgniteCache<Object, Object> cache0 = grid(i).cache(ccfg.getName());

                if (keepBinary)
                    cache0 = cache0.withKeepBinary();

                ScanQuery<Object, Object> qry = new ScanQuery<>();

                qry.setLocal(true);

                int size = 0;

                try (QueryCursor<Cache.Entry<Object, Object>> cur = cache0.query(qry)) {
                    for (Cache.Entry<Object, Object> e : cur) {
                        Object key = e.getKey();
                        Object val = e.getValue();

                        if (!primitives) {
                            assertTrue("Got unexpected object: " + key.getClass() + ", keepBinary: " + keepBinary,
                                keepBinary == key instanceof BinaryObject);
                            assertTrue("Got unexpected object: " + val.getClass() + ", keepBinary: " + keepBinary,
                                keepBinary == val instanceof BinaryObject);
                        }
                        else {
                            assertTrue("Got unexpected object: " + key.getClass() + ", keepBinary: " + keepBinary,
                                key instanceof Integer);
                            assertTrue("Got unexpected object: " + val.getClass() + ", keepBinary: " + keepBinary,
                                val instanceof Integer);
                        }

                        ++size;
                    }
                }

                assertTrue(size > 0);
            }
        }
        finally {
            if (ccfg.getEvictionPolicy() != null) { // TODO: IGNITE-3462. Fixes evictionPolicy issues at cache destroy.
                stopAllGrids();

                startGridsMultiThreaded(getServerNodeCount());
            }
            else
                grid(0).destroyCache(ccfg.getName());
        }
    }

    /**
     * @param ccfg Cache configuration.
     */
    private void doTestLocalEntries(final CacheConfiguration<Object, Object> ccfg,
        boolean keepBinary,
        boolean primitives) throws Exception {
        IgniteCache<Object, Object> cache = grid(0).createCache(ccfg);

        assertEquals(0, cache.size());

        try {
            for (int i = 0; i < KEYS; i++)
                if (primitives)
                    cache.put(i, i);
                else
                    cache.put(new QueryTestKey(i), new QueryTestValue(i));

            for (int i = 0; i < getServerNodeCount(); i++) {
                IgniteCache<Object, Object> cache0 = grid(i).cache(ccfg.getName());

                if (keepBinary)
                    cache0 = cache0.withKeepBinary();

                for (CachePeekMode mode : CachePeekMode.values()) {
                    int size = 0;

                    for (Cache.Entry<Object, Object> e : cache0.localEntries(mode)) {
                        Object key = e.getKey();
                        Object val = e.getValue();

                        if (!primitives) {
                            assertTrue("Got unexpected object: " + key.getClass() + ", keepBinary: " + keepBinary,
                                keepBinary == key instanceof BinaryObject);
                            assertTrue("Got unexpected object: " + key.getClass() + ", keepBinary: " + keepBinary,
                                keepBinary == val instanceof BinaryObject);
                        }
                        else {
                            assertTrue("Got unexpected object: " + key.getClass() + ", keepBinary: " + keepBinary,
                                key instanceof Integer);
                            assertTrue("Got unexpected object: " + key.getClass() + ", keepBinary: " + keepBinary,
                                val instanceof Integer);
                        }

                        ++size;
                    }

                    if (mode == CachePeekMode.ALL ||
                        mode == CachePeekMode.PRIMARY ||
                        mode == CachePeekMode.BACKUP ||
                        (mode == CachePeekMode.NEAR && i == 0 &&
                            ccfg.getMemoryMode() == CacheMemoryMode.ONHEAP_TIERED &&
                            ccfg.getNearConfiguration() != null) ||
                        (mode == CachePeekMode.ONHEAP && ccfg.getMemoryMode() == CacheMemoryMode.ONHEAP_TIERED) ||
                        (mode == CachePeekMode.OFFHEAP && ccfg.getMemoryMode() == CacheMemoryMode.OFFHEAP_TIERED) ||
                        (mode == CachePeekMode.SWAP && ccfg.isSwapEnabled()))
                        assertTrue("Zero result at mode: " + mode, size > 0);
                }
            }
        }
        finally {
            if (ccfg.getEvictionPolicy() != null) { // TODO: IGNITE-3462. Fixes evictionPolicy issues at cache destroy.
                stopAllGrids();

                startGridsMultiThreaded(getServerNodeCount());
            }
            else
                grid(0).destroyCache(ccfg.getName());
        }
    }

    /**
     * @return Count nodes.
     */
    protected int getServerNodeCount() {
        return NODES;
    }

    /**
     * @param cacheMode Cache mode.
     * @param backups Number of backups.
     * @param atomicityMode Cache atomicity mode.
     * @param memoryMode Cache memory mode.
     * @return Cache configuration.
     */
    protected CacheConfiguration<Object, Object> cacheConfiguration(
        CacheMode cacheMode,
        int backups,
        CacheAtomicityMode atomicityMode,
        CacheMemoryMode memoryMode) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>();

        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setCacheMode(cacheMode);
        ccfg.setMemoryMode(memoryMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setAtomicWriteOrderMode(PRIMARY);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        return ccfg;
    }

    /**
     *
     */
    public static class QueryTestKey implements Serializable {
        /** */
        private final Integer key;

        /**
         * @param key Key.
         */
        public QueryTestKey(Integer key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            QueryTestKey that = (QueryTestKey)o;

            return key.equals(that.key);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return key.hashCode();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(QueryTestKey.class, this);
        }
    }

    /**
     *
     */
    public static class QueryTestValue implements Serializable {
        /** */
        @GridToStringInclude
        protected final Integer val1;

        /** */
        @GridToStringInclude
        protected final String val2;

        /**
         * @param val Value.
         */
        public QueryTestValue(Integer val) {
            this.val1 = val;
            this.val2 = String.valueOf(val);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            QueryTestValue that = (QueryTestValue)o;

            return val1.equals(that.val1) && val2.equals(that.val2);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = val1.hashCode();

            res = 31 * res + val2.hashCode();

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(QueryTestValue.class, this);
        }
    }
}