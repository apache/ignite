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

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.PRIMARY;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_TIERED;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_VALUES;
import static org.apache.ignite.cache.CacheMemoryMode.ONHEAP_TIERED;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public abstract class CacheEnumOperationsAbstractTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setClientMode(client);

        return cfg;
    }

    /**
     * @return Number of nodes.
     */
    protected abstract boolean singleNode();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        if (!singleNode()) {
            startGridsMultiThreaded(4);

            client = true;

            startGridsMultiThreaded(4, 2);
        }
        else
            startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomic() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED, 1, ATOMIC, ONHEAP_TIERED);

        enumOperations(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicOffheapValues() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED, 1, ATOMIC, OFFHEAP_VALUES);

        enumOperations(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicOffheapTiered() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED, 1, ATOMIC, OFFHEAP_TIERED);

        enumOperations(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTx() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED, 1, ATOMIC, ONHEAP_TIERED);

        enumOperations(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxOffheapValues() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED, 1, ATOMIC, OFFHEAP_VALUES);

        enumOperations(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxOffheapTiered() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED, 1, ATOMIC, OFFHEAP_TIERED);

        enumOperations(ccfg);
    }

    /**
     * @param ccfg Cache configuration.
     */
    private void enumOperations(CacheConfiguration<Object, Object> ccfg) {
        ignite(0).createCache(ccfg);

        try {
            int key = 0;

            int nodes;

            if (!singleNode()) {
                nodes = 6;

                ignite(nodes - 1).createNearCache(ccfg.getName(), new NearCacheConfiguration<>());
            }
            else
                nodes = 1;

            for (int i = 0; i < nodes; i++) {
                IgniteCache<Object, Object> cache = ignite(i).cache(ccfg.getName());

                for (int j = 0; j < 100; j++)
                    enumOperations(cache, key++);
            }
        }
        finally {
            ignite(0).destroyCache(ccfg.getName());
        }
    }

    /**
     * @param cache Cache.
     * @param key Key.
     */
    private void enumOperations(IgniteCache<Object, Object> cache, int key) {
        assertNull(cache.get(key));

        assertFalse(cache.replace(key, TestEnum.VAL1));

        assertTrue(cache.putIfAbsent(key, TestEnum.VAL1));

        assertEquals(TestEnum.VAL1, cache.get(key));

        assertFalse(cache.putIfAbsent(key, TestEnum.VAL2));

        assertEquals(TestEnum.VAL1, cache.get(key));

        assertTrue(cache.replace(key, TestEnum.VAL2));

        assertEquals(TestEnum.VAL2, cache.get(key));

        assertFalse(cache.replace(key, TestEnum.VAL1, TestEnum.VAL3));

        assertEquals(TestEnum.VAL2, cache.get(key));

        assertTrue(cache.replace(key, TestEnum.VAL2, TestEnum.VAL3));

        assertEquals(TestEnum.VAL3, cache.get(key));

        assertEquals(TestEnum.VAL3, cache.getAndPut(key, TestEnum.VAL1));

        assertEquals(TestEnum.VAL1, cache.get(key));

        assertEquals(TestEnum.VAL1, cache.invoke(key, new EnumProcessor(TestEnum.VAL2, TestEnum.VAL1)));

        assertEquals(TestEnum.VAL2, cache.get(key));

        assertEquals(TestEnum.VAL2, cache.getAndReplace(key, TestEnum.VAL3));

        assertEquals(TestEnum.VAL3, cache.get(key));

        assertEquals(TestEnum.VAL3, cache.getAndPutIfAbsent(key, TestEnum.VAL1));

        assertEquals(TestEnum.VAL3, cache.get(key));

        cache.put(key, TestEnum.VAL1);

        assertEquals(TestEnum.VAL1, cache.get(key));

        assertEquals(TestEnum.VAL1, cache.getAndRemove(key));

        assertNull(cache.get(key));

        assertFalse(cache.replace(key, TestEnum.VAL2, TestEnum.VAL3));

        assertNull(cache.getAndPutIfAbsent(key, TestEnum.VAL1));

        assertEquals(TestEnum.VAL1, cache.get(key));
    }

    /**
     *
     * @param cacheMode Cache mode.
     * @param backups Number of backups.
     * @param atomicityMode Cache atomicity mode.
     * @param memoryMode Cache memory mode.
     * @return Cache configuration.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration(
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

        if (memoryMode == OFFHEAP_TIERED)
            ccfg.setOffHeapMaxMemory(0);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        return ccfg;
    }

    /**
     *
     */
    public enum TestEnum {
        /** */
        VAL1,
        /** */
        VAL2,
        /** */
        VAL3
    }

    /**
     *
     */
    static class EnumProcessor implements EntryProcessor<Object, Object, Object> {
        /** */
        private TestEnum newVal;

        /** */
        private TestEnum expOldVal;

        /**
         * @param newVal New value.
         * @param expOldVal Expected old value.
         */
        public EnumProcessor(TestEnum newVal, TestEnum expOldVal) {
            this.newVal = newVal;
            this.expOldVal = expOldVal;
        }

        /** {@inheritDoc} */
        @Override public Object process(MutableEntry<Object, Object> entry, Object... args) {
            TestEnum val = (TestEnum)entry.getValue();

            assertEquals(expOldVal, val);

            entry.setValue(newVal);

            return val;
        }
    }
}
