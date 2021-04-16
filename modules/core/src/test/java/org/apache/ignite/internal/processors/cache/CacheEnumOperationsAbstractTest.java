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
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.binary.BinaryEnumObjectImpl;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public abstract class CacheEnumOperationsAbstractTest extends GridCommonAbstractTest {
    /**
     * @return Number of nodes.
     */
    protected abstract boolean singleNode();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        if (!singleNode()) {
            startGridsMultiThreaded(4);
            startClientGridsMultiThreaded(4, 2);
        }
        else
            startGrid(0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAtomic() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED, 1, ATOMIC);

        enumOperations(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTx() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED, 1, TRANSACTIONAL);

        enumOperations(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMvccTx() throws Exception {
        Assume.assumeTrue("https://issues.apache.org/jira/browse/IGNITE-7187", singleNode());

        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED, 1, TRANSACTIONAL_SNAPSHOT);

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
        assertBinaryEnum(cache, key, TestEnum.VAL1);

        assertFalse(cache.putIfAbsent(key, TestEnum.VAL2));

        assertEquals(TestEnum.VAL1, cache.get(key));
        assertBinaryEnum(cache, key, TestEnum.VAL1);

        assertTrue(cache.replace(key, TestEnum.VAL2));

        assertEquals(TestEnum.VAL2, cache.get(key));
        assertBinaryEnum(cache, key, TestEnum.VAL2);

        assertFalse(cache.replace(key, TestEnum.VAL1, TestEnum.VAL3));

        assertEquals(TestEnum.VAL2, cache.get(key));
        assertBinaryEnum(cache, key, TestEnum.VAL2);

        assertTrue(cache.replace(key, TestEnum.VAL2, TestEnum.VAL3));

        assertEquals(TestEnum.VAL3, cache.get(key));
        assertBinaryEnum(cache, key, TestEnum.VAL3);

        assertEquals(TestEnum.VAL3, cache.getAndPut(key, TestEnum.VAL1));

        assertEquals(TestEnum.VAL1, cache.get(key));
        assertBinaryEnum(cache, key, TestEnum.VAL1);

        assertEquals(TestEnum.VAL1, cache.invoke(key, new EnumProcessor(TestEnum.VAL2, TestEnum.VAL1)));

        assertEquals(TestEnum.VAL2, cache.get(key));
        assertBinaryEnum(cache, key, TestEnum.VAL2);

        assertEquals(TestEnum.VAL2, cache.getAndReplace(key, TestEnum.VAL3));

        assertEquals(TestEnum.VAL3, cache.get(key));
        assertBinaryEnum(cache, key, TestEnum.VAL3);

        assertEquals(TestEnum.VAL3, cache.getAndPutIfAbsent(key, TestEnum.VAL1));

        assertEquals(TestEnum.VAL3, cache.get(key));
        assertBinaryEnum(cache, key, TestEnum.VAL3);

        cache.put(key, TestEnum.VAL1);

        assertEquals(TestEnum.VAL1, cache.get(key));
        assertBinaryEnum(cache, key, TestEnum.VAL1);

        assertEquals(TestEnum.VAL1, cache.getAndRemove(key));

        assertNull(cache.get(key));

        assertFalse(cache.replace(key, TestEnum.VAL2, TestEnum.VAL3));

        assertNull(cache.getAndPutIfAbsent(key, TestEnum.VAL1));

        assertEquals(TestEnum.VAL1, cache.get(key));
        assertBinaryEnum(cache, key, TestEnum.VAL1);
    }

    /**
     * @param cache Cache.
     * @param key Key.
     * @param expVal Expected value.
     */
    private static void assertBinaryEnum(IgniteCache<Object, Object> cache, int key, TestEnum expVal) {
        Marshaller marsh = ((IgniteCacheProxy)cache).context().marshaller();

        if (marsh instanceof BinaryMarshaller) {
            BinaryObject enumObj = (BinaryObject)cache.withKeepBinary().get(key);

            assertEquals(expVal.ordinal(), enumObj.enumOrdinal());
            assertTrue(enumObj.type().isEnum());
            assertTrue(enumObj instanceof BinaryEnumObjectImpl);
        }
    }

    /**
     *
     * @param cacheMode Cache mode.
     * @param backups Number of backups.
     * @param atomicityMode Cache atomicity mode.
     * @return Cache configuration.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration(
        CacheMode cacheMode,
        int backups,
        CacheAtomicityMode atomicityMode) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setCacheMode(cacheMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

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
