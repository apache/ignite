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

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IgniteCacheBinaryEntryProcessorSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int SRV_CNT = 4;

    /** */
    private static final int NODES = 5;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setMarshaller(null);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(NODES - 1);
        startClientGrid(SRV_CNT);
    }

    /**
     * @param cacheMode Cache mode.
     * @param atomicityMode Atomicity mode.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, TestValue> cacheConfiguration(CacheMode cacheMode, CacheAtomicityMode atomicityMode) {
        CacheConfiguration<Integer, TestValue> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(cacheMode);
        ccfg.setAtomicityMode(atomicityMode);

        ccfg.setBackups(1);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionedTransactional() throws Exception {
        checkInvokeBinaryObject(CacheMode.PARTITIONED, CacheAtomicityMode.TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReplicatedTransactional() throws Exception {
        checkInvokeBinaryObject(CacheMode.REPLICATED, CacheAtomicityMode.TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPartitionedAtomic() throws Exception {
        checkInvokeBinaryObject(CacheMode.PARTITIONED, CacheAtomicityMode.TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReplicatedAtomic() throws Exception {
        checkInvokeBinaryObject(CacheMode.REPLICATED, CacheAtomicityMode.TRANSACTIONAL);
    }

    /**
     * @param cacheMode Cache mode to test.
     * @param atomicityMode Atomicity mode to test.
     * @throws Exception
     */
    private void checkInvokeBinaryObject(CacheMode cacheMode, CacheAtomicityMode atomicityMode) throws Exception {
        Ignite client = ignite(SRV_CNT);

        IgniteCache<Integer, TestValue> clientCache = client.createCache(cacheConfiguration(cacheMode, atomicityMode));

        try {
            IgniteBinary binary = client.binary();

            for (int i = 0; i < 100; i++) {
                clientCache.put(i, new TestValue(i, "value-" + i));

                BinaryObjectBuilder bldr = binary.builder("NoClass");

                bldr.setField("val", i);
                bldr.setField("strVal", "value-" + i);

                clientCache.withKeepBinary().put(-(i + 1), bldr.build());
            }

            IgniteCache<Integer, BinaryObject> binaryClientCache = clientCache.withKeepBinary();

            for (int i = 0; i < 100; i++) {
                binaryClientCache.invoke(i, new TestEntryProcessor());
                binaryClientCache.invoke(-(i + 1), new TestEntryProcessor());
            }

            for (int g = 0; g < NODES; g++) {
                IgniteCache<Integer, TestValue> nodeCache = ignite(g).cache(DEFAULT_CACHE_NAME);
                IgniteCache<Integer, BinaryObject> nodeBinaryCache = nodeCache.withKeepBinary();

                for (int i = 0; i < 100; i++) {
                    TestValue updated = nodeCache.get(i);

                    assertEquals((Integer)(i + 1), updated.value());
                    assertEquals("updated-" + i, updated.stringValue());

                    BinaryObject updatedBinary = nodeBinaryCache.get(i);
                    assertEquals(new Integer(i + 1), updatedBinary.field("val"));
                    assertEquals("updated-" + i, updatedBinary.field("strVal"));

                    updatedBinary = nodeBinaryCache.get(-(i + 1));
                    assertEquals(new Integer(i + 1), updatedBinary.field("val"));
                    assertEquals("updated-" + i, updatedBinary.field("strVal"));
                }
            }
        }
        finally {
            client.destroyCache(DEFAULT_CACHE_NAME);
        }
    }

    /**
     *
     */
    private static class TestEntryProcessor implements CacheEntryProcessor<Integer, BinaryObject, Void> {
        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<Integer, BinaryObject> entry, Object... arguments)
            throws EntryProcessorException {
            BinaryObjectBuilder bldr = entry.getValue().toBuilder();

            Integer val = bldr.<Integer>getField("val");

            bldr.setField("val", val + 1);
            bldr.setField("strVal", "updated-" + val);

            entry.setValue(bldr.build());

            return null;
        }
    }

    /**
     *
     */
    static class TestValue {
        /** */
        private Integer val;

        /** */
        private String strVal;

        /**
         * @param val Value.
         */
        public TestValue(Integer val, String strVal) {
            this.val = val;
            this.strVal = strVal;
        }

        /**
         * @return Value.
         */
        public Integer value() {
            return val;
        }

        /**
         * @return String value.
         */
        public String stringValue() {
            return strVal;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestValue testVal = (TestValue) o;

            return val.equals(testVal.val);

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val.hashCode();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestValue.class, this);
        }
    }
}
