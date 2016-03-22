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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryObjectOffheapImpl;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class BinaryObjectOffHeapUnswapTemporaryTest extends GridCommonAbstractTest {
    /** */
    private CacheAtomicityMode atomicMode = CacheAtomicityMode.TRANSACTIONAL;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.setMarshaller(new BinaryMarshaller());

        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setAtomicityMode(atomicMode);
        cfg.setMemoryMode(CacheMemoryMode.OFFHEAP_TIERED);
        cfg.setBackups(1);
        cfg.setSwapEnabled(true);

        c.setCacheConfiguration(cfg);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(3);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testGet() throws Exception {
        IgniteCache<Integer, BinaryObject> keepBinaryCache = jcache(0).withKeepBinary();

        for (int key = 0; key < 100; key++) {
            BinaryObjectBuilder builder = ignite(0).binary().builder("SomeType");

            builder.setField("field1", key);
            builder.setField("field2", "name_" + key);

            keepBinaryCache.put(key, builder.build());
        }

        for (int key = 0; key < 100; key++) {
            try (Transaction tx = ignite(0).transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                BinaryObject val = keepBinaryCache.get(key);

                assertFalse(val instanceof BinaryObjectOffheapImpl);

                keepBinaryCache.put(key, val);

                tx.commit();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetRealObject() throws Exception {
        IgniteCache<Integer, BinaryObject> keepBinaryCache = jcache(0).withKeepBinary();

        for (int key = 0; key < 100; key++)
            jcache(0).put(key, new TestObject(key));

        for (int key = 0; key < 100; key++) {
            try (Transaction tx = ignite(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                BinaryObject val = keepBinaryCache.get(key);

                assertFalse(val instanceof BinaryObjectOffheapImpl);

                keepBinaryCache.put(key, val);

                tx.commit();
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAtomic() throws Exception {
        IgniteCache<Integer, BinaryObject> keepBinaryCache = jcache(0).withKeepBinary();

        for (int key = 0; key < 100; key++) {
            BinaryObjectBuilder builder = ignite(0).binary().builder("SomeType");
            builder.setField("field1", key);
            builder.setField("field2", "name_" + key);

            keepBinaryCache.put(key, builder.build());
        }

        for (int key = 0; key < 100; key++) {
            BinaryObject val = keepBinaryCache.get(key);

            assertFalse(val instanceof BinaryObjectOffheapImpl);

            keepBinaryCache.put(key, val);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAll() throws Exception {
        IgniteCache<Integer, BinaryObject> keepBinaryCache = jcache(0).withKeepBinary();

        Set<Integer> keys = new HashSet<>();

        for (int i = 0; i < 100; i++) {
            keys.add(i);

            BinaryObjectBuilder builder = ignite(0).binary().builder("SomeType");

            builder.setField("field1", i);
            builder.setField("field2", "name_" + i);

            keepBinaryCache.put(i, builder.build());
        }

        try (Transaction tx = ignite(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            Map<Integer, BinaryObject> vals = keepBinaryCache.getAll(keys);

            for (Map.Entry<Integer, BinaryObject> e : vals.entrySet()) {
                assertFalse("Key: " + e.getKey(), e.getValue() instanceof BinaryObjectOffheapImpl);

                keepBinaryCache.put(e.getKey(), e.getValue());
            }

            tx.commit();
        }
    }

    /**
     *
     */
    private static class TestObject {
        /** */
        String field;

        /** */
        int field2;

        /**
         * @param key Key.
         */
        TestObject(int key) {
            field = "str" + key;
            field2 = key;
        }
    }
}
