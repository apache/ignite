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

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryObjectOffheapImpl;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.swapspace.file.FileSwapSpaceSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_TIERED;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_VALUES;

/**
 *
 */
public class BinaryObjectOffHeapUnswapTemporaryTest extends GridCommonAbstractTest {
    /** */
    private static final int CNT = 20;

    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** */
    @SuppressWarnings("serial")
    private static final CacheEntryProcessor PROC = new CacheEntryProcessor() {
        @Override public Object process(MutableEntry entry, Object... arguments) throws EntryProcessorException {
            return entry.getValue();
        }
    };

    /** */
    private CacheAtomicityMode atomicityMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.setMarshaller(new BinaryMarshaller());
        c.setSwapSpaceSpi(new FileSwapSpaceSpi());

        return c;
    }

    /**
     * @param atomicityMode Atomicity mode.
     * @param memoryMode Memory mode.
     * @return Cache configuration.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration(CacheAtomicityMode atomicityMode,
        CacheMemoryMode memoryMode) {
        this.atomicityMode = atomicityMode;

        CacheConfiguration<Object, Object>  cfg = new CacheConfiguration<>();

        cfg.setName(CACHE_NAME);
        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setAtomicityMode(atomicityMode);
        cfg.setMemoryMode(memoryMode);
        cfg.setBackups(1);
        cfg.setSwapEnabled(true);

        return cfg;
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
    public void testTxOffheapTiered() throws Exception {
        ignite(0).getOrCreateCache(cacheConfiguration(TRANSACTIONAL, OFFHEAP_TIERED));

        try {
            doTest();
        }
        finally {
            ignite(0).destroyCache(null);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxOffheapValues() throws Exception {
        ignite(0).getOrCreateCache(cacheConfiguration(TRANSACTIONAL, OFFHEAP_VALUES));

        try {
            doTest();
        }
        finally {
            ignite(0).destroyCache(null);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicOffheapTiered() throws Exception {
        ignite(0).getOrCreateCache(cacheConfiguration(ATOMIC, OFFHEAP_TIERED));

        try {
            doTest();
        }
        finally {
            ignite(0).destroyCache(null);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicOffheapValues() throws Exception {
        ignite(0).getOrCreateCache(cacheConfiguration(ATOMIC, OFFHEAP_VALUES));

        try {
            doTest();
        }
        finally {
            ignite(0).destroyCache(null);
        }
    }

    /**
     *
     */
    @SuppressWarnings("serial")
    private void doTest() {
        final IgniteCache<Integer, BinaryObject> cache = jcache(0, CACHE_NAME).withKeepBinary();

        for (int key = 0; key < CNT; key++)
            jcache(0, CACHE_NAME).put(key, new TestObject(key));

        for (int key = CNT; key < 2 * CNT; key++) {
            BinaryObjectBuilder builder = ignite(0).binary().builder("SomeType");
            builder.setField("field1", key);
            builder.setField("field2", "name_" + key);

            cache.put(key, builder.build());
        }

        Set<Integer> keys = new LinkedHashSet<>();

        for (int i = 0; i < 2 * CNT; i++)
            keys.add(i);

        check(new IgniteInClosure<Integer>() {
            @Override public void apply(Integer key) {
                assertFalse(cache.get(key) instanceof BinaryObjectOffheapImpl);
            }
        });

        check(new IgniteInClosure<Integer>() {
            @Override public void apply(Integer key) {
                assertFalse(cache.getEntry(key).getValue() instanceof BinaryObjectOffheapImpl);
            }
        });

        check(new IgniteInClosure<Integer>() {
            @Override public void apply(Integer key) {
                assertFalse(cache.getAndPut(key, cache.get(key)) instanceof BinaryObjectOffheapImpl);
            }
        });

        check(new IgniteInClosure<Integer>() {
            @Override public void apply(Integer key) {
                assertFalse(cache.getAndReplace(key, cache.get(key)) instanceof BinaryObjectOffheapImpl);
            }
        });

        check(new IgniteInClosure<Integer>() {
            @Override public void apply(Integer key) {
                assertFalse(cache.getAndPutIfAbsent(key, cache.get(key)) instanceof BinaryObjectOffheapImpl);
            }
        });

        check(new IgniteInClosure<Integer>() {
            @Override public void apply(Integer key) {
                assertFalse(cache.localPeek(key) instanceof BinaryObjectOffheapImpl);
            }
        });

        check(new IgniteInClosure<Integer>() {
            @Override public void apply(Integer key) {
                assertFalse(cache.getAndRemove(key) instanceof BinaryObjectOffheapImpl);
            }
        });

        check(new IgniteInClosure<Integer>() {
            @Override public void apply(Integer key) {
                assertFalse(cache.invoke(key, PROC) instanceof BinaryObjectOffheapImpl);
            }
        });

        // GetAll.
        Map<Integer, BinaryObject> res = cache.getAll(keys);

        for (BinaryObject val : res.values())
            assertFalse(val instanceof BinaryObjectOffheapImpl);

        if (atomicityMode == TRANSACTIONAL) {
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
                    try (Transaction tx = ignite(0).transactions().txStart(concurrency, isolation)) {
                        res = cache.getAll(keys);

                        for (BinaryObject val : res.values())
                            assertFalse(val instanceof BinaryObjectOffheapImpl);

                        tx.commit();
                    }
                }
            }
        }

        // GetAllOutTx.
        res = cache.getAllOutTx(keys);

        for (BinaryObject val : res.values())
            assertFalse(val instanceof BinaryObjectOffheapImpl);

        if (atomicityMode == TRANSACTIONAL) {
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
                    try (Transaction tx = ignite(0).transactions().txStart(concurrency, isolation)) {
                        res = cache.getAllOutTx(keys);

                        for (BinaryObject val : res.values())
                            assertFalse(val instanceof BinaryObjectOffheapImpl);

                        tx.commit();
                    }
                }
            }
        }

        // InvokeAll.
        res = cache.invokeAll(keys, PROC);

        for (BinaryObject val : res.values())
            assertFalse(val instanceof BinaryObjectOffheapImpl);

        if (atomicityMode == TRANSACTIONAL) {
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
                    try (Transaction tx = ignite(0).transactions().txStart(concurrency, isolation)) {
                        res = cache.invokeAll(keys, PROC);

                        for (BinaryObject val : res.values())
                            assertFalse(val instanceof BinaryObjectOffheapImpl);

                        tx.commit();
                    }
                }
            }
        }

        // GetEntries.
        Collection<CacheEntry<Integer, BinaryObject>> entries = cache.getEntries(keys);

        for (CacheEntry<Integer, BinaryObject> e : entries)
            assertFalse(e.getValue() instanceof BinaryObjectOffheapImpl);

        if (atomicityMode == TRANSACTIONAL) {
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
                    try (Transaction tx = ignite(0).transactions().txStart(concurrency, isolation)) {
                        entries = cache.getEntries(keys);

                        for (CacheEntry<Integer, BinaryObject> e : entries)
                            assertFalse(e.getValue() instanceof BinaryObjectOffheapImpl);

                        tx.commit();
                    }
                }
            }
        }
    }

    /**
     *
     */
    private void check(IgniteInClosure<Integer> checkOp) {
        for (int key = 0; key < 2 * CNT; key++) {
            checkOp.apply(key);

            if (atomicityMode == TRANSACTIONAL) {
                for (TransactionIsolation isolation : TransactionIsolation.values()) {
                    for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
                        try (Transaction tx = ignite(0).transactions().txStart(concurrency, isolation)) {
                            checkOp.apply(key);

                            tx.commit();
                        }
                    }
                }
            }
        }
    }

    /**
     *
     */
    @SuppressWarnings("PackageVisibleField")
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
