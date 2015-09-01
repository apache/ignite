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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import javax.cache.Cache;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.PRIMARY;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_TIERED;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public abstract class GridCacheOffHeapTieredAbstractSelfTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /** {@inheritDoc} */
    protected boolean portableEnabled() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setAtomicWriteOrderMode(PRIMARY);

        ccfg.setMemoryMode(OFFHEAP_TIERED);
        ccfg.setOffHeapMaxMemory(1024 * 1024);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransform() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(null);

        checkTransform(primaryKey(cache));

        checkTransform(backupKey(cache));

        checkTransform(nearKey(cache));
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void checkTransform(Integer key) throws Exception {
        IgniteCache<Integer, Integer> c = grid(0).cache(null);

        c.invoke(key, new EntryProcessor<Integer, Integer, Void>() {
            @Override public Void process(MutableEntry<Integer, Integer> e, Object... args) {
                Integer val = e.getValue();

                assertNull("Unexpected value: " + val, val);

                return null;
            }
        });

        c.put(key, 1);

        c.invoke(key, new EntryProcessor<Integer, Integer, Void>() {
            @Override public Void process(MutableEntry<Integer, Integer> e, Object... args) {
                Integer val = e.getValue();

                assertNotNull("Unexpected value: " + val, val);

                assertEquals((Integer) 1, val);

                e.setValue(val + 1);

                return null;
            }
        });

        assertEquals((Integer)2, c.get(key));

        c.invoke(key, new EntryProcessor<Integer, Integer, Void>() {
            @Override public Void process(MutableEntry<Integer, Integer> e, Object... args) {
                Integer val = e.getValue();

                assertNotNull("Unexpected value: " + val, val);

                assertEquals((Integer)2, val);

                e.setValue(val);

                return null;
            }
        });

        assertEquals((Integer)2, c.get(key));

        c.invoke(key, new EntryProcessor<Integer, Integer, Void>() {
            @Override public Void process(MutableEntry<Integer, Integer> e, Object... args) {
                Integer val = e.getValue();

                assertNotNull("Unexpected value: " + val, val);

                assertEquals((Integer)2, val);

                e.remove();

                return null;
            }
        });

        assertNull(c.get(key));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutGetRemove() throws Exception {
        IgniteCache<Integer, Integer> c = grid(0).cache(null);

        checkPutGetRemove(primaryKey(c));

        checkPutGetRemove(backupKey(c));

        checkPutGetRemove(nearKey(c));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutGetRemoveByteArray() throws Exception {
        IgniteCache<Integer, Integer> c = grid(0).cache(null);

        checkPutGetRemoveByteArray(primaryKey(c));

        checkPutGetRemoveByteArray(backupKey(c));

        checkPutGetRemoveByteArray(nearKey(c));
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void checkPutGetRemove(Integer key) throws Exception {
        IgniteCache<Integer, Integer> c = grid(0).cache(null);

        checkValue(key, null);

        assertNull(c.getAndPut(key, key));

        checkValue(key, key);

        assertEquals(key, c.getAndRemove(key));

        checkValue(key, null);

        if (atomicityMode() == TRANSACTIONAL) {
            checkPutGetRemoveTx(key, PESSIMISTIC);

            checkPutGetRemoveTx(key, OPTIMISTIC);
        }
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void checkPutGetRemoveByteArray(Integer key) throws Exception {
        IgniteCache<Integer, byte[]> c = grid(0).cache(null);

        checkValue(key, null);

        byte[] val = new byte[] {key.byteValue()};

        assertNull(c.getAndPut(key, val));

        checkValue(key, val);

        Assert.assertArrayEquals(val, c.getAndRemove(key));

        checkValue(key, null);

        if (atomicityMode() == TRANSACTIONAL) {
            checkPutGetRemoveTxByteArray(key, PESSIMISTIC);

            checkPutGetRemoveTxByteArray(key, OPTIMISTIC);
        }
    }

    /**
     * @param key Key,
     * @param txConcurrency Transaction concurrency.
     * @throws Exception If failed.
     */
    private void checkPutGetRemoveTx(Integer key, TransactionConcurrency txConcurrency) throws Exception {
        IgniteCache<Integer, Integer> c = grid(0).cache(null);

        Transaction tx = grid(0).transactions().txStart(txConcurrency, REPEATABLE_READ);

        assertNull(c.getAndPut(key, key));

        tx.commit();

        checkValue(key, key);

        tx = grid(0).transactions().txStart(txConcurrency, REPEATABLE_READ);

        assertEquals(key, c.getAndRemove(key));

        tx.commit();

        checkValue(key, null);
    }

    /**
     * @param key Key,
     * @param txConcurrency Transaction concurrency.
     * @throws Exception If failed.
     */
    private void checkPutGetRemoveTxByteArray(Integer key, TransactionConcurrency txConcurrency) throws Exception {
        IgniteCache<Integer, byte[]> c = grid(0).cache(null);

        Transaction tx = grid(0).transactions().txStart(txConcurrency, REPEATABLE_READ);

        byte[] val = new byte[] {key.byteValue()};

        assertNull(c.getAndPut(key, val));

        tx.commit();

        checkValue(key, val);

        tx = grid(0).transactions().txStart(txConcurrency, REPEATABLE_READ);

        Assert.assertArrayEquals(val, c.getAndRemove(key));

        tx.commit();

        checkValue(key, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPromote() throws Exception {
        IgniteCache<Integer, TestValue> c = grid(0).cache(null);

        TestValue val = new TestValue(new byte[100 * 1024]);

        List<Integer> keys = primaryKeys(c, 200);

        for (Integer key : keys)
            c.put(key, val);

        c.localPromote(new HashSet<>(keys));

        for (Integer key : keys) {
            TestValue val0 = c.get(key);

            Assert.assertArrayEquals(val.val, val0.val);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutAllGetAllRemoveAll() throws Exception {
        Map<Integer, Integer> map = new HashMap<>();

        for (int i = 0; i < 100; i++)
            map.put(i, i);

        IgniteCache<Integer, Integer> c = grid(0).cache(null);

        Map<Integer, Integer> map0 = c.getAll(map.keySet());

        assertTrue(map0.isEmpty());

        c.putAll(map);

        map0 = c.getAll(map.keySet());

        assertEquals(map, map0);

        for (Map.Entry<Integer, Integer> e : map.entrySet())
            checkValue(e.getKey(), e.getValue());

        c.invokeAll(map.keySet(), new EntryProcessor<Integer, Integer, Void>() {
            @Override public Void process(MutableEntry<Integer, Integer> e, Object... args) {
                Integer val = e.getValue();

                e.setValue(val + 1);

                return null;
            }
        });

        map0 = c.getAll(map.keySet());

        for (Map.Entry<Integer, Integer> e : map0.entrySet())
            assertEquals((Integer)(e.getKey() + 1), e.getValue());

        for (Map.Entry<Integer, Integer> e : map.entrySet())
            checkValue(e.getKey(), e.getValue() + 1);

        c.removeAll(map.keySet());

        map0 = c.getAll(map.keySet());

        assertTrue(map0.isEmpty());

        for (Map.Entry<Integer, Integer> e : map.entrySet())
            checkValue(e.getKey(), null);

        if (atomicityMode() == TRANSACTIONAL) {
            checkPutAllGetAllRemoveAllTx(PESSIMISTIC);

            checkPutAllGetAllRemoveAllTx(OPTIMISTIC);
        }
    }

    /**
     * @param txConcurrency Transaction concurrency.
     * @throws Exception If failed.
     */
    private void checkPutAllGetAllRemoveAllTx(TransactionConcurrency txConcurrency) throws Exception {
        Map<Integer, Integer> map = new HashMap<>();

        for (int i = 0; i < 100; i++)
            map.put(i, i);

        IgniteCache<Integer, Integer> c = grid(0).cache(null);

        Map<Integer, Integer> map0 = c.getAll(map.keySet());

        assertTrue(map0.isEmpty());

        try (Transaction tx = grid(0).transactions().txStart(txConcurrency, REPEATABLE_READ)) {
            c.putAll(map);

            tx.commit();
        }

        map0 = c.getAll(map.keySet());

        assertEquals(map, map0);

        for (Map.Entry<Integer, Integer> e : map.entrySet())
            checkValue(e.getKey(), e.getValue());

        try (Transaction tx = grid(0).transactions().txStart(txConcurrency, REPEATABLE_READ)) {
            c.removeAll(map.keySet());

            tx.commit();
        }

        map0 = c.getAll(map.keySet());

        assertTrue(map0.isEmpty());

        for (Map.Entry<Integer, Integer> e : map.entrySet())
            checkValue(e.getKey(), null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutGetRemoveObject() throws Exception {
        IgniteCache<Integer, Integer> c = grid(0).cache(null);

        checkPutGetRemoveObject(primaryKey(c));

        checkPutGetRemoveObject(backupKey(c));

        checkPutGetRemoveObject(nearKey(c));
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void checkPutGetRemoveObject(Integer key) throws Exception {
        IgniteCache<Integer, TestValue> c = grid(0).cache(null);

        checkValue(key, null);

        TestValue val = new TestValue(new byte[10]);

        assertNull(c.getAndPut(key, val));

        checkValue(key, val);

        TestValue val2 = new TestValue(new byte[10]);

        if (portableEnabled()) // TODO: IGNITE-608, check return value when fixed.
            c.put(key, val);
        else
            assertEquals(val, c.getAndPut(key, val));

        checkValue(key, val2);

        if (portableEnabled()) // TODO: IGNITE-608, check return value when fixed.
            c.remove(key);
        else
            assertEquals(val2, c.getAndRemove(key));

        checkValue(key, null);

        if (atomicityMode() == TRANSACTIONAL) {
            checkPutGetRemoveTx(key, PESSIMISTIC);

            checkPutGetRemoveTx(key, OPTIMISTIC);
        }
    }

    /**
     * @param key Key,
     * @param txConcurrency Transaction concurrency.
     * @throws Exception If failed.
     */
    private void checkPutGetRemoveObjectTx(Integer key, TransactionConcurrency txConcurrency) throws Exception {
        IgniteCache<Integer, TestValue> c = grid(0).cache(null);

        TestValue val = new TestValue(new byte[10]);

        Transaction tx = grid(0).transactions().txStart(txConcurrency, REPEATABLE_READ);

        assertNull(c.getAndPut(key, val));

        tx.commit();

        checkValue(key, val);

        tx = grid(0).transactions().txStart(txConcurrency, REPEATABLE_READ);

        assertEquals(val, c.getAndRemove(key));

        tx.commit();

        checkValue(key, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLockUnlock() throws Exception {
        if (atomicityMode() == ATOMIC)
            return;

        IgniteCache<Integer, TestValue> c = grid(0).cache(null);

        checkLockUnlock(primaryKey(c));

        checkLockUnlock(backupKey(c));

        checkLockUnlock(nearKey(c));
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    @SuppressWarnings("UnnecessaryLocalVariable")
    private void checkLockUnlock(Integer key) throws Exception {
        IgniteCache<Integer, Integer> c = grid(0).cache(null);

        Integer val = key;

        c.put(key, val);

        assertNull(c.localPeek(key, CachePeekMode.ONHEAP));

        Lock lock = c.lock(key);

        lock.lock();

        assertTrue(c.isLocalLocked(key, false));

        lock.unlock();

        assertFalse(c.isLocalLocked(key, false));

        assertNull(c.localPeek(key, CachePeekMode.ONHEAP));

        checkValue(key, val);
    }

    /**
     * @param key Key.
     * @param val Value.
     * @throws Exception If failed.
     */
    private void checkValue(Object key, @Nullable Object val) throws Exception {
        for (int i = 0; i < gridCount(); i++) {
            if (val != null && val.getClass() == byte[].class) {
                Assert.assertArrayEquals("Unexpected value for grid: " + i,
                    (byte[])val,
                    (byte[])grid(i).cache(null).get(key));
            }
            else
                assertEquals("Unexpected value for grid: " + i, val, grid(i).cache(null).get(key));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testUnswap() throws Exception {
        IgniteCache<Integer, Integer> c = grid(0).cache(null);

        checkUnswap(primaryKey(c));

        checkUnswap(backupKey(c));

        checkUnswap(nearKey(c));
    }

    /**
     * @param key Key.
     * @throws Exception If failed.
     */
    private void checkUnswap(Integer key) throws Exception {
        IgniteCache<Integer, Integer> c = grid(0).cache(null);

        for (int i = 0; i < gridCount(); i++) {
            assertEquals("Unexpected entries for grid: " + i, 0, grid(i).cache(null).localSize(CachePeekMode.OFFHEAP));

            assertEquals("Unexpected offheap size for grid: " + i, 0, internalCache(i).offHeapAllocatedSize());
        }

        assertNull(c.localPeek(key, CachePeekMode.ONHEAP));

        c.put(key, key);

        assertNull(c.localPeek(key, CachePeekMode.ONHEAP));

        assertEquals(key, c.get(key));

        assertNull(c.localPeek(key, CachePeekMode.ONHEAP));

        assertTrue(c.remove(key));

        assertNull(c.localPeek(key, CachePeekMode.ONHEAP));

        for (int i = 0; i < gridCount(); i++) {
            assertEquals("Unexpected entries for grid: " + i, 0, grid(i).cache(null).localSize(CachePeekMode.OFFHEAP));

            assertEquals("Unexpected offheap size for grid: " + i, 0, internalCache(i).offHeapAllocatedSize());
        }
    }

    /**
     *
     */
    @SuppressWarnings("PublicInnerClass")
    public static class TestEntryPredicate implements IgnitePredicate<Cache.Entry<Integer, Integer>> {
        /** */
        private Integer expVal;

        /**
         * @param expVal Expected value.
         */
        TestEntryPredicate(Integer expVal) {
            this.expVal = expVal;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(Cache.Entry<Integer, Integer> e) {
            assertEquals(expVal, e.getValue());

            return true;
        }
    }

    /**
     *
     */
    @SuppressWarnings("PublicInnerClass")
    public static class TestValue {
        /** */
        @SuppressWarnings("PublicField")
        public byte[] val;

        /**
         * Default constructor.
         */
        public TestValue() {
            // No-op.
        }

        /**
         * @param val Value.
         */
        public TestValue(byte[] val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestValue other = (TestValue)o;

            return Arrays.equals(val, other.val);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Arrays.hashCode(val);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TestValue{" +
                "val=" + Arrays.toString(val) +
                '}';
        }
    }
}