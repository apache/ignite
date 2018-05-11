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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public abstract class IgniteCacheInvokeAbstractTest extends IgniteCacheAbstractTest {
    /** */
    private Integer lastKey = 0;

    /**
     * @throws Exception If failed.
     */
    public void testInvoke() throws Exception {
        IgniteCache<Integer, Integer> cache = jcache();

        invoke(cache, null);

        if (atomicityMode() == TRANSACTIONAL) {
            invoke(cache, PESSIMISTIC);

            invoke(cache, OPTIMISTIC);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testInternalInvokeNullable() throws Exception {
        IgniteInternalCache<Integer, Integer> cache = grid(0).cachex(DEFAULT_CACHE_NAME);

        EntryProcessor<Integer, Integer, Void> processor = new NullableProcessor();

        for (final Integer key : keys()) {
            log.info("Test invoke with a nullable result [key=" + key + ']');

            EntryProcessorResult<Void> result = cache.invoke(key, processor);
            EntryProcessorResult<Void> resultAsync = cache.invokeAsync(key, processor).get();

            assertNotNull(result);
            assertNotNull(resultAsync);

            assertNull(result.get());
            assertNull(resultAsync.get());
        }
    }

    /**
     * @param cache Cache.
     * @param txMode Not null transaction concurrency mode if explicit transaction should be started.
     * @throws Exception If failed.
     */
    private void invoke(final IgniteCache<Integer, Integer> cache, @Nullable TransactionConcurrency txMode)
        throws Exception {
        IncrementProcessor incProcessor = new IncrementProcessor();

        for (final Integer key : keys()) {
            log.info("Test invoke [key=" + key + ", txMode=" + txMode + ']');

            cache.remove(key);

            Transaction tx = startTx(txMode);

            Integer res = cache.invoke(key, incProcessor);

            if (tx != null)
                tx.commit();

            assertEquals(-1, (int)res);

            checkValue(key, 1);

            tx = startTx(txMode);

            res = cache.invoke(key, incProcessor);

            if (tx != null)
                tx.commit();

            assertEquals(1, (int)res);

            checkValue(key, 2);

            tx = startTx(txMode);

            res = cache.invoke(key, incProcessor);

            if (tx != null)
                tx.commit();

            assertEquals(2, (int)res);

            checkValue(key, 3);

            tx = startTx(txMode);

            res = cache.invoke(key, new ArgumentsSumProcessor(), 10, 20, 30);

            if (tx != null)
                tx.commit();

            assertEquals(3, (int)res);

            checkValue(key, 63);

            tx = startTx(txMode);

            String strRes = cache.invoke(key, new ToStringProcessor());

            if (tx != null)
                tx.commit();

            assertEquals("63", strRes);

            checkValue(key, 63);

            tx = startTx(txMode);

            TestValue testVal = cache.invoke(key, new UserClassValueProcessor());

            if (tx != null)
                tx.commit();

            assertEquals("63", testVal.value());

            checkValue(key, 63);

            tx = startTx(txMode);

            Collection<TestValue> testValCol = cache.invoke(key, new CollectionReturnProcessor());

            if (tx != null)
                tx.commit();

            assertEquals(10, testValCol.size());

            for (TestValue val : testValCol)
                assertEquals("64", val.value());

            checkValue(key, 63);

            tx = startTx(txMode);

            GridTestUtils.assertThrows(log, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    cache.invoke(key, new ExceptionProcessor(63));

                    return null;
                }
            }, EntryProcessorException.class, "Test processor exception.");

            if (tx != null)
                tx.commit();

            checkValue(key, 63);

            IgniteFuture<Integer> fut = cache.invokeAsync(key, incProcessor);

            assertNotNull(fut);

            assertEquals(63, (int)fut.get());

            checkValue(key, 64);

            tx = startTx(txMode);

            assertNull(cache.invoke(key, new RemoveProcessor(64)));

            if (tx != null)
                tx.commit();

            checkValue(key, null);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeAll() throws Exception {
        IgniteCache<Integer, Integer> cache = jcache();

        invokeAll(cache, null);

        if (atomicityMode() == TRANSACTIONAL) {
            invokeAll(cache, PESSIMISTIC);

            invokeAll(cache, OPTIMISTIC);
        }
    }

    /**
     * @param cache Cache.
     * @param txMode Not null transaction concurrency mode if explicit transaction should be started.
     * @throws Exception If failed.
     */
    private void invokeAll(IgniteCache<Integer, Integer> cache, @Nullable TransactionConcurrency txMode) throws Exception {
        invokeAll(cache, new HashSet<>(primaryKeys(cache, 3, 0)), txMode);

        if (gridCount() > 1) {
            invokeAll(cache, new HashSet<>(backupKeys(cache, 3, 0)), txMode);

            invokeAll(cache, new HashSet<>(nearKeys(cache, 3, 0)), txMode);

            Set<Integer> keys = new HashSet<>();

            keys.addAll(primaryKeys(jcache(0), 3, 0));
            keys.addAll(primaryKeys(jcache(1), 3, 0));
            keys.addAll(primaryKeys(jcache(2), 3, 0));

            invokeAll(cache, keys, txMode);
        }

        Set<Integer> keys = new HashSet<>();

        for (int i = 0; i < 1000; i++)
            keys.add(i);

        invokeAll(cache, keys, txMode);
    }

    /**
     * @param cache Cache.
     * @param keys Keys.
     * @param txMode Not null transaction concurrency mode if explicit transaction should be started.
     * @throws Exception If failed.
     */
    private void invokeAll(IgniteCache<Integer, Integer> cache,
        Set<Integer> keys,
        @Nullable TransactionConcurrency txMode) throws Exception {
        cache.removeAll(keys);

        log.info("Test invokeAll [keys=" + keys + ", txMode=" + txMode + ']');

        IncrementProcessor incProcessor = new IncrementProcessor();

        {
            Transaction tx = startTx(txMode);

            Map<Integer, EntryProcessorResult<Integer>> resMap = cache.invokeAll(keys, incProcessor);

            if (tx != null)
                tx.commit();

            Map<Object, Object> exp = new HashMap<>();

            for (Integer key : keys)
                exp.put(key, -1);

            checkResult(resMap, exp);

            for (Integer key : keys)
                checkValue(key, 1);
        }

        {
            Transaction tx = startTx(txMode);

            Map<Integer, EntryProcessorResult<TestValue>> resMap = cache.invokeAll(keys, new UserClassValueProcessor());

            if (tx != null)
                tx.commit();

            Map<Object, Object> exp = new HashMap<>();

            for (Integer key : keys)
                exp.put(key, new TestValue("1"));

            checkResult(resMap, exp);

            for (Integer key : keys)
                checkValue(key, 1);
        }

        {
            Transaction tx = startTx(txMode);

            Map<Integer, EntryProcessorResult<Collection<TestValue>>> resMap =
                cache.invokeAll(keys, new CollectionReturnProcessor());

            if (tx != null)
                tx.commit();

            Map<Object, Object> exp = new HashMap<>();

            for (Integer key : keys) {
                List<TestValue> expCol = new ArrayList<>();

                for (int i = 0; i < 10; i++)
                    expCol.add(new TestValue("2"));

                exp.put(key, expCol);
            }

            checkResult(resMap, exp);

            for (Integer key : keys)
                checkValue(key, 1);
        }

        {
            Transaction tx = startTx(txMode);

            Map<Integer, EntryProcessorResult<Integer>> resMap = cache.invokeAll(keys, incProcessor);

            if (tx != null)
                tx.commit();

            Map<Object, Object> exp = new HashMap<>();

            for (Integer key : keys)
                exp.put(key, 1);

            checkResult(resMap, exp);

            for (Integer key : keys)
                checkValue(key, 2);
        }

        {
            Transaction tx = startTx(txMode);

            Map<Integer, EntryProcessorResult<Integer>> resMap =
                cache.invokeAll(keys, new ArgumentsSumProcessor(), 10, 20, 30);

            if (tx != null)
                tx.commit();

            Map<Object, Object> exp = new HashMap<>();

            for (Integer key : keys)
                exp.put(key, 3);

            checkResult(resMap, exp);

            for (Integer key : keys)
                checkValue(key, 62);
        }

        {
            Transaction tx = startTx(txMode);

            Map<Integer, EntryProcessorResult<Integer>> resMap = cache.invokeAll(keys, new ExceptionProcessor(null));

            if (tx != null)
                tx.commit();

            for (Integer key : keys) {
                final EntryProcessorResult<Integer> res = resMap.get(key);

                assertNotNull("No result for " + key);

                GridTestUtils.assertThrows(log, new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        res.get();

                        return null;
                    }
                }, EntryProcessorException.class, "Test processor exception.");
            }

            for (Integer key : keys)
                checkValue(key, 62);
        }

        {
            Transaction tx = startTx(txMode);

            Map<Integer, EntryProcessor<Integer, Integer, Integer>> invokeMap = new HashMap<>();

            for (Integer key : keys) {
                switch (key % 4) {
                    case 0: invokeMap.put(key, new IncrementProcessor()); break;

                    case 1: invokeMap.put(key, new RemoveProcessor(62)); break;

                    case 2: invokeMap.put(key, new ArgumentsSumProcessor()); break;

                    case 3: invokeMap.put(key, new ExceptionProcessor(62)); break;

                    default:
                        fail();
                }
            }

            Map<Integer, EntryProcessorResult<Integer>> resMap = cache.invokeAll(invokeMap, 10, 20, 30);

            if (tx != null)
                tx.commit();

            for (Integer key : keys) {
                final EntryProcessorResult<Integer> res = resMap.get(key);

                switch (key % 4) {
                    case 0: {
                        assertNotNull("No result for " + key, res);

                        assertEquals(62, (int)res.get());

                        checkValue(key, 63);

                        break;
                    }

                    case 1: {
                        assertNull(res);

                        checkValue(key, null);

                        break;
                    }

                    case 2: {
                        assertNotNull("No result for " + key, res);

                        assertEquals(3, (int)res.get());

                        checkValue(key, 122);

                        break;
                    }

                    case 3: {
                        assertNotNull("No result for " + key, res);

                        GridTestUtils.assertThrows(log, new Callable<Void>() {
                            @Override public Void call() throws Exception {
                                res.get();

                                return null;
                            }
                        }, EntryProcessorException.class, "Test processor exception.");

                        checkValue(key, 62);

                        break;
                    }
                }
            }
        }

        cache.invokeAll(keys, new IncrementProcessor());

        {
            Transaction tx = startTx(txMode);

            Map<Integer, EntryProcessorResult<Integer>> resMap = cache.invokeAll(keys, new RemoveProcessor(null));

            if (tx != null)
                tx.commit();

            assertEquals("Unexpected results: " + resMap, 0, resMap.size());

            for (Integer key : keys)
                checkValue(key, null);
        }

        IgniteFuture<Map<Integer, EntryProcessorResult<Integer>>> fut = cache.invokeAllAsync(keys, new IncrementProcessor());

        Map<Integer, EntryProcessorResult<Integer>> resMap = fut.get();

        Map<Object, Object> exp = new HashMap<>();

        for (Integer key : keys)
            exp.put(key, -1);

        checkResult(resMap, exp);

        for (Integer key : keys)
            checkValue(key, 1);

        Map<Integer, EntryProcessor<Integer, Integer, Integer>> invokeMap = new HashMap<>();

        for (Integer key : keys)
            invokeMap.put(key, incProcessor);

        fut = cache.invokeAllAsync(invokeMap);

        resMap = fut.get();

        for (Integer key : keys)
            exp.put(key, 1);

        checkResult(resMap, exp);

        for (Integer key : keys)
            checkValue(key, 2);
    }

    /**
     * @param resMap Result map.
     * @param exp Expected results.
     */
    @SuppressWarnings("unchecked")
    private void checkResult(Map resMap, Map<Object, Object> exp) {
        assertNotNull(resMap);

        assertEquals(exp.size(), resMap.size());

        for (Map.Entry<Object, Object> expVal : exp.entrySet()) {
            EntryProcessorResult<?> res = (EntryProcessorResult)resMap.get(expVal.getKey());

            assertNotNull("No result for " + expVal.getKey(), res);

            assertEquals("Unexpected result for " + expVal.getKey(), res.get(), expVal.getValue());
        }
    }

    /**
     * @param key Key.
     * @param expVal Expected value.
     */
    protected void checkValue(Object key, @Nullable Object expVal) {
        if (expVal != null) {
            for (int i = 0; i < gridCount(); i++) {
                IgniteCache<Object, Object> cache = jcache(i);

                Object val = cache.localPeek(key);

                if (val == null)
                    assertFalse(ignite(0).affinity(DEFAULT_CACHE_NAME).isPrimaryOrBackup(ignite(i).cluster().localNode(), key));
                else
                    assertEquals("Unexpected value for grid " + i, expVal, val);
            }
        }
        else {
            for (int i = 0; i < gridCount(); i++) {
                IgniteCache<Object, Object> cache = jcache(i);

                assertNull("Unexpected non null value for grid " + i, cache.localPeek(key, CachePeekMode.ONHEAP));
            }
        }
    }

    /**
     * @return Test keys.
     * @throws Exception If failed.
     */
    protected Collection<Integer> keys() throws Exception {
        IgniteCache<Integer, Object> cache = jcache(0);

        ArrayList<Integer> keys = new ArrayList<>();

        keys.add(primaryKeys(cache, 1, lastKey).get(0));

        if (gridCount() > 1) {
            keys.add(backupKeys(cache, 1, lastKey).get(0));

            if (cache.getConfiguration(CacheConfiguration.class).getCacheMode() != REPLICATED)
                keys.add(nearKeys(cache, 1, lastKey).get(0));
        }

        lastKey = Collections.max(keys) + 1;

        return keys;
    }

    /**
     * @param txMode Transaction concurrency mode.
     * @return Transaction.
     */
    @Nullable private Transaction startTx(@Nullable TransactionConcurrency txMode) {
        return txMode == null ? null : ignite(0).transactions().txStart(txMode, REPEATABLE_READ);
    }

    /**
     *
     */
    private static class ArgumentsSumProcessor implements EntryProcessor<Integer, Integer, Integer> {
        /** {@inheritDoc} */
        @Override public Integer process(MutableEntry<Integer, Integer> e, Object... args)
            throws EntryProcessorException {
            assertEquals(3, args.length);
            assertEquals(10, args[0]);
            assertEquals(20, args[1]);
            assertEquals(30, args[2]);

            assertTrue(e.exists());

            Integer res = e.getValue();

            for (Object arg : args)
                res += (Integer)arg;

            e.setValue(res);

            return args.length;
        }
    }

    /**
     *
     */
    protected static class ToStringProcessor implements EntryProcessor<Integer, Integer, String> {
        /** {@inheritDoc} */
        @Override public String process(MutableEntry<Integer, Integer> e, Object... arguments)
            throws EntryProcessorException {
            return String.valueOf(e.getValue());
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ToStringProcessor.class, this);
        }
    }

    /**
     *
     */
    protected static class UserClassValueProcessor implements EntryProcessor<Integer, Integer, TestValue> {
        /** {@inheritDoc} */
        @Override public TestValue process(MutableEntry<Integer, Integer> e, Object... arguments)
            throws EntryProcessorException {
            return new TestValue(String.valueOf(e.getValue()));
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(UserClassValueProcessor.class, this);
        }
    }

    /**
     *
     */
    protected static class CollectionReturnProcessor implements
        EntryProcessor<Integer, Integer, Collection<TestValue>> {
        /** {@inheritDoc} */
        @Override public Collection<TestValue> process(MutableEntry<Integer, Integer> e, Object... arguments)
            throws EntryProcessorException {
            List<TestValue> vals = new ArrayList<>();

            for (int i = 0; i < 10; i++)
                vals.add(new TestValue(String.valueOf(e.getValue() + 1)));

            return vals;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CollectionReturnProcessor.class, this);
        }
    }

    /**
     *
     */
    protected static class IncrementProcessor implements EntryProcessor<Integer, Integer, Integer> {
        /** {@inheritDoc} */
        @Override public Integer process(MutableEntry<Integer, Integer> e,
            Object... arguments) throws EntryProcessorException {
            Ignite ignite = e.unwrap(Ignite.class);

            assertNotNull(ignite);

            if (e.exists()) {
                Integer val = e.getValue();

                assertNotNull(val);

                e.setValue(val + 1);

                assertTrue(e.exists());

                assertEquals(val + 1, (int) e.getValue());

                return val;
            }
            else {
                e.setValue(1);

                return -1;
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(IncrementProcessor.class, this);
        }
    }

    /**
     *
     */
    private static class RemoveProcessor implements EntryProcessor<Integer, Integer, Integer> {
        /** */
        private Integer expVal;

        /**
         * @param expVal Expected value.
         */
        RemoveProcessor(@Nullable Integer expVal) {
            this.expVal = expVal;
        }

        /** {@inheritDoc} */
        @Override public Integer process(MutableEntry<Integer, Integer> e,
            Object... arguments) throws EntryProcessorException {
            assertTrue(e.exists());

            if (expVal != null)
                assertEquals(expVal, e.getValue());

            e.remove();

            assertFalse(e.exists());

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RemoveProcessor.class, this);
        }
    }

    /**
     *
     */
    private static class ExceptionProcessor implements EntryProcessor<Integer, Integer, Integer> {
        /** */
        private Integer expVal;

        /**
         * @param expVal Expected value.
         */
        ExceptionProcessor(@Nullable Integer expVal) {
            this.expVal = expVal;
        }

        /** {@inheritDoc} */
        @Override public Integer process(MutableEntry<Integer, Integer> e,
            Object... arguments) throws EntryProcessorException {
            assertTrue(e.exists());

            if (expVal != null)
                assertEquals(expVal, e.getValue());

            throw new EntryProcessorException("Test processor exception.");
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ExceptionProcessor.class, this);
        }
    }

    /**
     * EntryProcessor which always returns {@code null}.
     */
    private static class NullableProcessor implements EntryProcessor<Integer, Integer, Void> {
        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<Integer, Integer> e,
            Object... arguments) throws EntryProcessorException {
            return null;
        }
    }

    /**
     *
     */
    static class TestValue {
        /** */
        private String val;

        /**
         * @param val Value.
         */
        public TestValue(String val) {
            this.val = val;
        }

        /**
         * @return Value.
         */
        public String value() {
            return val;
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