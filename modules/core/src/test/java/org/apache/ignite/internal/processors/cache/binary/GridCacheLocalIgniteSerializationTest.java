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

package org.apache.ignite.internal.processors.cache.binary;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Check that localIgnite() method calling during serialization
 * works correctly.
 */
public class GridCacheLocalIgniteSerializationTest extends GridCacheAbstractSelfTest {
    /** */
    private static final String CACHE_NAME = "cache_name";

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /**
     * Emulate user thread.
     *
     * @throws Throwable
     */
    @Override protected void runTest() throws Throwable {
        Class<?> cls = getClass();

        while (!cls.equals(GridAbstractTest.class))
            cls = cls.getSuperclass();

        final Method runTest = cls.getDeclaredMethod("runTestInternal");

        runTest.setAccessible(true);

        runTest.invoke(this);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        // Do nothing.
    }

    /**
     * Test that calling {@link Ignition#localIgnite()}
     * is safe for binary marshaller.
     *
     * @throws Exception
     */
    public void testUserThreadPutGet() throws Exception {
        final IgniteCache<Integer, MyObj> cache = startGrid().getOrCreateCache(CACHE_NAME);

        final List<MyObj> inObjs = generateTestObjs();

        int cnt = 0;

        for (final MyObj inObj : inObjs)
            cache.put(cnt++, inObj);

        assertLocalIgnite(inObjs, loadObjects(cache, cnt));

    }

    /**
     * Check that added to stream objects are serialized correctly.
     *
     * @throws Exception
     */
    public void testUserThreadStream() throws Exception {
        final Ignite ignite = startGrid();

        final IgniteCache<Integer, MyObj> cache = ignite.getOrCreateCache(CACHE_NAME);

        final List<MyObj> testObjs = generateTestObjs();

        final List<IgniteFuture> futs = new ArrayList<>();

        int cnt = 0;

        try (final IgniteDataStreamer<Integer, Object> strmr = ignite.dataStreamer(CACHE_NAME)) {

            for (final MyObj testobj : testObjs)
                futs.add(strmr.addData(cnt++, testobj));

        }

        waitFuts(futs);

        assertLocalIgnite(testObjs, loadObjects(cache, cnt));
    }

    /**
     * Check that cached in transaction objects are serialized correctly.
     *
     * @throws Exception
     */
    public void testTxUserThreadPutGet() throws Exception {
        final Ignite ignite = startGrid();

        for (final TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (final TransactionIsolation isolation : TransactionIsolation.values())
                checkTxUserThreadPutGet(concurrency, isolation, ignite);
        }
    }

    /**
     * Check that cached in transaction objects are serialized correctly.
     *
     * @param concurrency transaction concurrency.
     * @param isolation transaction isolation.
     * @param ignite ignite instance.
     * @throws Exception
     */
    protected void checkTxUserThreadPutGet(final TransactionConcurrency concurrency,
        final TransactionIsolation isolation, final Ignite ignite) throws Exception {
        final IgniteCache<Integer, MyObj> cache = ignite.getOrCreateCache(CACHE_NAME);

        cache.clear();

        final List<MyObj> testObjs = generateTestObjs();

        int cnt = 0;

        try (final Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
            for (final MyObj testObj : testObjs)
                cache.put(cnt++, testObj);

            tx.commit();
        }

        try (final Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
            assertLocalIgnite(testObjs, loadObjects(cache, cnt));

            tx.commit();
        }

    }

    /**
     * Load objects from cache one-by-one up to cnt.
     *
     * @param cache to load objects from
     * @param cnt obj id count.
     * @return list of loaded objects.
     */
    private List<MyObj> loadObjects(final IgniteCache<Integer, MyObj> cache, final int cnt) {
        final List<MyObj> loadedObjs = new ArrayList<>();

        for (int i = 0; i < cnt; i++)
            loadedObjs.add(cache.get(i));

        return loadedObjs;
    }

    /**
     * Wait unless futures done.
     *
     * @param futs futures to wait.
     * @throws Exception
     */
    private void waitFuts(final Iterable<IgniteFuture> futs) throws Exception {
        for (final IgniteFuture fut : futs)
            fut.get();
    }

    /**
     * @return list of test objects.
     */
    private List<MyObj> generateTestObjs() {
        return Arrays.asList(new MyObj("one"), new MyObj("two"), new MyObj("three"));
    }

    /**
     * Check whether objects and ignite instance are serialized correctly.
     *
     * @param input original objects.
     * @param loaded loaded objects.
     */
    private void assertLocalIgnite(final List<MyObj> input, final List<MyObj> loaded) {
        final Iterator<MyObj> inIter = input.iterator();
        final Iterator<MyObj> loadedIter = loaded.iterator();

        while (inIter.hasNext() && loadedIter.hasNext()) {
            final MyObj inNext = inIter.next();
            final MyObj loadedNext = loadedIter.next();

            assert inNext.equals(loadedNext);

            assert loadedNext.ignite1 != null && loadedNext.ignite2 != null;
        }

        assert !loadedIter.hasNext();
    }


    /**
     * Test obj.
     */
    private static class MyObj {

        /** */
        final String val;

        /** */
        Ignite ignite1;

        /** */
        Ignite ignite2;

        /** */
        private MyObj(final String val) {
            this.val = val;
        }

        private Object readResolve() {
            ignite1 = Ignition.localIgnite();
            return this;
        }

        private Object writeReplace() {
            ignite2 = Ignition.localIgnite();
            return this;
        }

        /** */
        @Override public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final MyObj myObj = (MyObj) o;

            return val != null ? val.equals(myObj.val) : myObj.val == null;

        }

        /** */
        @Override public int hashCode() {
            return val != null ? val.hashCode() : 0;
        }
    }
}
