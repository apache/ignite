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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import javax.cache.Cache;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.testframework.GridTestUtils;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.PRIMARY;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_TIERED;

/**
 * Tests that offheap entry is not evicted while cache entry is in use.
 */
public abstract class GridCacheOffHeapTieredEvictionAbstractSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int VALS = 100;

    /** */
    private static final int VAL_SIZE = 128;

    /** */
    private static final int KEYS = 100;

    /** */
    private List<TestValue> vals = new ArrayList<>(VALS);

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 120 * 1000;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setAtomicWriteOrderMode(PRIMARY);

        ccfg.setMemoryMode(OFFHEAP_TIERED);
        ccfg.setNearConfiguration(null);
        ccfg.setOffHeapMaxMemory(0);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        final IgniteCache<Integer, Object> cache = grid(0).cache(null);

        vals = new ArrayList<>(VALS);

        for (int i = 0; i < VALS; i++) {
            SB sb = new SB(VAL_SIZE);

            char c = Character.forDigit(i, 10);

            for (int j = 0; j < VAL_SIZE; j++)
                sb.a(c);

            vals.add(new TestValue(sb.toString()));
        }

        for (int i = 0; i < KEYS; i++)
            cache.put(i, vals.get(i % vals.size()));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        vals = null;
    }

    /**
     * @return Number of iterations per thread.
     */
    private int iterations() {
        return atomicityMode() == ATOMIC ? 100_000 : 50_000;
    }



    /**
     * @throws Exception If failed.
     */
    public void testPut() throws Exception {
        final IgniteCache<Integer, Object> cache = grid(0).cache(null);

        GridTestUtils.runMultiThreaded(new Callable<Void>() {
            @Override public Void call() throws Exception {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                for (int i = 0; i < iterations(); i++) {
                    int key = rnd.nextInt(KEYS);

                    final TestValue val = vals.get(key % VAL_SIZE);

                    cache.put(key, val);

                    if (i % 20_000 == 0 && i > 0)
                        info("Done " + i + " out of " + iterations());
                }

                return null;
            }
        }, 16, "test");
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemove() throws Exception {
        final IgniteCache<Integer, Object> cache = grid(0).cache(null);

        GridTestUtils.runMultiThreaded(new Callable<Void>() {
            @Override public Void call() throws Exception {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                for (int i = 0; i < iterations(); i++) {
                    int key = rnd.nextInt(KEYS);

                    final TestValue val = vals.get(key % VAL_SIZE);

                    if (rnd.nextBoolean())
                        cache.remove(key);
                    else
                        cache.put(key, val);
                }

                return null;
            }
        }, 16, "test");
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransform() throws Exception {
        final IgniteCache<Integer, Object> cache = grid(0).cache(null);

        GridTestUtils.runMultiThreaded(new Callable<Void>() {
            @Override public Void call() throws Exception {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                for (int i = 0; i < iterations(); i++) {
                    int key = rnd.nextInt(KEYS);

                    final TestValue val = vals.get(key % VAL_SIZE);

                    TestProcessor c = testClosure(val.val, false);

                    cache.invoke(key, c);
                }

                return null;
            }
        }, 16, "test");
    }

    /**
     * @param expVal Expected cache value.
     * @param acceptNull If {@code true} value can be null;
     * @return Predicate.
     */
    protected TestPredicate testPredicate(String expVal, boolean acceptNull) {
        return new TestValuePredicate(expVal, acceptNull);
    }

    /**
     * @param expVal Expected cache value.
     * @param acceptNull If {@code true} value can be null;
     * @return Predicate.
     */
    protected TestProcessor testClosure(String expVal, boolean acceptNull) {
        return new TestValueClosure(expVal, acceptNull);
    }

    /**
     *
     */
    @SuppressWarnings("PublicInnerClass")
    public static class TestValue {
        /** */
        @SuppressWarnings("PublicField")
        public String val;

        /**
         *
         */
        public TestValue() {
            // No-op.
        }

        /**
         * @param val Value.
         */
        public TestValue(String val) {
            this.val = val;
        }
    }

    /**
     *
     */
    protected abstract static class TestPredicate implements P1<Cache.Entry<Integer, Object>> {
        /** */
        protected String expVal;

        /** */
        protected boolean acceptNull;

        /**
         * @param expVal Expected value.
         * @param acceptNull If {@code true} value can be null;
         */
        protected TestPredicate(String expVal, boolean acceptNull) {
            this.expVal = expVal;
            this.acceptNull = acceptNull;
        }

        /** {@inheritDoc} */
        @Override public final boolean apply(Cache.Entry<Integer, Object> e) {
            assertNotNull(e);

            Object val = e.getValue();

            if (val == null) {
                if (!acceptNull)
                    assertNotNull(val);

                return true;
            }

            checkValue(val);

            return true;
        }

        /**
         * @param val Value.
         */
        public abstract void checkValue(Object val);
    }

    /**
     *
     */
    @SuppressWarnings("PackageVisibleInnerClass")
    static class TestValuePredicate extends TestPredicate {
        /**
         * @param expVal Expected value.
         * @param acceptNull If {@code true} value can be null;
         */
        TestValuePredicate(String expVal, boolean acceptNull) {
            super(expVal, acceptNull);
        }

        /** {@inheritDoc} */
        @Override public void checkValue(Object val) {
            TestValue obj = (TestValue)val;

            assertEquals(expVal, obj.val);
        }
    }

    /**
     *
     */
    protected abstract static class TestProcessor implements EntryProcessor<Integer, Object, Void>, Serializable {
        /** */
        protected String expVal;

        /** */
        protected boolean acceptNull;

        /**
         * @param expVal Expected value.
         * @param acceptNull If {@code true} value can be null;
         */
        protected TestProcessor(String expVal, boolean acceptNull) {
            this.expVal = expVal;
            this.acceptNull = acceptNull;
        }

        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<Integer, Object> e, Object... args) {
            Object val = e.getValue();

            if (val == null) {
                if (!acceptNull)
                    assertNotNull(val);

                e.setValue(true);

                return null;
            }

            checkValue(val);

            e.setValue(val);

            return null;
        }

        /**
         * @param val Value.
         */
        public abstract void checkValue(Object val);
    }

    /**
     *
     */
    @SuppressWarnings("PackageVisibleInnerClass")
    static class TestValueClosure extends TestProcessor {
        /**
         * @param expVal Expected value.
         * @param acceptNull If {@code true} value can be null;
         */
        TestValueClosure(String expVal, boolean acceptNull) {
            super(expVal, acceptNull);
        }

        /** {@inheritDoc} */
        @Override public void checkValue(Object val) {
            TestValue obj = (TestValue)val;

            assertEquals(expVal, obj.val);
        }
    }
}