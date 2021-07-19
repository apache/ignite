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

package org.apache.ignite.internal.binary;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_STORE_CUSTOM_ARRAY_TO_BINARY_AS_ARRAY;

/**
 *
 */
public class BinaryObjectArrayTest extends GridCommonAbstractTest {
    /** Key. */
    private static final int KEY = 1;

    /**
     *
     */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Checking we getting just Object[] as it was before the {@link BinaryObjectArrayWrapper} invention.
     */
    @Test
    @WithSystemProperty(key = IGNITE_STORE_CUSTOM_ARRAY_TO_BINARY_AS_ARRAY, value = "true")
    public void testArrayObjectLegacy() throws Exception {
        testArrayObject();
    }

    /**
     * Checking we able to get TestClass[], not just Object[].
     */
    @Test
    public void testArrayObjectWrapper() throws Exception {
        testArrayObject();
    }

    /**
     *
     */
    private void testArrayObject() throws Exception {
        Ignite ignite = startGrid();

        IgniteCache<Integer, TestClass[]> cache = ignite.createCache("cache");

        cache.put(KEY, generate());

        check(cache.get(KEY));
    }

    /**
     * Checking we able to get TestClass[], not just Object[].
     */
    @Test
    public void testArrayField() throws Exception {
        Ignite ignite = startGrid();

        IgniteCache<Integer, TestClassHolder> cache = ignite.createCache("cache");

        cache.put(KEY, new TestClassHolder(generate()));

        check(cache.get(KEY).arr);
    }

    /**
     *
     */
    private TestClass[] generate() {
        return new TestClass[] {
            new TestClass42(),
            new TestClass43(),
            new TestClass(44) {
                // No-op.
            }
        };
    }

    /**
     * @param arr Array.
     */
    private void check(Object[] arr) {
        if (IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_STORE_CUSTOM_ARRAY_TO_BINARY_AS_ARRAY))
            assertEquals(Object[].class, arr.getClass()); // Legacy data.
        else
            assertEquals(TestClass[].class, arr.getClass());

        assertEquals(TestClass42.class, arr[0].getClass());
        assertEquals(TestClass43.class, arr[1].getClass());

        assertEquals(new Integer(42), ((TestClass)arr[0]).field);
        assertEquals(new Integer(43), ((TestClass)arr[1]).field);
        assertEquals(new Integer(44), ((TestClass)arr[2]).field);
    }

    /**
     *
     */
    private abstract static class TestClass {
        /** */
        private final Integer field;

        /**
         * @param field Field.
         */
        protected TestClass(Integer field) {
            this.field = field;
        }
    }

    /**
     *
     */
    private static class TestClass42 extends TestClass {
        /**
         *
         */
        public TestClass42() {
            super(42);
        }
    }

    /**
     *
     */
    private static class TestClass43 extends TestClass {
        /**
         *
         */
        public TestClass43() {
            super(43);
        }
    }

    /**
     *
     */
    private static class TestClassHolder {
        /** */
        private final TestClass[] arr;

        /**
         * @param arr Array.
         */
        public TestClassHolder(TestClass[] arr) {
            this.arr = arr;
        }
    }
}