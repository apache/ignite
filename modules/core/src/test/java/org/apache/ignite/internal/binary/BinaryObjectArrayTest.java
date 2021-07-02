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
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class BinaryObjectArrayTest extends GridCommonAbstractTest {
    /**
     *
     */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Checking we able to get TestClass[], not just Object[].
     */
    @Test
    public void testArrayObject() throws Exception {
        Ignite ignite = startGrid();

        IgniteCache<Integer, TestClass[]> cache = ignite.createCache("cache");

        int key = 1;

        cache.put(key, generate());

        check(cache.get(key));
    }

    /**
     * Checking we able to get TestClass[], not just Object[].
     */
    @Test
    public void testArrayField() throws Exception {
        Ignite ignite = startGrid();

        IgniteCache<Integer, TestClassHolder> cache = ignite.createCache("cache");

        int key = 1;

        cache.put(key, new TestClassHolder(generate()));

        check(cache.get(key).arr);
    }

    /**
     *
     */
    private TestClass[] generate() {
        return new TestClass[] {
            new TestClass42(),
            new TestClass43(),
            new TestClass(44) {
            }};
    }

    /**
     * @param arr Array.
     */
    private void check(TestClass[] arr) {
        assertEquals(TestClass[].class, arr.getClass());
        assertEquals(TestClass42.class, arr[0].getClass());
        assertEquals(TestClass43.class, arr[1].getClass());

        assertEquals(new Integer(42), arr[0].field);
        assertEquals(new Integer(43), arr[1].field);
        assertEquals(new Integer(44), arr[2].field);
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