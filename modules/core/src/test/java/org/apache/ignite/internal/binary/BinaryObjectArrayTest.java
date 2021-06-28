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
     * Checking we able to get TestClass[], not just Object[].
     */
    @Test
    public void testArray() throws Exception {
        Ignite ign = startGrid();

        IgniteCache<Integer, TestClass[]> cache = ign.createCache("cache");

        int key = 1;

        cache.put(key, new TestClass[] {new TestClass(42), new TestClass(43)});

        TestClass[] arr = cache.get(key);

        assertEquals(TestClass[].class, arr.getClass());

        for (TestClass obj : arr)
            assertTrue(obj.field == 42 || obj.field == 43);
    }

    /**
     *
     */
    private static class TestClass{
        /** */
        private final Integer field;

        /**
         * @param field Field.
         */
        public TestClass(Integer field) {
            this.field = field;
        }
    }
}