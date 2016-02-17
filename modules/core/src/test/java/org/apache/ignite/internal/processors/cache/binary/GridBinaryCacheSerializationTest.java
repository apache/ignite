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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.lang.reflect.Method;

/**
 *
 */
public class GridBinaryCacheSerializationTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "cache_name";

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

    /**
     * Test that calling {@link Ignition#localIgnite()}
     * is safe for binary marshaller.
     *
     * @throws Exception
     */
    public void testPutGet() throws Exception {
        final IgniteCache<Integer, MyObj> cache = startGrid().getOrCreateCache(CACHE_NAME);

        final MyObj one = new MyObj("one");
        final MyObj two = new MyObj("two");
        final MyObj three = new MyObj("three");

        cache.put(1, one);
        cache.put(2, two);
        cache.put(3, three);

        final MyObj loadedOne = cache.get(1);
        final MyObj loadedTwo = cache.get(2);
        final MyObj loadedThree = cache.get(3);

        assert one.equals(loadedOne);
        assert two.equals(loadedTwo);
        assert three.equals(loadedThree);

    }

    /**
     * Test obj.
     */
    private static class MyObj {

        /** */
        final String val;

        /** */
        private MyObj(final String val) {
            this.val = val;
        }

        private Object readResolve() {
            Ignition.localIgnite();
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
