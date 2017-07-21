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

package org.apache.ignite.lang.utils;

import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.util.GridBoundedConcurrentOrderedMap;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Test for {@link GridBoundedConcurrentOrderedMap}.
 */
@GridCommonTest(group = "Lang")
public class GridBoundedConcurrentOrderedMapSelfTest extends GridCommonAbstractTest {
    /**
     *
     */
    public void testEvictionSingleElement() {
        SortedMap<Integer,String> m = new GridBoundedConcurrentOrderedMap<>(1);

        m.put(0, "0");

        assertEquals(1, m.size());

        for (int i = 1; i <= 10; i++) {
            m.put(i, Integer.toString(i));

            assertEquals(1, m.size());
        }

        assertEquals(1, m.size());
        assertEquals(Integer.valueOf(10), m.lastKey());
    }

    /**
     *
     */
    public void testEvictionListener() {
        GridBoundedConcurrentOrderedMap<Integer,String> m = new GridBoundedConcurrentOrderedMap<>(1);

        final AtomicInteger evicted = new AtomicInteger();

        m.evictionListener(new CI2<Integer, String>() {
            @Override public void apply(Integer k, String v) {
                assertEquals(Integer.toString(k), v);
                assertEquals(evicted.getAndIncrement(), k.intValue());
            }
        });

        m.put(0, "0");

        assertEquals(1, m.size());

        for (int i = 1; i <= 10; i++) {
            m.put(i, Integer.toString(i));

            assertEquals(1, m.size());
        }

        assertEquals(1, m.size());
        assertEquals(10, m.lastKey().intValue());
        assertEquals(10, evicted.get());
    }
}