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

package org.apache.ignite.internal.util;

import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Class for testing {@link GridMutableLong}.
 */
public class GridMutableLongTest extends GridCommonAbstractTest {
    /**
     * Checking the correctness of object initialization.
     */
    @Test
    public void testInitialization() {
        assertEquals(0, new GridMutableLong().get());
        assertEquals(5, new GridMutableLong(5).get());
        assertEquals(-25, new GridMutableLong(-25).get());
        assertEquals(Long.MIN_VALUE, new GridMutableLong(Long.MIN_VALUE).get());
        assertEquals(Long.MAX_VALUE, new GridMutableLong(Long.MAX_VALUE).get());
    }

    /**
     * Checking the correctness of the string representation of an object.
     */
    @Test
    public void testToString() {
        assertEquals(Long.toString(0), new GridMutableLong().toString());
        assertEquals(Long.toString(5), new GridMutableLong(5).toString());
        assertEquals(Long.toString(-25), new GridMutableLong(-25).toString());
    }

    /**
     * Checking the correctness of the {@link GridMutableLong#incrementAndGet()}.
     */
    @Test
    public void testIncrementAndGet() {
        for (long l : new long[] {0, 5, -25}) {
            GridMutableLong mutableLong = new GridMutableLong(l);

            for (int i = 0; i < 5; i++)
                assertEquals(++l, mutableLong.incrementAndGet());
        }
    }
}
