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

package org.apache.ignite.internal.binary.streams;

import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class BinaryAbstractOutputStreamTest extends GridCommonAbstractTest {
    /**
     *
     */
    public void testCapacity() {
        assertEquals(256, BinaryAbstractOutputStream.capacity(0, 1));

        assertEquals(256, BinaryAbstractOutputStream.capacity(256, 1));

        assertEquals(256, BinaryAbstractOutputStream.capacity(256, 256));

        assertEquals(512, BinaryAbstractOutputStream.capacity(256, 257));

        assertEquals(512, BinaryAbstractOutputStream.capacity(512, 256));

        assertEquals(1024, BinaryAbstractOutputStream.capacity(512, 513));

        assertEquals(2048, BinaryAbstractOutputStream.capacity(1024, 1025));

        assertEquals(4096, BinaryAbstractOutputStream.capacity(2048, 2049));

        assertEquals(8192, BinaryAbstractOutputStream.capacity(4096, 4097));

        assertEquals(16384, BinaryAbstractOutputStream.capacity(8192, 8193));

        assertEquals(32768, BinaryAbstractOutputStream.capacity(16384, 16385));

        assertEquals(65536, BinaryAbstractOutputStream.capacity(32768, 32769));

        assertEquals(131072, BinaryAbstractOutputStream.capacity(65536, 65537));

        assertEquals(262144, BinaryAbstractOutputStream.capacity(131072, 131073));

        assertEquals(524288, BinaryAbstractOutputStream.capacity(262144, 262145));

        assertEquals(1048576, BinaryAbstractOutputStream.capacity(524288, 524289));

        assertEquals(2097152, BinaryAbstractOutputStream.capacity(1048576, 1048577));

        assertEquals(4194304, BinaryAbstractOutputStream.capacity(2097152, 2097153));

        assertEquals(8388608, BinaryAbstractOutputStream.capacity(4194304, 4194305));

        assertEquals(16777216, BinaryAbstractOutputStream.capacity(8388608, 8388609));

        assertEquals(33554432, BinaryAbstractOutputStream.capacity(16777216, 16777217));

        assertEquals(67108864, BinaryAbstractOutputStream.capacity(33554432, 33554433));

        assertEquals(134217728, BinaryAbstractOutputStream.capacity(67108864, 67108865));

        assertEquals(268435456, BinaryAbstractOutputStream.capacity(134217728, 134217729));

        assertEquals(536870912, BinaryAbstractOutputStream.capacity(268435456, 268435457));

        assertEquals(1073741824, BinaryAbstractOutputStream.capacity(536870912, 536870913));

        final int MAX_SIZE = BinaryAbstractOutputStream.MAX_ARRAY_SIZE;

        assertEquals(MAX_SIZE, BinaryAbstractOutputStream.capacity(1073741824, 1073741825));

        assertEquals(MAX_SIZE, BinaryAbstractOutputStream.capacity(0, 1073741825));

        assertEquals(MAX_SIZE, BinaryAbstractOutputStream.capacity(1073741824, 1500000000));

        assertEquals(MAX_SIZE, BinaryAbstractOutputStream.capacity(1073741824, 2000000000));

        assertEquals(MAX_SIZE, BinaryAbstractOutputStream.capacity(1073741824, Integer.MAX_VALUE - 9));

        assertEquals(MAX_SIZE, BinaryAbstractOutputStream.capacity(1073741824, Integer.MAX_VALUE - 8));

        try {
            assertEquals(MAX_SIZE, BinaryAbstractOutputStream.capacity(0, Integer.MAX_VALUE - 7));

            fail();
        }
        catch (IllegalArgumentException ignored) {
            // Expected exception.
        }
    }
}
