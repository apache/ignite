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

package org.apache.ignite.internal.processors.query.stat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Unit tests for Hasher.
 */
public class HasherSelfTest extends GridCommonAbstractTest {
    /**
     * Create hash by arrays with different size and check that hash differ.
     */
    @Test
    public void testCornerCaseSizedArray() {
        Hasher h = new Hasher();
        List<Long> list = new ArrayList<>();

        list.add(h.fastHash(new byte[]{}));
        list.add(h.fastHash(new byte[]{1}));
        list.add(h.fastHash(new byte[]{1, 2, 3, 4}));
        list.add(h.fastHash(new byte[]{1, 2, 3, 4, 5, 6, 7}));
        list.add(h.fastHash(new byte[]{1, 2, 3, 4, 5, 6, 7, 8}));
        list.add(h.fastHash(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9}));
        list.add(h.fastHash(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}));
        list.add(h.fastHash(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}));
        list.add(h.fastHash(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}));

        Set<Long> set = new HashSet<>();
        set.addAll(list);

        assertEquals(list.size(), set.size());
    }

    /**
     * Test that hashes of few random generated arrays are the same if generated multiple times.
     */
    @Test
    public void testRandomHash() {
        Hasher h = new Hasher();
        Random r = new Random();

        int testSize = 100;
        Map<byte[], Long> hashes = new HashMap<>(testSize);

        for (int i = 0; i < testSize; i++) {
            byte arr[] = new byte[r.nextInt(1000)];
            r.nextBytes(arr);
            hashes.put(arr, h.fastHash(arr));
        }

        for (Map.Entry<byte[], Long> entry : hashes.entrySet())
            assertEquals(entry.getValue().longValue(), h.fastHash(entry.getKey()));
    }
}
