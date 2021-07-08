/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ArrayDequeTest {

    @Test
    public void testPeekPoll() {
        ArrayDeque<Integer> list = new ArrayDeque<>();
        for (int i = 0; i < 10; i++) {
            list.add(i);
        }
        assertEquals(0, (int) list.peekFirst());
        assertEquals(9, (int) list.peekLast());
        assertEquals(0, (int) list.peekFirst());
        assertEquals(9, (int) list.peekLast());
        assertEquals(10, list.size());

        for (int i = 0; i < 10; i++) {
            assertEquals(i, (int) list.get(i));
        }

        assertEquals(0, (int) list.pollFirst());
        assertEquals(1, (int) list.peekFirst());
        assertEquals(9, (int) list.pollLast());
        assertEquals(8, (int) list.peekLast());

        assertEquals(1, (int) list.pollFirst());
        assertEquals(2, (int) list.peekFirst());
        assertEquals(8, (int) list.pollLast());
        assertEquals(7, (int) list.peekLast());

        while (!list.isEmpty()) {
            list.pollFirst();
        }

        try {
            list.pollFirst();
            fail();
        }
        catch (IndexOutOfBoundsException e) {
            assertTrue(true);
        }

        try {
            list.pollLast();
            fail();
        }
        catch (IndexOutOfBoundsException e) {
            assertTrue(true);
        }
    }
}
