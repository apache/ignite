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

package org.apache.ignite.ml.dataset.impl.cache.util;

import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link IteratorWithConcurrentModificationChecker}.
 */
public class IteratorWithConcurrentModificationCheckerTest {
    /** */
    @Test(expected = ConcurrentModificationException.class)
    public void testNextWhenIteratorHasLessElementsThanExpected() {
        List<Integer> list = Arrays.asList(1, 2, 3);

        Iterator<Integer> iter = new IteratorWithConcurrentModificationChecker<>(list.iterator(), 4, "Exception");

        assertEquals(Integer.valueOf(1), iter.next());
        assertEquals(Integer.valueOf(2), iter.next());
        assertEquals(Integer.valueOf(3), iter.next());

        iter.next(); // Should throw an exception.
    }

    /** */
    @Test(expected = ConcurrentModificationException.class)
    public void testNextWhenIteratorHasMoreElementsThanExpected() {
        List<Integer> list = Arrays.asList(1, 2, 3);

        Iterator<Integer> iter = new IteratorWithConcurrentModificationChecker<>(list.iterator(), 2, "Exception");

        assertEquals(Integer.valueOf(1), iter.next());
        assertEquals(Integer.valueOf(2), iter.next());

        iter.next(); // Should throw an exception.
    }

    /** */
    @Test(expected = ConcurrentModificationException.class)
    public void testHasNextWhenIteratorHasLessElementsThanExpected() {
        List<Integer> list = Arrays.asList(1, 2, 3);

        Iterator<Integer> iter = new IteratorWithConcurrentModificationChecker<>(list.iterator(), 4, "Exception");

        assertTrue(iter.hasNext());
        iter.next();
        assertTrue(iter.hasNext());
        iter.next();
        assertTrue(iter.hasNext());
        iter.next();

        iter.hasNext(); // Should throw an exception.
    }

    /** */
    @Test(expected = ConcurrentModificationException.class)
    public void testHasNextWhenIteratorHasMoreElementsThanExpected() {
        List<Integer> list = Arrays.asList(1, 2, 3);

        Iterator<Integer> iter = new IteratorWithConcurrentModificationChecker<>(list.iterator(), 2, "Exception");

        assertTrue(iter.hasNext());
        iter.next();
        assertTrue(iter.hasNext());
        iter.next();

        iter.hasNext(); // Should throw an exception.
    }
}
