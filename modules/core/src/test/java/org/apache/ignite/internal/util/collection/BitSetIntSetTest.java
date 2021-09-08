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

package org.apache.ignite.internal.util.collection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 *
 */
public class BitSetIntSetTest extends GridCommonAbstractTest {
    /**
     *
     */
    @Test
    public void testSizeIsEmpty() {
        sizeIsEmpty(0);
        sizeIsEmpty(1024);
    }

    /**
     *
     */
    private void sizeIsEmpty(int initCap) {
        IntSet bitSetIntSet = initCap != 0 ? new BitSetIntSet(initCap) : new BitSetIntSet();

        assertEquals(0, bitSetIntSet.size());
        assertTrue(bitSetIntSet.isEmpty());

        bitSetIntSet = new BitSetIntSet();

        assertTrue(bitSetIntSet.add(1));
        assertEquals(1, bitSetIntSet.size());
        assertTrue(bitSetIntSet.add(10));
        assertEquals(2, bitSetIntSet.size());
        assertTrue(bitSetIntSet.add(1025));
        assertEquals(3, bitSetIntSet.size());

        assertEquals(3, bitSetIntSet.size());
        assertFalse(bitSetIntSet.isEmpty());
    }

    /** */
    @Test
    public void testItetator() {
        testIterator(0);
        testIterator(1024);
    }

    @Test(expected = NoSuchElementException.class)
    public void shouldThrowExceptionIfHasNotNextElement() {
        IntSet intSet = new BitSetIntSet(2);
        intSet.add(1);
        intSet.add(2);
        Iterator<Integer> iterator = intSet.iterator();

        iterator.next();
        iterator.next();
        iterator.next();
    }

    @Test
    public void hasNextShouldBeIdempotent() {
        IntSet intSet = new BitSetIntSet(3);
        intSet.add(1);
        intSet.add(2);
        intSet.add(3);
        Iterator<Integer> iter = intSet.iterator();

        assertEquals(1, (int) iter.next());

        iter.hasNext();
        iter.hasNext();
        iter.hasNext();
        assertEquals(2, (int) iter.next());

        iter.hasNext();
        iter.hasNext();
        assertEquals(3, (int) iter.next());
    }

    @Test
    public void toIntArray() {
        IntSet emptySet = new BitSetIntSet();
        int[] emptyArr = emptySet.toIntArray();
        assertThat(emptyArr.length, is(0));

        IntSet withGapsSet = new BitSetIntSet(100, Arrays.asList(43, 23, 53));
        int[] arr = withGapsSet.toIntArray();
        assertThat(arr.length, is(3));
        assertThat(arr[0], is(23));
        assertThat(arr[1], is(43));
        assertThat(arr[2], is(53));
    }

    /** */
    private void testIterator(int initCap) {
        IntSet bitSet = initCap != 0 ? new BitSetIntSet(initCap) : new BitSetIntSet();

        for (Integer ignored : bitSet)
            fail("BitSet is empty, shouldn't be invoked.");

        assertFalse(bitSet.iterator().hasNext());

        try {
            bitSet.iterator().next();

            fail("NoSuchElement expected.");
        }
        catch (NoSuchElementException ignored) {

        }

        assertTrue(bitSet.add(0));
        assertTrue(bitSet.add(1));

        assertTrue(bitSet.add(10));
        assertFalse(bitSet.add(10));

        assertTrue(bitSet.add(11));
        assertTrue(bitSet.add(1025));

        Iterator<Integer> iterator = bitSet.iterator();

        assertTrue(iterator.hasNext());
        assertEquals(0, (int)iterator.next());

        assertTrue(iterator.hasNext());
        assertEquals(1, (int)iterator.next());

        assertTrue(iterator.hasNext());
        assertEquals(10, (int)iterator.next());

        assertTrue(iterator.hasNext());
        assertEquals(11, (int)iterator.next());

        assertTrue(iterator.hasNext());
        assertEquals(1025, (int)iterator.next());

        assertFalse(iterator.hasNext());

        List<Integer> list = new ArrayList<>();

        for (Integer i : bitSet)
            list.add(i);

        assertEquals(5, list.size());

        assertEquals(0, (int)list.get(0));
        assertEquals(1, (int)list.get(1));
        assertEquals(10, (int)list.get(2));
        assertEquals(11, (int)list.get(3));
        assertEquals(1025, (int)list.get(4));

        assertFalse(bitSet.remove(2));
        assertEquals(5, bitSet.size());

        assertTrue(bitSet.remove(1));
        assertFalse(bitSet.remove(1));
        assertEquals(4, bitSet.size());
        assertFalse(bitSet.isEmpty());

        assertTrue(bitSet.remove(10));
        assertFalse(bitSet.remove(10));
        assertEquals(3, bitSet.size());
        assertFalse(bitSet.isEmpty());
    }

    /** */
    @Test
    public void testContains() {
        IntSet intSet = new BitSetIntSet();

        intSet.add(Integer.valueOf(1));
        intSet.add(10);
        intSet.add(10);
        intSet.add(11);
        intSet.add(1025);

        assertTrue(intSet.contains(1));
        assertFalse(intSet.contains(2));
        assertFalse(intSet.contains(3));
        assertFalse(intSet.contains(4));
        assertTrue(intSet.contains(10));
        assertTrue(intSet.contains(11));
        assertFalse(intSet.contains(1024));
        assertTrue(intSet.contains(1025));
    }

    /**
     *
     */
    @Test
    public void testContainsAll() {
        IntSet intSet = new BitSetIntSet();

        intSet.add(1);
        intSet.add(10);
        intSet.add(10);
        intSet.add(11);
        intSet.add(1025);

        assertTrue(intSet.containsAll(new ArrayList<Integer>() {{
            add(1);
            add(10);
        }}));

        assertFalse(intSet.containsAll(new ArrayList<Integer>() {{
            add(1);
            add(10);
            add(11);
            add(1025);
            add(1026);
        }}));

        assertFalse(intSet.containsAll(new ArrayList<Integer>() {{
            add(1);
            add(10);
            add(12);
        }}));
    }

    /**
     *
     */
    @Test
    public void testAddAllRemoveAllRetainAll() {
        IntSet intSet = new BitSetIntSet();

        intSet.add(1);
        intSet.add(10);
        intSet.add(10);
        intSet.add(11);
        intSet.add(1025);

        assertFalse(intSet.addAll(new ArrayList<Integer>() {{
            add(1);
            add(10);
        }}));

        assertEquals(4, intSet.size());

        assertTrue(intSet.addAll(new ArrayList<Integer>() {{
            add(1);
            add(10);
            add(11);
            add(1025);
            add(1026);
        }}));

        assertEquals(5, intSet.size());

        try {
            intSet.retainAll(new ArrayList<Integer>() {{
                add(10);
                add(1025);
            }});

            fail("retainAll is not supported");
        }
        catch (UnsupportedOperationException ignored) {
            // Ignored.
        }
    }

    /**
     *
     */
    @Test
    public void testToArray() {
        IntSet intSet = new BitSetIntSet();

        assertEquals(0, intSet.toArray().length);

        intSet.add(1);
        intSet.add(10);
        intSet.add(10);
        intSet.add(11);
        intSet.add(1025);

        Object[] arr = intSet.toArray();

        assertEquals(4, arr.length);

        assertEquals(1, (int)arr[0]);
        assertEquals(10, (int)arr[1]);
        assertEquals(11, (int)arr[2]);
        assertEquals(1025, (int)arr[3]);

        Integer[] input = new Integer[1];

        Integer[] output = intSet.toArray(input);

        assertNotSame(input, output);

        assertEquals(4, arr.length);

        assertEquals(1, arr[0]);
        assertEquals(10, arr[1]);
        assertEquals(11, arr[2]);
        assertEquals(1025, arr[3]);

        input = new Integer[6];

        output = intSet.toArray(input);

        assertSame(input, output);

        assertEquals(6, output.length);

        assertEquals(1, (int)output[0]);
        assertEquals(10, (int)output[1]);
        assertEquals(11, (int)output[2]);
        assertEquals(1025, (int)output[3]);
        assertNull(output[4]);
        assertNull(output[5]);
    }

    /**
     *
     */
    @Test
    public void testInvalidValues() {
        IntSet intSet = new BitSetIntSet();

        try {
            intSet.add(null);
            fail("add should fail here");
        }
        catch (IllegalArgumentException ignored) {
            // Ignored.
        }

        try {
            intSet.add(-1);
            fail("add should fail here");
        }
        catch (IllegalArgumentException ignored) {
            // Ignored.
        }

        try {
            intSet.contains(null);
            fail("contains should fail here");
        }
        catch (NullPointerException ignored) {
            // Ignored.
        }

        assertFalse(intSet.contains(-1));
        assertFalse(intSet.remove(null));
        assertFalse(intSet.remove(-1));
    }
}
