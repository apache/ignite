/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

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
        BitSetIntSet bitSetIntSet = initCap != 0 ? new BitSetIntSet(initCap) : new BitSetIntSet();

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

    /** */
    private void testIterator(int initCap) {
        BitSetIntSet bitSet = initCap != 0 ? new BitSetIntSet(initCap) : new BitSetIntSet();

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

        for(Integer i : bitSet)
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
        BitSetIntSet bitSetInt = new BitSetIntSet();

        bitSetInt.add(1);
        bitSetInt.add(10);
        bitSetInt.add(10);
        bitSetInt.add(11);
        bitSetInt.add(1025);


        assertTrue(bitSetInt.contains(1));
        assertFalse(bitSetInt.contains(2));
        assertFalse(bitSetInt.contains(3));
        assertFalse(bitSetInt.contains(4));
        assertTrue(bitSetInt.contains(10));
        assertTrue(bitSetInt.contains(11));
        assertFalse(bitSetInt.contains(1024));
        assertTrue(bitSetInt.contains(1025));
    }

    /**
     *
     */
    @Test
    public void testContainsAll() {
        BitSetIntSet bitSetInt = new BitSetIntSet();

        bitSetInt.add(1);
        bitSetInt.add(10);
        bitSetInt.add(10);
        bitSetInt.add(11);
        bitSetInt.add(1025);

        assertTrue(bitSetInt.containsAll(new ArrayList<Integer>() {{
            add(1);
            add(10);
        }}));

        assertFalse(bitSetInt.containsAll(new ArrayList<Integer>() {{
            add(1);
            add(10);
            add(11);
            add(1025);
            add(1026);
        }}));

        assertFalse(bitSetInt.containsAll(new ArrayList<Integer>() {{
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
        BitSetIntSet bitSetInt = new BitSetIntSet();

        bitSetInt.add(1);
        bitSetInt.add(10);
        bitSetInt.add(10);
        bitSetInt.add(11);
        bitSetInt.add(1025);

        assertFalse(bitSetInt.addAll(new ArrayList<Integer>() {{
            add(1);
            add(10);
        }}));

        assertEquals(4, bitSetInt.size());

        assertTrue(bitSetInt.addAll(new ArrayList<Integer>() {{
            add(1);
            add(10);
            add(11);
            add(1025);
            add(1026);
        }}));

        assertEquals(5, bitSetInt.size());

        try {
            bitSetInt.retainAll(new ArrayList<Integer>() {{
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
        BitSetIntSet bitSetInt = new BitSetIntSet();

        assertEquals(0, bitSetInt.toArray().length);

        bitSetInt.add(1);
        bitSetInt.add(10);
        bitSetInt.add(10);
        bitSetInt.add(11);
        bitSetInt.add(1025);

        Object[] arr = bitSetInt.toArray();

        assertEquals(4, arr.length);

        assertEquals(1, (int)arr[0]);
        assertEquals(10, (int)arr[1]);
        assertEquals(11, (int)arr[2]);
        assertEquals(1025, (int)arr[3]);

        Integer[] input = new Integer[1];

        Integer[] output = bitSetInt.toArray(input);

        assertNotSame(input, output);

        assertEquals(4, arr.length);

        assertEquals(1, arr[0]);
        assertEquals(10, arr[1]);
        assertEquals(11, arr[2]);
        assertEquals(1025, arr[3]);

        input = new Integer[6];

        output = bitSetInt.toArray(input);

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
        BitSetIntSet bitSetInt = new BitSetIntSet();

        try {
            bitSetInt.add(null);
            fail("add should fail here");
        }
        catch (UnsupportedOperationException ignored) {
            // Ignored.
        }

        try {
            bitSetInt.add(-1);
            fail("add should fail here");
        }
        catch (UnsupportedOperationException ignored) {
            // Ignored.
        }

        try {
            bitSetInt.contains(null);
            fail("contains should fail here");
        }
        catch (UnsupportedOperationException ignored) {
            // Ignored.
        }

        try {
            bitSetInt.contains(-1);
            fail("contains should fail here");
        }
        catch (UnsupportedOperationException ignored) {
            // Ignored.
        }

        try {
            bitSetInt.remove(null);
            fail("remove should fail here");
        }
        catch (UnsupportedOperationException ignored) {
            // Ignored.
        }

        try {
            bitSetInt.remove(-1);
            fail("remove should fail here");
        }
        catch (UnsupportedOperationException ignored) {
            // Ignored.
        }
    }
}
