package org.apache.ignite.internal.util;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class ImmutableBitSetTest extends GridCommonAbstractTest {

    @Test
    public void testSizeIsEmpty() {
        BitSet bitSet = new BitSet(1024);

        ImmutableBitSet immutable = new ImmutableBitSet(bitSet);

        assertEquals(0, immutable.size());
        assertTrue(immutable.isEmpty());

        bitSet = new BitSet(1024);

        bitSet.set(1);
        bitSet.set(10);
        bitSet.set(1025);

        immutable = new ImmutableBitSet(bitSet);

        assertEquals(3, immutable.size());
        assertFalse(immutable.isEmpty());
    }

    @Test
    public void testItetator() {
        BitSet bitSet = new BitSet(1024);

        bitSet.set(1);
        bitSet.set(10);
        bitSet.set(10);
        bitSet.set(11);
        bitSet.set(1025);

        ImmutableBitSet immutable = new ImmutableBitSet(bitSet);

        Iterator<Integer> iterator = immutable.iterator();

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

        for(Integer i : immutable)
            list.add(i);

        assertEquals(4, list.size());

        assertEquals(1, (int)list.get(0));
        assertEquals(10, (int)list.get(1));
        assertEquals(11, (int)list.get(2));
        assertEquals(1025, (int)list.get(3));
    }

    @Test
    public void testToArray() {
        BitSet bitSet = new BitSet(1024);

        ImmutableBitSet immutable = new ImmutableBitSet(bitSet);

        assertEquals(0, immutable.toArray().length);

        bitSet = new BitSet(1024);

        bitSet.set(1);
        bitSet.set(10);
        bitSet.set(10);
        bitSet.set(11);
        bitSet.set(1025);

        immutable = new ImmutableBitSet(bitSet);

        Object[] arr = immutable.toArray();

        assertEquals(4, arr.length);

        assertEquals(1, (int)arr[0]);
        assertEquals(10, (int)arr[1]);
        assertEquals(11, (int)arr[2]);
        assertEquals(1025, (int)arr[3]);

        Integer[] input = new Integer[1];

        Integer[] output = immutable.toArray(input);

        assertNotSame(input, output);

        assertEquals(4, arr.length);

        assertEquals(1, arr[0]);
        assertEquals(10, arr[1]);
        assertEquals(11, arr[2]);
        assertEquals(1025, arr[3]);

        input = new Integer[6];

        output = immutable.toArray(input);

        assertSame(input, output);

        assertEquals(6, output.length);

        assertEquals(1, (int)output[0]);
        assertEquals(10, (int)output[1]);
        assertEquals(11, (int)output[2]);
        assertEquals(1025, (int)output[3]);
        assertNull(output[4]);
        assertNull(output[5]);
    }

}
