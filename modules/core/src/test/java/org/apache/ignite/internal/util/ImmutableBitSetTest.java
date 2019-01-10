package org.apache.ignite.internal.util;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
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

}
