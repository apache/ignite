package org.apache.ignite.internal.cluster.graph;

import java.util.BitSet;
import java.util.Iterator;

/**
 * Iterator over set bits in {@link BitSet}.
 */
public class BitSetIterator implements Iterator<Integer> {
    /** Bitset. */
    private final BitSet bitSet;

    /** Current index. */
    private int idx = -1;

    /**
     * @param bitSet Bitset.
     */
    public BitSetIterator(BitSet bitSet) {
        this.bitSet = bitSet;

        advance();
    }

    /**
     * Find index of the next set bit.
     */
    private void advance() {
        idx = bitSet.nextSetBit(idx + 1);
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        return idx != -1;
    }

    /** {@inheritDoc} */
    @Override public Integer next() {
        int result = idx;

        advance();

        return result;
    }
}
