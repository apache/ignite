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

package org.apache.ignite.internal.cluster.graph;

import java.util.BitSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

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
    @Override public Integer next() throws NoSuchElementException {
        if (idx == -1)
            throw new NoSuchElementException();

        int res = idx;

        advance();

        return res;
    }
}
