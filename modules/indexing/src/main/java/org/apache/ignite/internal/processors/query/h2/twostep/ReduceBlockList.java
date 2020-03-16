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

package org.apache.ignite.internal.processors.query.h2.twostep;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.RandomAccess;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class ReduceBlockList<Z> extends AbstractList<Z> implements RandomAccess {
    /** */
    private final List<List<Z>> blocks;

    /** */
    private int size;

    /** */
    private final int maxBlockSize;

    /** */
    private final int shift;

    /** */
    private final int mask;

    /**
     * @param maxBlockSize Max block size.
     */
    public ReduceBlockList(int maxBlockSize) {
        assert U.isPow2(maxBlockSize);

        this.maxBlockSize = maxBlockSize;

        shift = Integer.numberOfTrailingZeros(maxBlockSize);
        mask = maxBlockSize - 1;

        blocks = new ArrayList<>();
        blocks.add(new ArrayList<Z>());
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return size;
    }

    /** {@inheritDoc} */
    @Override public boolean add(Z z) {
        size++;

        List<Z> lastBlock = lastBlock();

        lastBlock.add(z);

        if (lastBlock.size() == maxBlockSize)
            blocks.add(new ArrayList<Z>());

        return true;
    }

    /** {@inheritDoc} */
    @Override public Z get(int idx) {
        return blocks.get(idx >>> shift).get(idx & mask);
    }

    /**
     * @return Last block.
     */
    public List<Z> lastBlock() {
        return blocks.get(blocks.size() - 1);
    }

    /**
     * @return Evicted block.
     */
    public List<Z> evictFirstBlock() {
        // Remove head block.
        List<Z> res = blocks.remove(0);

        size -= res.size();

        return res;
    }
}
