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
 *
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * Atomic bitset array.
 */
class AtomicBitSet {
    /**
     * Container of bits.
     */
    private final AtomicIntegerArray arr;

    /**
     * Size of array of bits.
     */
    private final int size;

    /**
     * @param size Size of array.
     */
    public AtomicBitSet(int size) {
        this.size = size;

        arr = new AtomicIntegerArray((size + 31) >>> 5);
    }

    /**
     * @return Size of bitset in bits.
     */
    public int size() {
        return size;
    }

    /**
     * @param idx Index in bitset array.
     * @return {@code true} if the bit is set.
     */
    public boolean check(long idx) {
        if (idx >= size)
            return false;

        int bit = 1 << idx;
        int bucket = (int)(idx >>> 5);

        int cur = arr.get(bucket);

        return (cur & bit) == bit;
    }

    /**
     * @param off Bit position to change.
     * @return {@code true} if bit has been set,
     * {@code false} if bit changed by another thread or out of range.
     */
    public boolean touch(long off) {
        if (off >= size)
            return false;

        int bit = 1 << off;
        int bucket = (int)(off >>> 5);

        while (true) {
            int cur = arr.get(bucket);
            int val = cur | bit;

            if (cur == val)
                return false;

            if (arr.compareAndSet(bucket, cur, val))
                return true;
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder sb = new StringBuilder();

        for (int j = 0; j < arr.length(); j++)
            sb.append(Integer.toBinaryString(arr.get(j)));

        for (int n = sb.length(); n < size; n++)
            sb.insert(0, "0");

        return "AtomicBitSet [" +
            "arr=" + sb +
            ", size=" + size +
            ']';
    }
}
