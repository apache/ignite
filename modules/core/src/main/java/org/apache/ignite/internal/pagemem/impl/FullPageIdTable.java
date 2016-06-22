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

package org.apache.ignite.internal.pagemem.impl;


import org.apache.ignite.internal.pagemem.DirectMemoryUtils;
import org.apache.ignite.internal.mem.OutOfMemoryException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.IgniteSystemProperties.*;

/**
 *
 */
public class FullPageIdTable {
    /** Load factor. */
    private static final int LOAD_FACTOR = getInteger(IGNITE_LONG_LONG_HASH_MAP_LOAD_FACTOR, 2);

    /** */
    private static final int BYTES_PER_ENTRY = /*page ID*/8 + /*cache ID*/4 + /*pointer*/8;

    /** */
    private static final FullPageId EMPTY_FULL_PAGE_ID = new FullPageId(0, 0);

    /** */
    private static final long EMPTY_PAGE_ID = EMPTY_FULL_PAGE_ID.pageId();

    /** */
    private static final int EMPTY_CACHE_ID = EMPTY_FULL_PAGE_ID.cacheId();

    /** */
    private static final long REMOVED_PAGE_ID = 0x8000000000000000L;

    /** */
    private static final int REMOVED_CACHE_ID = 0;

    /** */
    private static final int EQUAL = 0;

    /** */
    private static final int EMPTY = 1;

    /** */
    private static final int REMOVED = -1;

    /** */
    private static final int NOT_EQUAL = 2;

    /** Max size, in elements. */
    protected int capacity;

    /** Maximum number of steps to try before failing. */
    protected int maxSteps;

    /** Pointer to the values array. */
    protected long valPtr;

    /** */
    protected DirectMemoryUtils mem;

    /** Addressing strategy. */
    private final AddressingStrategy strategy;

    /** Specifies types of addressing. */
    public enum AddressingStrategy {
        /**
         * Insertion will search for each available cell.
         * Slower, but more suitable when used many removes/insertions.
         */
        LINEAR,

        /**
         * Insertion will search for available cell with limited steps.
         * Faster, but requires more memory to resolve collisions.
         */
        QUADRATIC
    }

    /**
     * @return Estimated memory size required for this map to store the given number of elements.
     */
    public static long requiredMemory(long elementCnt) {
        assert LOAD_FACTOR != 0;

        return elementCnt * BYTES_PER_ENTRY * LOAD_FACTOR + 4;
    }

    /**
     * @param mem Memory interface.
     * @param addr Base address.
     * @param len Allocated memory length.
     * @param clear If {@code true}, then memory is considered dirty and will be cleared. Otherwise,
     *      map will assume that the given memory region is in valid state.
     * @param stgy Addressing strategy {@link AddressingStrategy}.
     */
    public FullPageIdTable(DirectMemoryUtils mem, long addr, long len, boolean clear, AddressingStrategy stgy) {
        valPtr = addr;
        this.strategy = stgy;
        capacity = (int)((len - 4) / BYTES_PER_ENTRY);

        if (stgy == AddressingStrategy.LINEAR)
            maxSteps = capacity;
        else if (stgy == AddressingStrategy.QUADRATIC)
            maxSteps = (int) Math.sqrt(capacity);
        else
            throw new IllegalArgumentException("Unsupported addressing strategy: " + stgy);

        this.mem = mem;

        if (clear)
            clear();
    }

    /**
     * @return Current number of entries in the map.
     */
    public final int size() {
        return mem.readInt(valPtr);
    }

    /**
     * @return Maximum number of entries in the map. This maximum can not be always reached.
     */
    public final int capacity() {
        return capacity;
    }

    /**
     * Gets value associated with the given key.
     *
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     *
     * @return A value associated with the given key.
     */
    public long get(int cacheId, long pageId, long absent) {
        assert assertKey(cacheId, pageId);

        if (pageId == 0x000020000000001bL && cacheId == 689859866)
            U.debug("Read value at: ");

        int index = getKey(cacheId, pageId);

        if (index < 0)
            return absent;

        return valueAt(index);
    }

    /**
     * Associates the given key with the given value.
     *
     * @param cacheId Cache ID
     * @param pageId Page ID.
     * @param value Value to set.
     */
    public void put(int cacheId, long pageId, long value) {
        assert assertKey(cacheId, pageId);

        if (pageId == 0x000020000000001bL && cacheId == 689859866)
            U.debug("Set page pointer at: ");

        int index = putKey(cacheId, pageId);

        setValueAt(index, value);
    }

    /**
     * Removes key-value association for the given key.
     *
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     */
    public void remove(int cacheId, long pageId) {
        assert assertKey(cacheId, pageId);

        int index = removeKey(cacheId, pageId);

        if (pageId == 0x000020000000001bL && cacheId == 689859866)
            U.debug("Remove value at: " + index);

        if (index >= 0)
            setValueAt(index, 0);
    }

    /**
     * Find nearest value from specified position to the right.
     *
     * @param idx Index to start searching from.
     * @param absent Default value that will be returned if no values present.
     * @return Closest value to the index or {@code absent} if no values found.
     */
    public long getNearestAt(final int idx, final long absent) {
        for (int i = idx; i < capacity + idx; i++) {
            final int idx2 = i >= capacity ? i - capacity : i;

            if (isValuePresentAt(idx2))
                return valueAt(idx2);
        }

        return absent;
    }

    /**
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @return Key index.
     */
    private int putKey(int cacheId, long pageId) {
        int step = 1;

        int index = U.safeAbs(FullPageId.hashCode(cacheId, pageId)) % capacity;

        do {
            int res = testKeyAt(index, cacheId, pageId);

            if (res == EMPTY || res == REMOVED) {
                setKeyAt(index, cacheId, pageId);

                incrementSize();

                return index;
            }
            else if (res == EQUAL)
                return index;
            else
                assert res == NOT_EQUAL;

            if (strategy == AddressingStrategy.QUADRATIC)
                index += step;
            else if (strategy == AddressingStrategy.LINEAR)
                index++;

            if (index >= capacity)
                index -= capacity;
        }
        while (++step <= maxSteps);

        throw new OutOfMemoryException("No room for a new key");
    }

    /**
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @return Key index.
     */
    private int getKey(int cacheId, long pageId) {
        int step = 1;

        int index = U.safeAbs(FullPageId.hashCode(cacheId, pageId)) % capacity;

        do {
            long res = testKeyAt(index, cacheId, pageId);

            if (res == EQUAL)
                return index;
            else if (res == EMPTY)
                return -1;
            else
                assert res == REMOVED || res == NOT_EQUAL;

            if (strategy == AddressingStrategy.QUADRATIC)
                index += step;
            else if (strategy == AddressingStrategy.LINEAR)
                index++;

            if (index >= capacity)
                index -= capacity;
        } while (++step <= maxSteps);

        return -1;
    }

    /**
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @return Key index.
     */
    private int removeKey(int cacheId, long pageId) {
        int step = 1;

        int index = U.safeAbs(FullPageId.hashCode(cacheId, pageId)) % capacity;

        do {
            long res = testKeyAt(index, cacheId, pageId);

            if (res == EQUAL) {
                setKeyAt(index, REMOVED_CACHE_ID, REMOVED_PAGE_ID);

                decrementSize();

                return index;
            }
            else if (res == EMPTY)
                return -1;
            else
                assert res == REMOVED || res == NOT_EQUAL;

            if (strategy == AddressingStrategy.QUADRATIC)
                index += step;
            else if (strategy == AddressingStrategy.LINEAR)
                index++;

            if (index >= capacity)
                index -= capacity;
        }
        while (++step <= maxSteps);

        return -1;
    }

    /**
     * @param index Entry index.
     * @return Key value.
     */
    private int testKeyAt(int index, int testCacheId, long testPageId) {
        long base = valPtr + 4 + (long)index * BYTES_PER_ENTRY;

        long pageId = mem.readLong(base);
        int cacheId = mem.readInt(base + 8);

        if (pageId == REMOVED_PAGE_ID && cacheId == REMOVED_CACHE_ID)
            return REMOVED;
        else if (pageId == testPageId && cacheId == testCacheId)
            return EQUAL;
        else if(pageId == EMPTY_PAGE_ID && cacheId == EMPTY_CACHE_ID)
            return EMPTY;
        else
            return NOT_EQUAL;
    }

    /**
     * @param idx Index to test.
     * @return {@code True} if value set for index.
     */
    private boolean isValuePresentAt(final int idx) {
        long base = valPtr + 4 + (long)idx * BYTES_PER_ENTRY;

        long pageId = mem.readLong(base);
        int cacheId = mem.readInt(base + 8);

        return !((pageId == REMOVED_PAGE_ID && cacheId == REMOVED_CACHE_ID)
            || (pageId == EMPTY_PAGE_ID && cacheId == EMPTY_CACHE_ID));
    }

    /**
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @return {@code True} if checks succeeded.
     */
    private boolean assertKey(int cacheId, long pageId) {
        assert cacheId != EMPTY_CACHE_ID || pageId != EMPTY_PAGE_ID :
            "cacheId=" + cacheId + ", pageId=" + U.hexLong(pageId);

        return true;
    }

    /**
     * @param index Entry index.
     * @param cacheId Cache ID to write.
     * @param pageId Page ID to write.
     */
    private void setKeyAt(int index, int cacheId, long pageId) {
        long base = valPtr + 4 + (long)index * BYTES_PER_ENTRY;

        mem.writeLong(base, pageId);
        mem.writeLong(base + 8, cacheId);
    }

    /**
     * @param index Entry index.
     * @return Value.
     */
    private long valueAt(int index) {
        return mem.readLong(valPtr + 4 + (long)index * BYTES_PER_ENTRY + 12);
    }

    /**
     * @param index Entry index.
     * @param value Value.
     */
    private void setValueAt(int index, long value) {
        mem.writeLong(valPtr + 4 + (long)index * BYTES_PER_ENTRY + 12, value);
    }

    /**
     *
     */
    public void clear() {
        mem.setMemory(valPtr, capacity * BYTES_PER_ENTRY + 4, (byte)0);
    }

    /**
     *
     */
    private void incrementSize() {
        mem.writeInt(valPtr, mem.readInt(valPtr) + 1);
    }

    /**
     *
     */
    private void decrementSize() {
        mem.writeInt(valPtr, mem.readInt(valPtr) - 1);
    }
}
