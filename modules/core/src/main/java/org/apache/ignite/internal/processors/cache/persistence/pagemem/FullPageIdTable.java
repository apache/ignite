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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.lang.GridPredicate3;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_LONG_LONG_HASH_MAP_LOAD_FACTOR;
import static org.apache.ignite.IgniteSystemProperties.getFloat;

/**
 *
 */
public class FullPageIdTable {
    /** Load factor. */
    private static final float LOAD_FACTOR = getFloat(IGNITE_LONG_LONG_HASH_MAP_LOAD_FACTOR, 2.5f);

    /** */
    private static final int BYTES_PER_ENTRY = /*cache ID*/4 + /*partition tag*/4 + /*page ID*/8 + /*pointer*/8;

    /** */
    private static final FullPageId EMPTY_FULL_PAGE_ID = new FullPageId(0, 0);

    /** */
    private static final long EMPTY_PAGE_ID = EMPTY_FULL_PAGE_ID.pageId();

    /** */
    private static final int EMPTY_CACHE_GRP_ID = EMPTY_FULL_PAGE_ID.groupId();

    /** */
    private static final long REMOVED_PAGE_ID = 0x8000000000000000L;

    /** */
    private static final int REMOVED_CACHE_GRP_ID = 0;

    /** */
    private static final int EQUAL = 0;

    /** */
    private static final int EMPTY = 1;

    /** */
    private static final int REMOVED = -1;

    /** */
    private static final int NOT_EQUAL = 2;

    /** */
    private static final int OUTDATED = -3;

    /** Max size, in elements. */
    protected int capacity;

    /** Maximum number of steps to try before failing. */
    protected int maxSteps;

    /** Pointer to the values array. */
    protected long valPtr;

    /**
     * @return Estimated memory size required for this map to store the given number of elements.
     */
    public static long requiredMemory(long elementCnt) {
        assert LOAD_FACTOR != 0;

        return ((long)(elementCnt * LOAD_FACTOR)) * BYTES_PER_ENTRY + 8;
    }

    /**
     * @param addr Base address.
     * @param len Allocated memory length.
     * @param clear If {@code true}, then memory is considered dirty and will be cleared. Otherwise,
     *      map will assume that the given memory region is in valid state.
     */
    public FullPageIdTable(long addr, long len, boolean clear) {
        valPtr = addr;
        capacity = (int)((len - 8) / BYTES_PER_ENTRY);

        maxSteps = capacity;

        if (clear)
            clear();
    }

    /**
     * @return Current number of entries in the map.
     */
    public final int size() {
        return GridUnsafe.getInt(valPtr);
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
     * @return A value associated with the given key.
     */
    public long get(int cacheId, long pageId, int tag, long absent, long outdated) {
        assert assertKey(cacheId, pageId);

        int idx = getKey(cacheId, pageId, tag, false);

        if (idx == -1)
            return absent;

        if (idx == -2)
            return outdated;

        return valueAt(idx);
    }

    /**
     * Refresh outdated value.
     *
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @param tag Partition tag.
     * @return A value associated with the given key.
     */
    public long refresh(int cacheId, long pageId, int tag) {
        assert assertKey(cacheId, pageId);

        int idx = getKey(cacheId, pageId, tag, true);

        assert idx >= 0 : "[idx=" + idx + ", tag=" + tag + ", cacheId=" + cacheId +
            ", pageId=" + U.hexLong(pageId) + ']';

        assert tagAt(idx) < tag : "[idx=" + idx + ", tag=" + tag + ", cacheId=" + cacheId +
            ", pageId=" + U.hexLong(pageId) + ", tagAtIdx=" + tagAt(idx) + ']';

        setTagAt(idx, tag);

        return valueAt(idx);
    }

    /**
     * Associates the given key with the given value.
     *  @param cacheId Cache ID
     * @param pageId Page ID.
     * @param value Value to set.
     */
    public void put(int cacheId, long pageId, long value, int tag) {
        assert assertKey(cacheId, pageId);

        int index = putKey(cacheId, pageId, tag);

        setValueAt(index, value);
    }

    /**
     * Removes key-value association for the given key.
     *
     * @param grpId Cache group ID.
     * @param pageId Page ID.
     */
    public void remove(int grpId, long pageId, int tag) {
        assert assertKey(grpId, pageId);

        int index = removeKey(grpId, pageId, tag);

        if (index >= 0)
            setValueAt(index, 0);
    }

    /**
     * Find nearest value from specified position to the right.
     *
     * @param idx Index to start searching from.
     * @param absent Default value that will be returned if no values present.
     * @return Closest value to the index and it's partition tag or {@code absent} and -1 if no values found.
     */
    public EvictCandidate getNearestAt(final int idx, final long absent) {
        for (int i = idx; i < capacity + idx; i++) {
            final int idx2 = i >= capacity ? i - capacity : i;

            if (isValuePresentAt(idx2)) {
                long base = entryBase(idx2);

                int cacheId = GridUnsafe.getInt(base);
                int tag = GridUnsafe.getInt(base + 4);
                long pageId = GridUnsafe.getLong(base + 8);
                long val = GridUnsafe.getLong(base + 16);

                return new EvictCandidate(tag, val, new FullPageId(pageId, cacheId));
            }
        }

        return null;
    }

    /**
     * @param idx Index to clear value at.
     * @param pred Test predicate.
     * @param absent Value to return if the cell is empty.
     * @return Value at the given index.
     */
    public long clearAt(int idx, GridPredicate3<Integer, Long, Integer> pred, long absent) {
        long base = entryBase(idx);

        int grpId = GridUnsafe.getInt(base);
        int tag = GridUnsafe.getInt(base + 4);
        long pageId = GridUnsafe.getLong(base + 8);

        if ((pageId == REMOVED_PAGE_ID && grpId == REMOVED_CACHE_GRP_ID)
            || (pageId == EMPTY_PAGE_ID && grpId == EMPTY_CACHE_GRP_ID))
            return absent;

        if (pred.apply(grpId, pageId, tag)) {
            long res = valueAt(idx);

            setKeyAt(idx, REMOVED_CACHE_GRP_ID, REMOVED_PAGE_ID);
            setValueAt(idx, 0);

            return res;
        }
        else
            return absent;
    }

    /**
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @return Key index.
     */
    private int putKey(int cacheId, long pageId, int tag) {
        int step = 1;

        int index = U.safeAbs(FullPageId.hashCode(cacheId, pageId)) % capacity;

        int foundIndex = -1;
        int res;

        do {
            res = testKeyAt(index, cacheId, pageId, tag);

            if (res == OUTDATED) {
                foundIndex = index;

                break;
            }
            else if (res == EMPTY) {
                if (foundIndex == -1)
                    foundIndex = index;

                break;
            }
            else if (res == REMOVED) {
                // Must continue search to the first empty slot to ensure there are no duplicate mappings.
                if (foundIndex == -1)
                    foundIndex = index;
            }
            else if (res == EQUAL)
                return index;
            else
                assert res == NOT_EQUAL;

            index++;

            if (index >= capacity)
                index -= capacity;
        }
        while (++step <= maxSteps);

        if (foundIndex != -1) {
            setKeyAt(foundIndex, cacheId, pageId);
            setTagAt(foundIndex, tag);

            if (res != OUTDATED)
                incrementSize();

            return foundIndex;
        }

        throw new IgniteOutOfMemoryException("No room for a new key");
    }

    /**
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @return Key index.
     */
    private int getKey(int cacheId, long pageId, int tag, boolean refresh) {
        int step = 1;

        int index = U.safeAbs(FullPageId.hashCode(cacheId, pageId)) % capacity;

        do {
            long res = testKeyAt(index, cacheId, pageId, tag);

            if (res == EQUAL)
                return index;
            else if (res == EMPTY)
                return -1;
            else if (res == OUTDATED)
                return !refresh ? -2 : index;
            else
                assert res == REMOVED || res == NOT_EQUAL;

            index++;

            if (index >= capacity)
                index -= capacity;
        }
        while (++step <= maxSteps);

        return -1;
    }

    /**
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @return Key index.
     */
    private int removeKey(int cacheId, long pageId, int tag) {
        int step = 1;

        int index = U.safeAbs(FullPageId.hashCode(cacheId, pageId)) % capacity;

        int foundIndex = -1;

        do {
            long res = testKeyAt(index, cacheId, pageId, tag);

            if (res == EQUAL || res == OUTDATED) {
                foundIndex = index;

                break;
            }
            else if (res == EMPTY)
                return -1;
            else
                assert res == REMOVED || res == NOT_EQUAL;

            index++;

            if (index >= capacity)
                index -= capacity;
        }
        while (++step <= maxSteps);

        if (foundIndex != -1) {
            setKeyAt(foundIndex, REMOVED_CACHE_GRP_ID, REMOVED_PAGE_ID);

            decrementSize();
        }

        return foundIndex;
    }

    /**
     * @param index Entry index.
     * @return Key value.
     */
    private int testKeyAt(int index, int testCacheId, long testPageId, int testTag) {
        long base = entryBase(index);

        int grpId = GridUnsafe.getInt(base);
        int tag = GridUnsafe.getInt(base + 4);
        long pageId = GridUnsafe.getLong(base + 8);

        if (pageId == REMOVED_PAGE_ID && grpId == REMOVED_CACHE_GRP_ID)
            return REMOVED;
        else if (pageId == testPageId && grpId == testCacheId && tag >= testTag)
            return EQUAL;
        else if (pageId == testPageId && grpId == testCacheId && tag < testTag)
            return OUTDATED;
        else if (pageId == EMPTY_PAGE_ID && grpId == EMPTY_CACHE_GRP_ID)
            return EMPTY;
        else
            return NOT_EQUAL;
    }

    /**
     * @param idx Index to test.
     * @return {@code True} if value set for index.
     */
    private boolean isValuePresentAt(final int idx) {
        long base = entryBase(idx);

        int grpId = GridUnsafe.getInt(base);
        long pageId = GridUnsafe.getLong(base + 8);

        return !((pageId == REMOVED_PAGE_ID && grpId == REMOVED_CACHE_GRP_ID)
            || (pageId == EMPTY_PAGE_ID && grpId == EMPTY_CACHE_GRP_ID));
    }

    /**
     * @param grpId Cache group ID.
     * @param pageId Page ID.
     * @return {@code True} if checks succeeded.
     */
    private boolean assertKey(int grpId, long pageId) {
        assert grpId != EMPTY_CACHE_GRP_ID && PageIdUtils.isEffectivePageId(pageId):
            "grpId=" + grpId + ", pageId=" + U.hexLong(pageId);

        return true;
    }

    /**
     * @param index Entry index.
     * @param grpId Cache group ID to write.
     * @param pageId Page ID to write.
     */
    private void setKeyAt(int index, int grpId, long pageId) {
        long base = entryBase(index);

        GridUnsafe.putInt(base, grpId);
        GridUnsafe.putLong(base + 8, pageId);
    }

    /**
     * Gets distance from the ideal key location to the actual location if this entry is present in the table,
     * or returns negative distance that needed to be scanned to determine the absence of the mapping.
     *
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @param tag Tag.
     * @return Distance scanned if the entry is found or negative distance scanned, if entry was not found.
     */
    public int distanceFromIdeal(int cacheId, long pageId, int tag) {
        int step = 1;

        int index = U.safeAbs(FullPageId.hashCode(cacheId, pageId)) % capacity;

        int scans = 0;
        boolean found = false;

        do {
            scans++;

            long res = testKeyAt(index, cacheId, pageId, tag);

            if (res == EQUAL || res == OUTDATED) {
                found = true;

                break;
            }
            else if (res == EMPTY)
                break;
            else
                assert res == REMOVED || res == NOT_EQUAL;

            index++;

            if (index >= capacity)
                index -= capacity;
        }
        while (++step <= maxSteps);

        return found ? scans : -scans;
    }

    /**
     * Scans all the elements in this table.
     *
     * @param visitor Visitor.
     */
    public void visitAll(IgniteBiInClosure<FullPageId, Long> visitor) {
        for (int i = 0; i < capacity; i++) {
            if (isValuePresentAt(i)) {
                long base = entryBase(i);

                int cacheId = GridUnsafe.getInt(base);
                long pageId = GridUnsafe.getLong(base + 8);
                long val = GridUnsafe.getLong(base + 16);

                visitor.apply(new FullPageId(pageId, cacheId), val);
            }
        }
    }

    /**
     * @param index Entry index.
     * @return Value.
     */
    private long valueAt(int index) {
        return GridUnsafe.getLong(entryBase(index) + 16);
    }

    /**
     * @param index Entry index.
     * @param value Value.
     */
    private void setValueAt(int index, long value) {
        GridUnsafe.putLong(entryBase(index) + 16, value);
    }

    private long entryBase(int index) {
        return valPtr + 8 + (long)index * BYTES_PER_ENTRY;
    }

    /**
     * @param index Index to get tag for.
     * @return Tag at the given index.
     */
    private int tagAt(int index) {
        return GridUnsafe.getInt(entryBase(index) + 4);
    }

    /**
     * @param index Index to set tag for.
     * @param tag Tag to set at the given index.
     */
    private void setTagAt(int index, int tag) {
        GridUnsafe.putInt(entryBase(index) + 4, tag);
    }

    /**
     *
     */
    public void clear() {
        GridUnsafe.setMemory(valPtr, (long)capacity * BYTES_PER_ENTRY + 8, (byte)0);
    }

    /**
     *
     */
    private void incrementSize() {
        GridUnsafe.putInt(valPtr, GridUnsafe.getInt(valPtr) + 1);
    }

    /**
     *
     */
    private void decrementSize() {
        GridUnsafe.putInt(valPtr, GridUnsafe.getInt(valPtr) - 1);
    }
}
