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

import java.util.function.BiConsumer;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_LONG_LONG_HASH_MAP_LOAD_FACTOR;
import static org.apache.ignite.IgniteSystemProperties.getFloat;

/**
 *
 */
public class FullPageIdTable implements LoadedPagesMap {
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

    /** Tag offset from entry base. */
    private static final int TAG_OFFSET = 4;

    /** Page id offset from entry base. */
    private static final int PAGE_ID_OFFSET = 8;

    /** Value offset from entry base. */
    private static final int VALUE_OFFSET = 16;

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

    /** {@inheritDoc} */
    @Override public final int size() {
        return GridUnsafe.getInt(valPtr);
    }

    /** {@inheritDoc} */
    @Override public final int capacity() {
        return capacity;
    }

    /** {@inheritDoc} */
    @Override public long get(int grpId, long pageId, int reqVer, long absent, long outdated) {
        assert assertKey(grpId, pageId);

        int idx = getKey(grpId, pageId, reqVer, false);

        if (idx == -1)
            return absent;

        if (idx == -2)
            return outdated;

        return valueAt(idx);
    }

    /** {@inheritDoc} */
    @Override public long refresh(int grpId, long pageId, int ver) {
        assert assertKey(grpId, pageId);

        int idx = getKey(grpId, pageId, ver, true);

        if (!(idx >= 0) || !(tagAt(idx) < ver)) {
            A.ensure(idx >= 0, "[idx=" + idx + ", tag=" + ver + ", cacheId=" + grpId +
                ", pageId=" + U.hexLong(pageId) + ']');

            A.ensure(tagAt(idx) < ver, "[idx=" + idx + ", tag=" + ver + ", cacheId=" + grpId +
                ", pageId=" + U.hexLong(pageId) + ", tagAtIdx=" + tagAt(idx) + ']');
        }

        setTagAt(idx, ver);

        return valueAt(idx);
    }

    /** {@inheritDoc} */
    @Override public void put(int grpId, long pageId, long val, int ver) {
        assert assertKey(grpId, pageId);

        int idx = putKey(grpId, pageId, ver);

        setValueAt(idx, val);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(int grpId, long pageId) {
        assert assertKey(grpId, pageId);

        int idx = removeKey(grpId, pageId);

        boolean valRmv = idx >= 0;

        if (valRmv)
            setValueAt(idx, 0);

        return valRmv;
    }

    /** {@inheritDoc} */
    @Override public ReplaceCandidate getNearestAt(final int idxStart) {
        for (int i = idxStart; i < capacity + idxStart; i++) {
            final int idx2 = normalizeIndex(i);

            if (isValuePresentAt(idx2)) {
                long base = entryBase(idx2);

                int grpId = GridUnsafe.getInt(base);
                int tag = GridUnsafe.getInt(base + TAG_OFFSET);
                long pageId = GridUnsafe.getLong(base + PAGE_ID_OFFSET);
                long val = GridUnsafe.getLong(base + VALUE_OFFSET);

                return new ReplaceCandidate(tag, val, new FullPageId(pageId, grpId));
            }
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public GridLongList removeIf(int startIdxToClear, int endIdxToClear, KeyPredicate keyPred) {
        assert endIdxToClear > startIdxToClear
            : "Start and end indexes are not consistent: {" + startIdxToClear + ", " + endIdxToClear + "}";

        int sz = endIdxToClear - startIdxToClear;

        GridLongList list = new GridLongList(sz);

        for (int idx = startIdxToClear; idx < endIdxToClear; idx++) {
            long base = entryBase(idx);

            int grpId = GridUnsafe.getInt(base);
            long pageId = GridUnsafe.getLong(base + PAGE_ID_OFFSET);

            if (isRemoved(grpId, pageId) || isEmpty(grpId, pageId))
                continue;

            if (!keyPred.test(grpId, pageId))
                continue;

            long res = valueAt(idx);

            setRemoved(idx);

            list.add(res);
        }

        return list;
    }

    /**
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @return Key index.
     */
    private int putKey(int cacheId, long pageId, int tag) {
        int step = 1;

        int idx = U.safeAbs(FullPageId.hashCode(cacheId, pageId)) % capacity;

        int foundIdx = -1;
        int res;

        do {
            res = testKeyAt(idx, cacheId, pageId, tag);

            if (res == OUTDATED) {
                foundIdx = idx;

                break;
            }
            else if (res == EMPTY) {
                if (foundIdx == -1)
                    foundIdx = idx;

                break;
            }
            else if (res == REMOVED) {
                // Must continue search to the first empty slot to ensure there are no duplicate mappings.
                if (foundIdx == -1)
                    foundIdx = idx;
            }
            else if (res == EQUAL)
                return idx;
            else
                assert res == NOT_EQUAL;

            idx++;

            if (idx >= capacity)
                idx -= capacity;
        }
        while (++step <= maxSteps);

        if (foundIdx != -1) {
            setKeyAt(foundIdx, cacheId, pageId);
            setTagAt(foundIdx, tag);

            if (res != OUTDATED)
                incrementSize();

            return foundIdx;
        }

        throw new IgniteOutOfMemoryException("No room for a new key");
    }

    /**
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @param refresh Refresh.
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
     * @param grpId Cache group ID.
     * @param pageId Page ID.
     * @return {@code true} if group & page id indicates cell has state 'Removed'.
     */
    private boolean isRemoved(int grpId, long pageId) {
        return pageId == REMOVED_PAGE_ID && grpId == REMOVED_CACHE_GRP_ID;
    }

    /**
     * @param idx cell index, normalized.
     * @return {@code true} if cell with index idx has state 'Empty'.
     */
    private boolean isRemoved(int idx) {
        long base = entryBase(idx);

        int grpId = GridUnsafe.getInt(base);
        long pageId = GridUnsafe.getLong(base + PAGE_ID_OFFSET);

        return isRemoved(grpId, pageId);
    }

    /**
     * Sets cell state to 'Removed' or to 'Empty' if next cell is already 'Empty'.
     * @param idx cell index, normalized.
     */
    private void setRemoved(int idx) {
        setKeyAt(idx, REMOVED_CACHE_GRP_ID, REMOVED_PAGE_ID);

        setValueAt(idx, 0);
    }

    /**
     * @param grpId Cache group ID.
     * @param pageId Page ID.
     * @return {@code true} if group & page id indicates cell has state 'Empty'.
     */
    private boolean isEmpty(int grpId, long pageId) {
        return pageId == EMPTY_PAGE_ID && grpId == EMPTY_CACHE_GRP_ID;
    }

    /**
     * @param idx cell index, normalized.
     * @return {@code true} if cell with index idx has state 'Empty'.
     */
    private boolean isEmpty(int idx) {
        long base = entryBase(idx);

        int grpId = GridUnsafe.getInt(base);
        long pageId = GridUnsafe.getLong(base + PAGE_ID_OFFSET);

        return isEmpty(grpId, pageId);
    }

    /**
     * Sets cell state to 'Empty'.
     *
     * @param idx cell index, normalized.
     */
    private void setEmpty(int idx) {
        setKeyAt(idx, EMPTY_CACHE_GRP_ID, EMPTY_PAGE_ID);

        setValueAt(idx, 0);
    }

    /**
     * @param i index probably outsize internal array of cells.
     * @return corresponding index inside cells array.
     */
    private int normalizeIndex(int i) {
        assert i < 2 * capacity;

        return i < capacity ? i : i - capacity;
    }

    /**
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @return Key index.
     */
    private int removeKey(int cacheId, long pageId) {
        int step = 1;

        int idx = U.safeAbs(FullPageId.hashCode(cacheId, pageId)) % capacity;

        int foundIdx = -1;

        do {
            long res = testKeyAt(idx, cacheId, pageId, -1);

            if (res == EQUAL || res == OUTDATED) {
                foundIdx = idx;

                break;
            }
            else if (res == EMPTY)
                return -1;
            else
                assert res == REMOVED || res == NOT_EQUAL;

            idx++;

            if (idx >= capacity)
                idx -= capacity;
        }
        while (++step <= maxSteps);

        if (foundIdx != -1) {
            setRemoved(foundIdx);

            decrementSize();
        }

        return foundIdx;
    }

    /**
     * @param index Entry index.
     * @return Key value.
     */
    private int testKeyAt(int index, int testCacheId, long testPageId, int testTag) {
        long base = entryBase(index);

        int grpId = GridUnsafe.getInt(base);
        int tag = GridUnsafe.getInt(base + TAG_OFFSET);
        long pageId = GridUnsafe.getLong(base + PAGE_ID_OFFSET);

        if (isRemoved(grpId, pageId))
            return REMOVED;
        else if (pageId == testPageId && grpId == testCacheId && tag >= testTag)
            return EQUAL;
        else if (pageId == testPageId && grpId == testCacheId && tag < testTag)
            return OUTDATED;
        else if (isEmpty(grpId, pageId))
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
        long pageId = GridUnsafe.getLong(base + PAGE_ID_OFFSET);

        return !isRemoved(grpId, pageId) && !isEmpty(grpId, pageId);
    }

    /**
     * @param grpId Cache group ID.
     * @param pageId Page ID.
     * @return {@code True} if checks succeeded.
     */
    private boolean assertKey(int grpId, long pageId) {
        assert grpId != EMPTY_CACHE_GRP_ID && PageIdUtils.isEffectivePageId(pageId) :
            "grpId=" + grpId + ", pageId=" + U.hexLong(pageId);

        return true;
    }

    /**
     * @param idx Entry index.
     * @param grpId Cache group ID to write.
     * @param pageId Page ID to write.
     */
    private void setKeyAt(int idx, int grpId, long pageId) {
        long base = entryBase(idx);

        GridUnsafe.putInt(base, grpId);
        GridUnsafe.putLong(base + PAGE_ID_OFFSET, pageId);
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

    /** {@inheritDoc} */
    @Override public void forEach(BiConsumer<FullPageId, Long> act) {
        for (int i = 0; i < capacity; i++) {
            if (isValuePresentAt(i)) {
                long base = entryBase(i);

                int cacheId = GridUnsafe.getInt(base);
                long pageId = GridUnsafe.getLong(base + PAGE_ID_OFFSET);
                long val = GridUnsafe.getLong(base + VALUE_OFFSET);

                act.accept(new FullPageId(pageId, cacheId), val);
            }
        }
    }

    /**
     * @param index Entry index.
     * @return Value.
     */
    private long valueAt(int index) {
        return GridUnsafe.getLong(entryBase(index) + VALUE_OFFSET);
    }

    /**
     * @param index Entry index.
     * @param value Value.
     */
    private void setValueAt(int index, long value) {
        GridUnsafe.putLong(entryBase(index) + VALUE_OFFSET, value);
    }

    /**
     * @param idx Entry index.
     * @return address of entry.
     */
    private long entryBase(int idx) {
        return valPtr + 8 + (long)idx * BYTES_PER_ENTRY;
    }

    /**
     * @param idx Index to get tag for.
     * @return Tag at the given index.
     */
    private int tagAt(int idx) {
        return GridUnsafe.getInt(entryBase(idx) + TAG_OFFSET);
    }

    /**
     * @param idx Index to set tag for.
     * @param tag Tag to set at the given index.
     */
    private void setTagAt(int idx, int tag) {
        GridUnsafe.putInt(entryBase(idx) + TAG_OFFSET, tag);
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
