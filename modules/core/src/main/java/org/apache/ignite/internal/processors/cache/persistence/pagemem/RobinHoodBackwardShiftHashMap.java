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
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.lang.GridPredicate3;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_LONG_LONG_HASH_MAP_LOAD_FACTOR;
import static org.apache.ignite.IgniteSystemProperties.getFloat;
import static org.apache.ignite.internal.util.GridUnsafe.getInt;
import static org.apache.ignite.internal.util.GridUnsafe.getLong;

/**
 *
 */
public class RobinHoodBackwardShiftHashMap implements LoadedPagesMap {
    /** Load factor. */
    private static final float LOAD_FACTOR = getFloat(IGNITE_LONG_LONG_HASH_MAP_LOAD_FACTOR, 2.5f);

    /** Size of count of entries (value returned by size() method). */
    private static final int MAPSIZE_SIZE = 4;

    /** Count of entries offset starting from base address.  */
    private static final int MAPSIZE_OFFSET = 0;

    /** Size of ideal bucket (cell to store value to avoid probing other cells followed). */
    private static final int IDEAL_BUCKET_SIZE = 4;

    /** Offset of ideal bucket starting from entry base.  */
    private static final int IDEAL_BUCKET_OFFSET = 0;

    /** Group ID size. */
    private static final int GRP_ID_SIZE = 4;

    /** Group ID offset from entry base. */
    private static final int GRP_ID_OFFSET = IDEAL_BUCKET_OFFSET + IDEAL_BUCKET_SIZE;

    /** Page ID size. */
    private static final int PAGE_ID_SIZE = 8;

    /** Page id offset from entry base. */
    private static final int PAGE_ID_OFFSET = GRP_ID_OFFSET + GRP_ID_SIZE;

    /** Value size. */
    private static final int VALUE_SIZE = 8;

    /** Value offset from entry base. */
    private static final int VALUE_OFFSET = PAGE_ID_OFFSET + PAGE_ID_SIZE;

    /** Tag offset from entry base. */
    private static final int TAG_OFFSET = 4;

    /** */
    private static final long EMPTY_PAGE_ID = 0;

    /** */
    private static final int EMPTY_CACHE_GRP_ID = 0;

    /** Bytes required for storing one entry (cell). */
    private static final int BYTES_PER_CELL = IDEAL_BUCKET_SIZE + GRP_ID_SIZE + PAGE_ID_SIZE + VALUE_SIZE;

    /** Number of buckets, indicates range of scan memory, max probe count and maximum map size. */
    private final int numBuckets;
    /** Base address of map content. */
    private long baseAddr;

    //todo remove {{
    /** Avg get steps. */
    IntervalBasedMeasurement avgGetSteps = new IntervalBasedMeasurement(250, 4);

    /** Avg put steps. */
    IntervalBasedMeasurement avgPutSteps = new IntervalBasedMeasurement(250, 4);

    /**
     * @param elementsCnt Maximum elements can be stored in map, its maximum size.
     * @return Estimated memory size required for this map to store the given number of elements.
     */
    public static long requiredMemory(long elementsCnt) {
        float loadFactor = LOAD_FACTOR;
        assert loadFactor != 0;

        return requiredMemoryByBuckets((long)(elementsCnt * loadFactor));
    }

    /**
     * @return required size to allocate, based on number of buckets (cells) to store in map, its capacity.
     * @param numBuckets Number of buckets (cells) to store, capacity.
     */
    static long requiredMemoryByBuckets(long numBuckets) {
        return numBuckets * BYTES_PER_CELL + MAPSIZE_SIZE;
    }

    /**
     * Creates map in preallocated unsafe memory segment.
     *
     * @param baseAddr Base buffer address.
     * @param size Size available for map, number of buckets (cells) to store will be determined accordingly.
     */
    public RobinHoodBackwardShiftHashMap(long baseAddr, long size) {
        this.numBuckets = (int)((size - MAPSIZE_SIZE) / BYTES_PER_CELL);
        this.baseAddr = baseAddr;

        GridUnsafe.setMemory(baseAddr, size, (byte)0);
    }

    /**
     * @param idx cell index.
     * @return base cell (bucket) address in buffer.
     */
    private long entryBase(int idx) {
        return baseAddr + MAPSIZE_SIZE + (long)idx * BYTES_PER_CELL;
    }

    /** {@inheritDoc} */
    @Override public long get(int grpId, long pageId, int ver, long absent, long outdated) {
        assert grpId != 0;
        int idxInit = U.safeAbs(FullPageId.hashCode(grpId, pageId)) % numBuckets;

        for (int i = 0; i < numBuckets; i++) {
            int idxCurr = (idxInit + i) % numBuckets;

            final long base = entryBase(idxCurr);
            final int dibEntryToInsert = distance(idxCurr, idxInit);

            final int curGrpId = getGrpId(base);
            final long curPageId = getPageId(base);
            final int curIdealBucket = getIdealBucket(base);
            final long curVal = getValue(base);
            final int dibCurEntry = distance(idxCurr, curIdealBucket);

            if (isEmpty(curGrpId, curPageId))
                return absent;
            else if (curGrpId == grpId && curPageId == pageId) {
                //equal value found
                return curVal;
            }
            else if (dibCurEntry < dibEntryToInsert)
                return absent;
        }

        return absent;
    }

    /** {@inheritDoc} */
    @Override public void put(int grpId, long pageId, long val, int ver) {
        assert grpId != 0;

        int idxInit = U.safeAbs(FullPageId.hashCode(grpId, pageId)) % numBuckets;

        int grpIdToInsert = grpId;
        long pageIdToInsert = pageId;
        int tagToInsert = ver;
        long valToInsert = val;
        long idxIdealToInsert = idxInit;
        int swapCount = 0;

        int i = 0;
        try {
            for (i = 0; i < numBuckets; i++) {
                int idxCurr = (idxInit + i) % numBuckets;

                final long base = entryBase(idxCurr);
                final int dibEntryToInsert = distance(idxCurr, idxInit);

                final int curGrpId = getGrpId(base);
                final long curPageId = getPageId(base);
                final int curIdealBucket = getIdealBucket(base);
                final long curVal = getValue(base);
                final int dibCurEntry = distance(idxCurr, curIdealBucket);

                if (isEmpty(curGrpId, curPageId)) {
                    setCellValue(base, idxIdealToInsert, grpIdToInsert, pageIdToInsert, valToInsert);

                    setSize(size() + 1);
                    return;
                }
                else if (curGrpId == grpIdToInsert && curPageId == pageIdToInsert) {

                    if (swapCount != 0) {
                        StringBuilder sb = new StringBuilder();

                        dumpEntry(sb, idxCurr);

                        assert swapCount == 0 : "Swapped " + swapCount + " times:" + sb;
                    }

                    setValue(base, valToInsert);

                    return; //equal value found
                }
                else if (dibCurEntry < dibEntryToInsert) {
                    //swapping *toInsert and state in bucket: save cur state to bucket
                    setCellValue(base, idxIdealToInsert, grpIdToInsert, pageIdToInsert, valToInsert);

                    idxIdealToInsert = curIdealBucket;
                    pageIdToInsert = curPageId;
                    grpIdToInsert = curGrpId;
                    valToInsert = curVal;

                    swapCount++;
                }
            }
        }
        finally {
            avgPutSteps.addMeasurementForAverageCalculation(i);
        }
        // no free space left

        throw new IgniteOutOfMemoryException("No room for a new key");
    }


    /** {@inheritDoc} */
    @Override public boolean remove(int grpId, long pageId) {
        assert grpId != 0;

        int idxInit = U.safeAbs(FullPageId.hashCode(grpId, pageId)) % numBuckets;

        int idxEqualValFound   = -1;
        for (int i = 0; i < numBuckets; i++) {
            int idxCurr = (idxInit + i) % numBuckets;

            final long base = entryBase(idxCurr);
            final int dibEntryToInsert = distance(idxCurr, idxInit);

            final int curGrpId = getGrpId(base);
            final long curPageId = getPageId(base);
            final int curIdealBucket = getIdealBucket(base);
            final int dibCurEntry = distance(idxCurr, curIdealBucket);

            if (isEmpty(curGrpId, curPageId))
                return false;
            else if (curGrpId == grpId && curPageId == pageId) {
                idxEqualValFound = idxCurr;
                break; //equal value found
            }
            else if (dibCurEntry < dibEntryToInsert) {
                //If our value was present in map we had already found it.
                return false;
            }
        }
        assert idxEqualValFound >= 0;

        setSize(size() - 1);

        //scanning rest of map
        for (int i = 0; i < numBuckets - 1; i++) {
            int idxCurr = (idxEqualValFound + i) % numBuckets;
            int idxNext = (idxEqualValFound + i + 1) % numBuckets;

            long baseCurr = entryBase(idxCurr);

            long baseNext = entryBase(idxNext);
            final int nextGrpId = getGrpId(baseNext);
            final long nextPageId = getPageId(baseNext);
            final int nextIdealBucket = getIdealBucket(baseNext);

            if (isEmpty(nextGrpId, nextPageId)
                || distance(idxNext, nextIdealBucket) == 0) {
                setEmpty(baseCurr);

                return true;
            }
            else
                setCellValue(baseCurr, nextIdealBucket, nextGrpId, nextPageId, getValue(baseNext));
        }

        int lastShiftedIdx = (idxEqualValFound -1) % numBuckets;

        setEmpty(entryBase(lastShiftedIdx));

        return true;
    }


    /** {@inheritDoc} */
    @Override public ReplaceCandidate getNearestAt(final int idxStart) {
        for (int i = 0; i < numBuckets; i++) {
            int idxCurr = (idxStart + i) % numBuckets;

            if (isEmptyAt(idxCurr))
                continue;

            long base = entryBase(idxCurr);

            //todo provide tag
            int tag = 1; //GridUnsafe.getInt(base + TAG_OFFSET);

            return new ReplaceCandidate(tag, getValue(base),
                new FullPageId(getPageId(base), getGrpId(base)));
        }

        return null;
    }

    /**
     * @param idx Index to test.
     * @return {@code True} if value is not provided in cell having index.
     */
    private boolean isEmptyAt(int idx) {
        long base = entryBase(idx);

        return isEmpty(getGrpId(base), getPageId(base));
    }

    //todo implement
    @Override public long refresh(int cacheId, long pageId, int tag) {
        throw new UnsupportedOperationException();
    }

    //todo implement
    @Override public long clearAt(int idx, GridPredicate3<Integer, Long, Integer> pred, long absent) {
        throw new UnsupportedOperationException();
    }


    /** {@inheritDoc} */
    @Override public int capacity() {
        return numBuckets;
    }

    /**
     * @param i index probably outsize internal array of cells.
     * @return corresponding index inside cells array.
     */
    private int normalizeIndex(int i) {
        assert i < 2 * numBuckets;

        return i < numBuckets ? i : i - numBuckets;
    }

    /**
     * @param curr current selected index to store value.
     * @param baseIdx base or ideal bucket to store entry value to avoid probing.
     * @return distance between cells, or 0 if cell is ideal.
     */
    private int distance(int curr, int baseIdx) {
        int diff = curr - baseIdx;

        if (diff < 0)
            return diff + numBuckets;

        return diff;
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
     * Sets cell value to be empty.
     * @param addr entry base address.
     */
    private void setEmpty(long addr) {
        setPageId(addr, EMPTY_PAGE_ID);
        setGrpId(addr, EMPTY_CACHE_GRP_ID);
        setValue(addr, 0);
        setIdealBucket(addr, 0);
    }

    /**
     * @param base Entry base, address in buffer of the entry start.
     * @param idxIdeal number of ideal bucket (cell) to insert this value.
     */
    private void setIdealBucket(long base, long idxIdeal) {
        assert idxIdeal>=0 && idxIdeal < numBuckets;

        GridUnsafe.putLong(base + IDEAL_BUCKET_OFFSET, idxIdeal);
    }

    /**
     * @return printable dump with all buckets state.
     */
    public String dump() {
        StringBuilder sb = new StringBuilder();

        for (int idx = 0; idx < numBuckets; idx++)
            dumpEntry(sb, idx);

        return sb.toString();
    }

    /**
     * @param sb destination string builder to dump entry to.
     * @param idx bucket index.
     */
    private void dumpEntry(StringBuilder sb, int idx) {
        long base = entryBase(idx);
        int curGrpId = getGrpId(base);
        long curPageId = getPageId(base);
        long curVal = getValue(base);

        sb.append("slot [").append(idx).append("]:");

        if(isEmpty(curGrpId, curPageId))
            sb.append("Empty: ");

        sb.append("i.buc=").append(getIdealBucket(base)).append(",");
        sb.append("(grp=").append(curGrpId).append(",");
        sb.append("page=").append(curPageId).append(")");
        sb.append("->");
        sb.append("val=").append(curVal).append(",");
        sb.append("\n");
    }


    /**
     * @param base Entry base, address in buffer of the entry start.
     * @param idealBucket number of ideal bucket (cell) to insert this value.
     * @param grpId Group ID to be stored in entry.
     * @param pageId
     * @param val
     */
    private void setCellValue(long base, long idealBucket, int grpId, long pageId, long val) {
        setIdealBucket(base, idealBucket);
        setGrpId(base, grpId);
        setPageId(base, pageId);
        setValue(base, val);
    }

    /**
     * @param base address of current cell.
     * @return number of ideal bucket (cell) to store this value.
     */
    private int getIdealBucket(long base) {
        return getInt(base + IDEAL_BUCKET_OFFSET);
    }

    private long getPageId(long base) {
        return getLong(base + PAGE_ID_OFFSET);
    }

    private void setPageId(long base, long pageIdToInsert) {
        GridUnsafe.putLong(base + PAGE_ID_OFFSET, pageIdToInsert);
    }

    /**
     * @param base Entry base address.
     * @return Group ID stored in entry.
     */
    private int getGrpId(long base) {
        return getInt(base + GRP_ID_OFFSET);
    }

    /**
     * @param base Entry base address.
     * @param grpId Group ID to be stored in entry.
     */
    private void setGrpId(long base, int grpId) {
        GridUnsafe.putInt(base + GRP_ID_OFFSET, grpId);
    }

    private long getValue(long base) {
        return getLong(base + VALUE_OFFSET);
    }

    private void setValue(long base, long pageId) {
        GridUnsafe.putLong(base + VALUE_OFFSET, pageId);
    }

    /** {@inheritDoc} */
    @Override public final int size() {
        return GridUnsafe.getInt(baseAddr + MAPSIZE_OFFSET);
    }

    /**
     * Changes collection size.
     * @param sz new size to set.
     */
    private void setSize(int sz) {
        GridUnsafe.putInt(baseAddr + MAPSIZE_OFFSET, sz);
    }

    /**
     * Scans all the elements in this table.
     *
     * @param visitor Visitor.
     */
    public void forEach(BiConsumer<FullPageId, Long> visitor) {
        for (int i = 0; i < numBuckets; i++) {
            if (!isEmptyAt(i)) {
                long base = entryBase(i);

                int grpId = GridUnsafe.getInt(base + GRP_ID_OFFSET);
                long pageId = GridUnsafe.getLong(base + PAGE_ID_OFFSET);
                long val = GridUnsafe.getLong(base + VALUE_OFFSET);

                visitor.accept(new FullPageId(pageId, grpId), val);
            }
        }
    }
}
