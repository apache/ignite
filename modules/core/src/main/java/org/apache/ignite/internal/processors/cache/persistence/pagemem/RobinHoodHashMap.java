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

import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.util.GridUnsafe.getInt;
import static org.apache.ignite.internal.util.GridUnsafe.getLong;

/**
 *
 */
public class RobinHoodHashMap {
    public static final int SIZE_OF_COUNT = 8;


    /** Size of ideal bucket. */
    private static final int IDEAL_BUCKET_SIZE = 4;

    /** Offset of ideal bucket. */
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

    /** */
    private static final long REMOVED_PAGE_ID = 0x8000000000000000L;

    /** */
    private static final int REMOVED_CACHE_GRP_ID = 0;

    public static final int BYTES_PER_ENTRY = IDEAL_BUCKET_SIZE + GRP_ID_SIZE + PAGE_ID_SIZE + VALUE_SIZE;

    private final int numBuckets;
    private long baseAddr;

    public RobinHoodHashMap(long baseAddr, int numBuckets) {
        this.baseAddr = baseAddr;
        this.numBuckets = numBuckets;

        GridUnsafe.setMemory(baseAddr, (long)numBuckets * BYTES_PER_ENTRY + SIZE_OF_COUNT, (byte)0);
    }
    private long entryBase(int index) {
        return baseAddr + SIZE_OF_COUNT + (long)index * BYTES_PER_ENTRY;
    }

    public long get(int grpId, long pageId, int tag, long absent, long outdated) {
        assert grpId != 0;
        int idxInit = U.safeAbs(FullPageId.hashCode(grpId, pageId)) % numBuckets;


        int probingMax = numBuckets;

        for (int i = 0; i < probingMax; i++) {
            int indexCurrent = (idxInit + i) % numBuckets;

            final long base = entryBase(indexCurrent);
            final int dibEntryToInsert = distance(indexCurrent, idxInit);

            final int curGrpId = getGrpId(base);
            final long curPageId = getPageId(base);
            final int curIdealBucket = getIdealBucket(base);
            final long curVal = getValue(base);
            final int dibCurEntry = distance(indexCurrent, curIdealBucket);

            if (isEmpty(curGrpId, curPageId))
                return absent;
            else if (isRemoved(curGrpId, curPageId)) {
                //don't need to scan anymore
                if (dibCurEntry < dibEntryToInsert)
                    return absent;

                //otherwise continue to scan all chain
            }
            else if (curGrpId == grpId && curPageId == pageId) {
                //equal value found
                return curVal;
            }
            else if (dibCurEntry < dibEntryToInsert)
                return absent;
            else {
                //need to switch to next idx;
            }
        }

        return absent;
    }

    public boolean put(int grpId, long pageId, long val, int tag) {
        assert grpId != 0;

        int probingMax = numBuckets;
        int idxInit = U.safeAbs(FullPageId.hashCode(grpId, pageId)) % numBuckets;

        int grpIdToInsert = grpId;
        long pageIdToInsert = pageId;
        int tagToInsert = tag;
        long valToInsert = val;
        long idxIdealToInsert = idxInit;
        int swapCount = 0;


        for (int i = 0; i < probingMax; i++) {
            int idxCurr = (idxInit + i) % numBuckets;

            final long base = entryBase(idxCurr);
            final int dibEntryToInsert = distance(idxCurr, idxInit);

            final int curGrpId = getGrpId(base);
            final long curPageId = getPageId(base);
            final int curIdealBucket = getIdealBucket(base);
            final long curVal = getValue(base);
            final int dibCurEntry = distance(idxCurr, curIdealBucket);

            if (isEmpty(curGrpId, curPageId)) {
                setIdealBucket(base, idxIdealToInsert);
                setGrpId(base, grpIdToInsert);
                setPageId(base, pageIdToInsert);
                setValue(base, valToInsert);
                return true;
            } else if (isRemoved(curGrpId, curPageId)) {
                if (dibCurEntry < dibEntryToInsert) {
                    setIdealBucket(base, idxIdealToInsert);
                    setGrpId(base, grpIdToInsert);
                    setPageId(base, pageIdToInsert);
                    setValue(base, valToInsert);
                    //If a deleted entry is moved during an insertion,
                    // and becomes the entry to insert, it is simply discarded, and the insertion finishes.
                    return true;
                } else {
                    //todo
                }
            } else if (curGrpId == grpIdToInsert && curPageId == pageIdToInsert) {

                if (swapCount != 0) {
                    StringBuilder sb = new StringBuilder();
                    dumpEntry(sb, idxCurr);

                    assert swapCount == 0 : "Swaped " + swapCount + " times:" +  sb.toString();
                }
                //equal value found
                return false;
            }
            else if (dibCurEntry < dibEntryToInsert) {
                //swapping *toInsert and state in bucket: save cur state to bucket
                setIdealBucket(base, idxIdealToInsert);
                setGrpId(base, grpIdToInsert);
                setPageId(base, pageIdToInsert);
                setValue(base, valToInsert);

                idxIdealToInsert = curIdealBucket;
                pageIdToInsert = curPageId;
                grpIdToInsert = curGrpId;
                valToInsert = curVal;

                swapCount++;
            } else {
                //need to switch to next idx;
            }
        }
        // no free space left
        return false;
    }


    public boolean remove(int grpId, long pageId, int tag) {
        assert grpId != 0;

        int idxInit = U.safeAbs(FullPageId.hashCode(grpId, pageId)) % numBuckets;

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
            else if (isRemoved(curGrpId, curPageId)) {
                if (dibCurEntry < dibEntryToInsert)
                    return false;
            }
            else if (curGrpId == grpId && curPageId == pageId) {

                //equal value found
                setRemoved(base);
                return true;
            }
            else if (dibCurEntry < dibEntryToInsert) {
                //swapping *toInsert and state in bucket: save cur state to bucket
                return false;
            } else {
                //need to switch to next idx;
            }
        }
        return false;
    }


    /**
     * @param i index probably outsize internal array of cells.
     * @return corresponding index inside cells array.
     */
    private int normalizeIndex(int i) {
        assert i < 2 * numBuckets;

        return i < numBuckets ? i : i - numBuckets;
    }


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

    private void setEmpty(long addr) {
        setPageId(addr, EMPTY_PAGE_ID);
        setGrpId(addr, EMPTY_CACHE_GRP_ID);
        setValue(addr, 0);
        setIdealBucket(addr, 0);
    }

    private boolean isRemoved(int grpId, long pageId) {
        return pageId == REMOVED_PAGE_ID && grpId == REMOVED_CACHE_GRP_ID;
    }
    private void setRemoved(long addr) {
        setPageId(addr, REMOVED_PAGE_ID);
        setGrpId(addr, REMOVED_CACHE_GRP_ID);
        setValue(addr, 0);
        // don't reset ideal bucket, setIdealBucket(addr, 0);
    }

    private void setIdealBucket(long base, long idxIdealToInsert) {
        assert idxIdealToInsert < numBuckets;

        GridUnsafe.putLong(base + IDEAL_BUCKET_OFFSET, idxIdealToInsert);
    }

    public String dump() {
        StringBuilder sb = new StringBuilder();

        for (int idx = 0; idx < numBuckets; idx++)
            dumpEntry(sb, idx);

        return sb.toString();
    }

    private void dumpEntry(StringBuilder sb, int i) {
        long base = entryBase(i);
        int curGrpId = getGrpId(base);
        long curPageId = getPageId(base);
        long curVal = getValue(base);
        sb.append("slot [").append(i).append("]:");

        if(isEmpty(curGrpId, curPageId))
            sb.append("Empty: ");
        else if(isRemoved(curGrpId, curPageId))
            sb.append("Removed: ");

        sb.append("i.buc=" ).append(getIdealBucket(base)).append(",");
        sb.append("grp=" ).append(curGrpId).append(",");
        sb.append("page=" ).append(curPageId).append("->");
        sb.append("val=" ).append(curVal).append(",");

        sb.append("\n");
    }

    private int getIdealBucket(long base) {
        return getInt(base + IDEAL_BUCKET_OFFSET);
    }


    private long getPageId(long base) {
        return getLong(base + PAGE_ID_OFFSET);
    }

    private void setPageId(long base, long pageIdToInsert) {
        GridUnsafe.putLong(base + PAGE_ID_OFFSET, pageIdToInsert);
    }

    private int getGrpId(long base) {
        return getInt(base + GRP_ID_OFFSET);
    }


    private void setGrpId(long base, int grpIdToInsert) {
        GridUnsafe.putInt(base + GRP_ID_OFFSET, grpIdToInsert);
    }

    private long getValue(long base) {
        return getLong(base + VALUE_OFFSET);
    }

    private void setValue(long base, long pageIdToInsert) {
        GridUnsafe.putLong(base + VALUE_OFFSET, pageIdToInsert);
    }

    public void refresh(int id, int id1, int tag) {
        throw new UnsupportedOperationException();
    }
}
