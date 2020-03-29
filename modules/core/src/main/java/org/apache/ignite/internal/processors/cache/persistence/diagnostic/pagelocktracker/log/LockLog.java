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

package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.log;

import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTracker;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTrackerManager.MemoryCalculator;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageMetaInfoStore;

/**
 * Abstract page lock log class.
 **/
public class LockLog extends PageLockTracker<PageLockLogSnapshot> {
    /** */
    protected int headIdx;

    /**
     * Constructor.
     *
     * @param name Page lock log name.
     * @param pageMetaInfoStore Object storing page meta info.
     */
    public LockLog(String name, PageMetaInfoStore pageMetaInfoStore, MemoryCalculator memCalc) {
        super(name, pageMetaInfoStore, memCalc);
    }

    /** {@inheritDoc} */
    @Override public void onWriteLock0(int structureId, long pageId, long page, long pageAddr) {
        log(WRITE_LOCK, structureId, pageId, page, pageAddr);
    }

    /** {@inheritDoc} */
    @Override public void onWriteUnlock0(int structureId, long pageId, long page, long pageAddr) {
        log(WRITE_UNLOCK, structureId, pageId, page, pageAddr);
    }

    /** {@inheritDoc} */
    @Override public void onReadLock0(int structureId, long pageId, long page, long pageAddr) {
        log(READ_LOCK, structureId, pageId, page, pageAddr);
    }

    /** {@inheritDoc} */
    @Override public void onReadUnlock0(int structureId, long pageId, long page, long pageAddr) {
        log(READ_UNLOCK, structureId, pageId, page, pageAddr);
    }

    /**
     * Log lock operation.
     *
     * @param structureId Structure id.
     * @param pageId Page id.
     * @param op Operation.
     */
    private void log(int op, int structureId, long pageId, long pageAddrHeader, long pageAddr) {
        if (!validateOperation(structureId, pageId, op))
            return;

        if (headIdx + 1 > pages.capacity()) {
            invalid("Log overflow, size:" + pages.capacity() +
                ", headIdx=" + headIdx + " " + argsToString(structureId, pageId, op));

            return;
        }

        long pageId0 = pages.getPageId(headIdx);

        if (pageId0 != 0L && pageId0 != pageId) {
            invalid("Head should be empty, headIdx=" + headIdx + " " +
                argsToString(structureId, pageId, op));

            return;
        }

        if (READ_LOCK == op || WRITE_LOCK == op)
            heldLockCnt++;

        if (READ_UNLOCK == op || WRITE_UNLOCK == op)
            heldLockCnt--;

        int curIdx = heldLockCnt << OP_OFFSET & LOCK_IDX_MASK;

        pages.add(headIdx, curIdx | op, structureId, pageId, pageAddrHeader, pageAddr);

        if (BEFORE_READ_LOCK == op || BEFORE_WRITE_LOCK == op)
            return;

        headIdx++;

        if (heldLockCnt == 0)
            reset();

        if (op != BEFORE_READ_LOCK && op != BEFORE_WRITE_LOCK &&
            nextOpPageId == pageId && nextOpStructureId == structureId) {
            nextOpStructureId = 0;
            nextOpPageId = 0;
            nextOp = 0;
        }
    }

    /**
     * Reset log state.
     */
    private void reset() {
        for (int itemIdx = 0; itemIdx < headIdx; itemIdx++)
            pages.remove(itemIdx);

        headIdx = 0;
    }

    /** {@inheritDoc} */
    @Override public PageLockLogSnapshot snapshot() {
        PageMetaInfoStore log = pages.copy();

        return new PageLockLogSnapshot(
            name,
            System.currentTimeMillis(),
            headIdx,
            log,
            nextOp,
            nextOpStructureId,
            nextOpPageId
        );
    }
}
