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

package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.stack;

import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTracker;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTrackerManager.MemoryCalculator;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageMetaInfoStore;

/**
 * Abstract page lock stack.
 */
public class LockStack extends PageLockTracker<PageLockStackSnapshot> {
    /**
     *
     */
    protected int headIdx;

    /**
     * @param name Page lock stack name.
     * @param pageMetaInfoStore Capacity.
     */
    public LockStack(String name, PageMetaInfoStore pageMetaInfoStore, MemoryCalculator memCalc) {
        super(name, pageMetaInfoStore, memCalc);
    }

    /** {@inheritDoc} */
    @Override public void onWriteLock0(int structureId, long pageId, long page, long pageAddr) {
        push(WRITE_LOCK, structureId, pageId, page, pageAddr);
    }

    /** {@inheritDoc} */
    @Override public void onWriteUnlock0(int structureId, long pageId, long page, long pageAddr) {
        pop(WRITE_UNLOCK, structureId, pageId, page, pageAddr);
    }

    /** {@inheritDoc} */
    @Override public void onReadLock0(int structureId, long pageId, long page, long pageAddr) {
        push(READ_LOCK, structureId, pageId, page, pageAddr);
    }

    /** {@inheritDoc} */
    @Override public void onReadUnlock0(int structureId, long pageId, long page, long pageAddr) {
        pop(READ_UNLOCK, structureId, pageId, page, pageAddr);
    }

    /**
     * Push operation on top of stack.
     *
     * @param structureId Strcuture id.
     * @param pageId Page id.
     * @param op Operation type.
     */
    private void push(int op, int structureId, long pageId, long pageAddrHeader, long pageAddr) {
        if (!validateOperation(structureId, pageId, op))
            return;

        reset();

        if (headIdx + 1 > pages.capacity()) {
            invalid("Stack overflow, size=" + pages.capacity() +
                ", headIdx=" + headIdx + " " + argsToString(structureId, pageId, op));

            return;
        }

        long pageId0 = pages.getPageId(headIdx);

        if (pageId0 != 0L) {
            invalid("Head element should be empty, headIdx=" + headIdx +
                ", pageIdOnHead=" + pageId0 + " " + argsToString(structureId, pageId, op));

            return;
        }

        int curIdx = heldLockCnt << OP_OFFSET & LOCK_IDX_MASK;

        pages.add(headIdx, curIdx | op, structureId, pageId, pageAddrHeader, pageAddr);

        headIdx++;
        heldLockCnt++;
    }

    /**
     * Pop operation from top of stack.
     *
     * @param structureId Structure id.
     * @param pageId Page id.
     * @param op Operation type.
     */
    private void pop(int op, int structureId, long pageId, long pageAddrHeader, long pageAddr) {
        if (!validateOperation(structureId, pageId, op))
            return;

        reset();

        if (headIdx > 1) {
            int lastItemIdx = headIdx - 1;

            long lastPageId = pages.getPageId(lastItemIdx);

            if (lastPageId == pageId) {
                pages.remove(lastItemIdx);

                //Reset head to the first not empty element.
                do {
                    headIdx--;
                    heldLockCnt--;
                }
                while (headIdx > 0 && pages.getPageId(headIdx - 1) == 0);
            }
            else {
                for (int itemIdx = lastItemIdx - 1; itemIdx >= 0; itemIdx--) {
                    if (pages.getPageId(itemIdx) == pageId) {
                        pages.remove(itemIdx);
                        return;
                    }
                }

                invalid("Can not find pageId in stack, headIdx=" + headIdx + " "
                    + argsToString(structureId, pageId, op));
            }
        }
        else {
            if (headIdx < 0) {
                invalid("HeadIdx can not be less, headIdx="
                    + headIdx + ", " + argsToString(structureId, pageId, op));

                return;
            }

            long pageId0 = pages.getPageId(0);

            if (pageId0 == 0) {
                invalid("Stack is empty, can not pop elemnt" + argsToString(structureId, pageId, op));

                return;
            }

            if (pageId0 == pageId) {
                pages.remove(0);

                headIdx = 0;
                heldLockCnt = 0;
            }
            else
                invalid("Can not find pageId in stack, headIdx=" + headIdx + " "
                    + argsToString(structureId, pageId, op));
        }
    }

    /** {@inheritDoc} */
    @Override protected PageLockStackSnapshot snapshot() {
        PageMetaInfoStore stack = pages.copy();

        return new PageLockStackSnapshot(
            name,
            System.currentTimeMillis(),
            headIdx,
            stack,
            nextOp,
            nextOpStructureId,
            nextOpPageId
        );
    }

    /**
     * Reset next opeation info.
     */
    private void reset() {
        nextOpPageId = 0;
        nextOp = 0;
        nextOpStructureId = 0;
    }
}
