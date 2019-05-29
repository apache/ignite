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

import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.LongStore;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTracker;

/**
 * Abstract page lock stack.
 */
public class LockStack extends PageLockTracker<PageLockStackSnapshot> {
    /** */
    protected int headIdx;

    /**
     * @param name Page lock stack name.
     * @param longStore Capacity.
     */
    public LockStack(String name, LongStore longStore) {
        super(name, longStore);
    }

    /** {@inheritDoc} */
    @Override public void onWriteLock0(int structureId, long pageId, long page, long pageAddr) {
        push(structureId, pageId, WRITE_LOCK);
    }

    /** {@inheritDoc} */
    @Override public void onWriteUnlock0(int structureId, long pageId, long page, long pageAddr) {
        pop(structureId, pageId, WRITE_UNLOCK);
    }

    /** {@inheritDoc} */
    @Override public void onReadLock0(int structureId, long pageId, long page, long pageAddr) {
        push(structureId, pageId, READ_LOCK);
    }

    /** {@inheritDoc} */
    @Override public void onReadUnlock0(int structureId, long pageId, long page, long pageAddr) {
        pop(structureId, pageId, READ_UNLOCK);
    }

    /**
     * Push operation on top of stack.
     *
     * @param structureId Strcuture id.
     * @param pageId Page id.
     * @param op Operation type.
     */
    private void push(int structureId, long pageId, int op) {
        if (!validateOperation(structureId, pageId, op))
            return;

        reset();

        if (headIdx / 2 + 1 > longStore.capacity()) {
            invalid("Stack overflow, size=" + longStore.capacity() +
                ", headIdx=" + headIdx + " " + argsToString(structureId, pageId, op));

            return;
        }

        long pageId0 = longStore.getByIndex(headIdx);

        if (pageId0 != 0L) {
            invalid("Head element should be empty, headIdx=" + headIdx +
                ", pageIdOnHead=" + pageId0 + " " + argsToString(structureId, pageId, op));

            return;
        }

        int curIdx = holdedLockCnt << OP_OFFSET & LOCK_IDX_MASK;

        long meta = meta(structureId, curIdx | op);

        longStore.setByIndex(headIdx, pageId);
        longStore.setByIndex(headIdx + 1, meta);

        headIdx += 2;
        holdedLockCnt++;
    }

    /**
     * Pop operation from top of stack.
     *
     * @param structureId Structure id.
     * @param pageId Page id.
     * @param op Operation type.
     */
    private void pop(int structureId, long pageId, int op) {
        if (!validateOperation(structureId, pageId, op))
            return;

        reset();

        if (headIdx > 2) {
            int last = headIdx - 2;

            long val = longStore.getByIndex(last);

            if (val == pageId) {
                longStore.setByIndex(last, 0);
                longStore.setByIndex(last + 1, 0);

                //Reset head to the first not empty element.
                do {
                    headIdx -= 2;
                    holdedLockCnt--;
                }
                while (headIdx > 0 && longStore.getByIndex(headIdx - 2) == 0);
            }
            else {
                for (int idx = last - 2; idx >= 0; idx -= 2) {
                    if (longStore.getByIndex(idx) == pageId) {
                        longStore.setByIndex(idx, 0);
                        longStore.setByIndex(idx + 1, 0);

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

            long val = longStore.getByIndex(0);

            if (val == 0) {
                invalid("Stack is empty, can not pop elemnt" + argsToString(structureId, pageId, op));

                return;
            }

            if (val == pageId) {
                longStore.setByIndex(0, 0);
                longStore.setByIndex(1, 0);

                headIdx = 0;
                holdedLockCnt = 0;
            }
            else
                invalid("Can not find pageId in stack, headIdx=" + headIdx + " "
                    + argsToString(structureId, pageId, op));
        }
    }

    /** {@inheritDoc} */
    @Override protected PageLockStackSnapshot snapshot() {
        long[] stack = longStore.copy();

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
