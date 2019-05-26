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

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTracker;

/**
 * Abstract page lock log class.
 **/
public abstract class LockLog extends PageLockTracker<PageLockLogSnapshot> {
    /** */
    protected int headIdx;

    /**
     * Constructor.
     *
     * @param name Page lock log name.
     * @param capacity Capacity.
     */
    protected LockLog(String name, int capacity) {
        super(name, capacity);
    }

    /** {@inheritDoc} */
    @Override public void onWriteLock0(int structureId, long pageId, long page, long pageAddr) {
        log(structureId, pageId, WRITE_LOCK);
    }

    /** {@inheritDoc} */
    @Override public void onWriteUnlock0(int structureId, long pageId, long page, long pageAddr) {
        log(structureId, pageId, WRITE_UNLOCK);
    }

    /** {@inheritDoc} */
    @Override public void onReadLock0(int structureId, long pageId, long page, long pageAddr) {
        log(structureId, pageId, READ_LOCK);
    }

    /** {@inheritDoc} */
    @Override public void onReadUnlock0(int structureId, long pageId, long page, long pageAddr) {
        log(structureId, pageId, READ_UNLOCK);
    }

    /**
     * Log lock operation.
     *
     * @param structureId Structure id.
     * @param pageId Page id.
     * @param op Operation.
     */
    private void log(int structureId, long pageId, int op) {
        if (!validateOperation(structureId, pageId, op))
            return;

        if ((headIdx + 2) / 2 > capacity()) {
            invalid("Log overflow, size:" + capacity() +
                ", headIdx=" + headIdx + " " + argsToString(structureId, pageId, op));

            return;
        }

        long pageId0 = getByIndex(headIdx);

        if (pageId0 != 0L && pageId0 != pageId) {
            invalid("Head should be empty, headIdx=" + headIdx + " " +
                argsToString(structureId, pageId, op));

            return;
        }

        setByIndex(headIdx, pageId);

        if (READ_LOCK == op || WRITE_LOCK == op)
            holdedLockCnt++;

        if (READ_UNLOCK == op || WRITE_UNLOCK == op)
            holdedLockCnt--;

        int curIdx = holdedLockCnt << OP_OFFSET & LOCK_IDX_MASK;

        long meta = meta(structureId, curIdx | op);

        setByIndex(headIdx + 1, meta);

        if (BEFORE_READ_LOCK == op || BEFORE_WRITE_LOCK == op)
            return;

        headIdx += 2;

        if (holdedLockCnt == 0)
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
        for (int i = 0; i < headIdx; i++)
            setByIndex(i, 0);

        headIdx = 0;
    }

    /** {@inheritDoc} */
    @Override public PageLockLogSnapshot snapshot() {
        return new PageLockLogSnapshot(
            name,
            System.currentTimeMillis(),
            headIdx / 2,
            toList(),
            nextOp,
            nextOpStructureId,
            nextOpPageId
        );
    }

    /**
     * Convert log to list {@link PageLockLogSnapshot.LogEntry}.
     *
     * @return List of {@link PageLockLogSnapshot.LogEntry}.
     */
    protected List<PageLockLogSnapshot.LogEntry> toList() {
        List<PageLockLogSnapshot.LogEntry> lockLog = new ArrayList<>(capacity);

        for (int i = 0; i < headIdx; i += 2) {
            long metaOnLock = getByIndex(i + 1);

            assert metaOnLock != 0;

            int idx = ((int)(metaOnLock >> 32) & LOCK_IDX_MASK) >> OP_OFFSET;

            assert idx >= 0;

            long pageId = getByIndex(i);

            int op = (int)((metaOnLock >> 32) & LOCK_OP_MASK);
            int structureId = (int)(metaOnLock);

            lockLog.add(new PageLockLogSnapshot.LogEntry(pageId, structureId, op, idx));
        }

        return lockLog;
    }
}
