package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.log;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTracker;

public abstract class LockLog extends PageLockTracker<LockLogSnapshot> {
    public static final int OP_OFFSET = 16;
    public static final int LOCK_IDX_MASK = 0xFFFF0000;
    public static final int LOCK_OP_MASK = 0x000000000000FF;

    protected int headIdx;

    protected int holdedLockCnt;

    protected LockLog(String name, int capacity) {
        super(name, capacity);
    }


    @Override public void onWriteLock0(int structureId, long pageId, long page, long pageAddr) {
        log(structureId, pageId, WRITE_LOCK);
    }

    @Override public void onWriteUnlock0(int structureId, long pageId, long page, long pageAddr) {
        log(structureId, pageId, WRITE_UNLOCK);
    }

    @Override public void onReadLock0(int structureId, long pageId, long page, long pageAddr) {
        log(structureId, pageId, READ_LOCK);
    }

    @Override public void onReadUnlock0(int structureId, long pageId, long page, long pageAddr) {
        log(structureId, pageId, READ_UNLOCK);
    }

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

    private void reset() {
        for (int i = 0; i < headIdx; i++)
            setByIndex(i, 0);

        headIdx = 0;
    }

    private long meta(int structureId, int flags) {
        long major = ((long)flags) << 32;

        long minor = (long)structureId;

        return major | minor;
    }

    @Override public LockLogSnapshot snapshot() {
        return new LockLogSnapshot(
            name,
            System.currentTimeMillis(),
            headIdx / 2,
            toList(),
            nextOp,
            nextOpStructureId,
            nextOpPageId
        );
    }

    protected List<LockLogSnapshot.LogEntry> toList() {
        List<LockLogSnapshot.LogEntry> lockLog = new ArrayList<>(capacity);

        for (int i = 0; i < headIdx; i += 2) {
            long metaOnLock = getByIndex(i + 1);

            assert metaOnLock != 0;

            int idx = ((int)(metaOnLock >> 32) & LOCK_IDX_MASK) >> OP_OFFSET;

            assert idx >= 0;

            long pageId = getByIndex(i);

            int op = (int)((metaOnLock >> 32) & LOCK_OP_MASK);
            int structureId = (int)(metaOnLock);

            lockLog.add(new LockLogSnapshot.LogEntry(pageId, structureId, op, idx));
        }

        return lockLog;
    }
}
