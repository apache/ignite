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

package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker;

import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockTrackerManager.MemoryCalculator;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;
import org.apache.ignite.lang.IgniteFuture;

import static org.apache.ignite.internal.pagemem.PageIdUtils.flag;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageIndex;
import static org.apache.ignite.internal.pagemem.PageIdUtils.partId;
import static org.apache.ignite.internal.util.IgniteUtils.hexInt;
import static org.apache.ignite.internal.util.IgniteUtils.hexLong;

/**
 * Abstract page lock tracker.
 */
public abstract class PageLockTracker<T extends PageLockDump> implements PageLockListener, DumpSupported<T> {
    /** */
    private static final long OVERHEAD_SIZE = 16 + 8 + 8 + 4 + 4 + 4 + 8 + 8 + 4 + 4 + 8;

    /** */
    public static final int OP_OFFSET = 16;

    /** */
    public static final int LOCK_IDX_MASK = 0xFFFF0000;

    /** */
    public static final int LOCK_OP_MASK = 0x000000000000FF;

    /** Page read lock operation id. */
    public static final int READ_LOCK = 1;

    /** Page read unlock operation id. */
    public static final int READ_UNLOCK = 2;

    /** Page write lock operation id. */
    public static final int WRITE_LOCK = 3;

    /** Page write unlock operation id. */
    public static final int WRITE_UNLOCK = 4;

    /** Page read before lock operation id. */
    public static final int BEFORE_READ_LOCK = 5;

    /** Page write before lock operation id. */
    public static final int BEFORE_WRITE_LOCK = 6;

    /** */
    protected final String name;

    /** */
    protected final PageMetaInfoStore pages;

    /** Counter for track lock/unlock operations. */
    protected int heldLockCnt;

    /** */
    protected int nextOp;

    /** */
    protected int nextOpStructureId;

    /** */
    protected long nextOpPageId;

    /** */
    private long opCntr;

    /** */
    private volatile boolean dump;

    /** */
    private volatile boolean locked;

    /** */
    private volatile InvalidContext<T> invalidCtx;

    /**
     *
     */
    protected PageLockTracker(String name, PageMetaInfoStore pages, MemoryCalculator memCalc) {
        this.name = name;
        this.pages = pages;

        memCalc.onHeapAllocated(OVERHEAD_SIZE);
    }

    /** */
    public void onBeforeWriteLock0(int structureId, long pageId, long page) {
        this.nextOp = BEFORE_WRITE_LOCK;
        this.nextOpStructureId = structureId;
        this.nextOpPageId = pageId;
    }

    /** */
    public void onBeforeReadLock0(int structureId, long pageId, long page) {
        this.nextOp = BEFORE_READ_LOCK;
        this.nextOpStructureId = structureId;
        this.nextOpPageId = pageId;
    }

    /** {@inheritDoc} */
    @Override public void onBeforeWriteLock(int structureId, long pageId, long page) {
        if (isInvalid())
            return;

        lock();

        try {
            onBeforeWriteLock0(structureId, pageId, page);
        }
        finally {
            unLock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onWriteLock(int structureId, long pageId, long page, long pageAddr) {
        if (checkFailedLock(pageAddr) || isInvalid())
            return;

        lock();

        try {
            onWriteLock0(structureId, pageId, page, pageAddr);
        }
        finally {
            unLock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onWriteUnlock(int structureId, long pageId, long page, long pageAddr) {
        if (isInvalid())
            return;

        lock();

        try {
            onWriteUnlock0(structureId, pageId, page, pageAddr);
        }
        finally {
            unLock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onBeforeReadLock(int structureId, long pageId, long page) {
        if (isInvalid())
            return;

        lock();

        try {
            onBeforeReadLock0(structureId, pageId, page);
        }
        finally {
            unLock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onReadLock(int structureId, long pageId, long page, long pageAddr) {
        if (checkFailedLock(pageAddr) || isInvalid())
            return;

        lock();

        try {
            onReadLock0(structureId, pageId, page, pageAddr);
        }
        finally {
            unLock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onReadUnlock(int structureId, long pageId, long page, long pageAddr) {
        if (isInvalid())
            return;

        lock();

        try {
            onReadUnlock0(structureId, pageId, page, pageAddr);
        }
        finally {
            unLock();
        }
    }

    /** */
    public abstract void onWriteLock0(int structureId, long pageId, long page, long pageAddr);

    /** */
    public abstract void onWriteUnlock0(int structureId, long pageId, long page, long pageAddr);

    /** */
    public abstract void onReadLock0(int structureId, long pageId, long page, long pageAddr);

    /** */
    public abstract void onReadUnlock0(int structureId, long pageId, long page, long pageAddr);

    /** */
    public boolean isInvalid() {
        return invalidCtx != null;
    }

    /** */
    private boolean checkFailedLock(long pageAddr) {
        if (pageAddr == 0) {
            this.nextOp = 0;
            this.nextOpStructureId = 0;
            this.nextOpPageId = 0;

            return true;
        }

        return false;
    }

    /** */
    public InvalidContext<T> invalidContext() {
        return invalidCtx;
    }

    /** */
    protected void free() {
        pages.free();
    }

    /** */
    protected void invalid(String msg) {
        T dump = snapshot();

        invalidCtx = new InvalidContext<>(msg, dump);
    }

    /** */
    private void lock() {
        while (!lock0()) {
            // Busy wait.
        }
    }

    /** */
    private boolean lock0() {
        awaitDump();

        locked = true;
        if (dump) {
            locked = false;

            return false;
        }

        return true;
    }

    /** */
    private void unLock() {
        opCntr++;

        locked = false;
    }

    /** */
    private void awaitDump() {
        while (dump) {
            // Busy wait.
        }
    }

    /** */
    private void awaitLocks() {
        while (locked) {
            // Busy wait.
        }
    }

    /**
     * @return Number of locks operations.
     */
    public long operationsCounter() {
        // Read  volatile for thread safety.
        boolean locked = this.locked;

        return opCntr;
    }

    /**
     *
     */
    public int heldLocksNumber() {
        // Read  volatile for thread safety.
        boolean locked = this.locked;

        return heldLockCnt;
    }

    /** */
    protected boolean validateOperation(int structureId, long pageId, int op) {
        if (nextOpStructureId == 0 || nextOp == 0 || nextOpPageId == 0)
            return true;

        if ((op == READ_LOCK && nextOp != BEFORE_READ_LOCK) ||
            (op == WRITE_LOCK && nextOp != BEFORE_WRITE_LOCK) ||
            (structureId != nextOpStructureId) ||
            (pageId != nextOpPageId)) {

            invalid("Unepected operation: " +
                "exp=" + argsToString(nextOpStructureId, nextOpPageId, nextOp) + "," +
                "actl=" + argsToString(structureId, pageId, op)
            );

            return false;
        }

        return true;
    }

    /** */
    protected abstract T snapshot();

    /** {@inheritDoc} */
    @Override public synchronized boolean acquireSafePoint() {
        return dump ? false : (dump = true);

    }

    /** {@inheritDoc} */
    @Override public synchronized boolean releaseSafePoint() {
        return !dump ? false : !(dump = false);
    }

    /** {@inheritDoc} */
    @Override public synchronized T dump() {
        boolean needRelease = acquireSafePoint();

        awaitLocks();

        T dump0 = snapshot();

        if (needRelease)
            releaseSafePoint();

        return dump0;
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<T> dumpSync() {
        throw new UnsupportedOperationException();
    }

    /** */
    public static String argsToString(int structureId, long pageId, int flags) {
        return "[structureId=" + structureId + ", pageId" + pageIdToString(pageId) + "]";
    }

    /** */
    public static String pageIdToString(long pageId) {
        return "pageId=" + pageId
            + " [pageIdHex=" + hexLong(pageId)
            + ", partId=" + partId(pageId) + ", pageIdx=" + pageIndex(pageId)
            + ", flags=" + hexInt(flag(pageId)) + "]";
    }
}
