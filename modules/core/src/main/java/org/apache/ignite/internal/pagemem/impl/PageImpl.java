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

package org.apache.ignite.internal.pagemem.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.SB;

import static org.apache.ignite.internal.pagemem.impl.PageMemoryImpl.PAGE_OVERHEAD;

/**
 *
 */
public class PageImpl extends AbstractQueuedSynchronizer implements Page {
    /** */
    private static final AtomicIntegerFieldUpdater<PageImpl> refCntUpd =
        AtomicIntegerFieldUpdater.newUpdater(PageImpl.class, "refCnt");

    /** */
    private final FullPageId fullId;

    /** Pointer to the system page start. */
    private final long ptr;

    /** */
    @SuppressWarnings("unused")
    private volatile int refCnt;

    /** */
    private final ByteBuffer buf;

    /** Buffer copy to ensure writes during a checkpoint. */
    private TmpCopy cp;

    /** */
    private final PageMemoryImpl pageMem;

    /**
     * @param fullId Full page ID.
     * @param pageMem Page memory.
     */
    PageImpl(FullPageId fullId, long ptr, PageMemoryImpl pageMem) {
        this.fullId = fullId;
        this.ptr = ptr;
        this.pageMem = pageMem;

        buf = pageMem.wrapPointer(ptr + PAGE_OVERHEAD, pageMem.pageSize());
    }

    /**
     * @param buf Byte buffer.
     * @return The given buffer back.
     */
    private ByteBuffer reset(ByteBuffer buf) {
        buf.order(ByteOrder.nativeOrder());

        buf.rewind();

        return buf;
    }

    /** {@inheritDoc} */
    @Override public long id() {
        return fullId.pageId();
    }

    /** {@inheritDoc} */
    @Override public FullPageId fullId() {
        return fullId;
    }

    /** {@inheritDoc} */
    @Override protected boolean tryAcquire(int ignore) {
        return compareAndSetState(0, -1);
    }

    /** {@inheritDoc} */
    @Override protected boolean tryRelease(int ignore) {
        boolean res = compareAndSetState(-1, 0);

        assert res : "illegal monitor state";

        return res;
    }

    /** {@inheritDoc} */
    @Override protected int tryAcquireShared(int ignore) {
        for (;;) {
            int state = getState();

            if (state == -1 || hasQueuedPredecessors()) // TODO we are fair here, may be this an overkill
                return -1;

            if (compareAndSetState(state, state + 1))
                return 1;
        }
    }

    /** {@inheritDoc} */
    @Override protected boolean tryReleaseShared(int ignore) {
        for (;;) {
            int state = getState();

            if (state <= 0)
                throw new IllegalMonitorStateException("State: " + state);

            if (compareAndSetState(state, state - 1))
                return true;
        }
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer getForRead() {
        acquireShared(1);

        pageMem.writeCurrentTimestamp(ptr);

        if (cp != null) {
            assert refCntUpd.get(this) > 0 : this;

            assert pageMem.isInCheckpoint(fullId) :
                "The page has a temporary buffer but is not in the checkpoint set: " + this;

            return reset(cp.userBuf.asReadOnlyBuffer());
        }

        return reset(buf.asReadOnlyBuffer());
    }

    /** {@inheritDoc} */
    @Override public void releaseRead() {
        releaseShared(1);
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer getForInitialWrite() {
        assert getExclusiveOwnerThread() == null: fullId();
        assert getState() == 0: fullId();

        markDirty(true);

        pageMem.writeCurrentTimestamp(ptr);

        return reset(buf);
    }

    /** {@inheritDoc} */
    @Override public void finishInitialWrite() {
        pageMem.beforeReleaseWrite(fullId, pageMem.wrapPointer(ptr, pageMem.systemPageSize()));
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer getForWrite() {
        acquire(1); // This call is not reentrant.

        assert getExclusiveOwnerThread() == null: fullId();

        setExclusiveOwnerThread(Thread.currentThread());

        pageMem.writeCurrentTimestamp(ptr);

        assert refCntUpd.get(this) > 0 : this;

        // Create a buffer copy if the page needs to be checkpointed.
        if (pageMem.isInCheckpoint(fullId)) {
            if (cp == null) {
                // Pin the page until checkpoint is not finished.
                acquireReference();

                byte[] pageData = new byte[pageMem.systemPageSize()];

                GridUnsafe.copyMemory(null, ptr, pageData, GridUnsafe.BYTE_ARR_OFF, pageData.length);

                cp = new TmpCopy(pageData);
            }

            return reset(cp.userBuf);
        }
        else
            assert cp == null;

        return reset(buf);
    }

    /**
     * If page was concurrently modified during the checkpoint phase, this method will flush all changes from the
     * temporary location to main memory.
     * This method must be called outside of the segment write lock because we can ask for another pages
     *      while holding a page read or write lock.
     */
    public boolean flushCheckpoint(IgniteLogger log) {
        acquire(1); // This call is not reentrant.

        assert getExclusiveOwnerThread() == null: fullId();

        setExclusiveOwnerThread(Thread.currentThread());

        try {
            assert refCntUpd.get(this) > 0 : this;

            if (cp != null) {
                ByteBuffer cpBuf = reset(cp.userBuf);

                reset(buf);

                buf.put(cpBuf);

                // It is important to clear checkpoint status before the write lock is released.
                pageMem.clearCheckpoint(fullId);

                pageMem.setDirty(fullId, ptr, cp.dirty, true);

                cp = null;

                return releaseReference();
            }
            else {
                // It is important to clear checkpoint status before the write lock is released.
                pageMem.clearCheckpoint(fullId);

                pageMem.setDirty(fullId, ptr, false, true);
            }

            return false;
        }
        finally {
            setExclusiveOwnerThread(null);

            release(1);
        }
    }

    /**
     * @return {@code True} if page has a temp on-heap copy.
     */
    public boolean hasTempCopy() {
        return cp != null;
    }

    /**
     * @return Pointer.
     */
    long pointer() {
        return ptr;
    }

    /**
     * Mark dirty.
     */
    private void markDirty(boolean forceAdd) {
        if (cp != null)
            cp.dirty = true;
        else
            pageMem.setDirty(fullId, ptr, true, forceAdd);
    }

    /** {@inheritDoc} */
    @Override public void releaseWrite(boolean markDirty) {
        assert getState() == -1;
        assert getExclusiveOwnerThread() == Thread.currentThread() : "illegal monitor state";

        if (markDirty) {
            markDirty(false);

            if (cp != null)
                pageMem.beforeReleaseWrite(fullId, reset(cp.sysBuf));
            else
                pageMem.beforeReleaseWrite(fullId, pageMem.wrapPointer(ptr, pageMem.systemPageSize()));
        }

        setExclusiveOwnerThread(null);

        release(1);
    }

    /**
     * @return {@code true} If this page is not acquired neither for read nor for write.
     */
    public boolean isFree() {
        return getState() == 0;
    }

    /** {@inheritDoc} */
    @Override public boolean isDirty() {
        if (cp != null)
            return cp.dirty;

        return pageMem.isDirty(ptr);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        SB sb = new SB("PageImpl [handle=");

        sb.a(fullId);
        sb.a(", relPtr=");
        sb.appendHex(pageMem.readRelative(ptr));
        sb.a(", absPtr=");
        sb.appendHex(ptr);
        sb.a(", cp=");
        sb.a(cp);
        sb.a(']');

        return sb.toString();
    }

    /**
     * Increments reference count. This method is invoked from the PageMemoryImpl segment read lock, so it
     * must be synchronized.
     */
    boolean acquireReference() {
        while (true) {
            int cnt = refCntUpd.get(this);

            if (cnt >= 0) {
                if (refCntUpd.compareAndSet(this, cnt, cnt + 1))
                    break;
            }
            else
                return false;
        }

        pageMem.writeCurrentTimestamp(ptr);

        return true;
    }

    /**
     * Checks if page is acquired by a thread.
     */
    boolean isAcquired() {
        return refCnt != 0;
    }

    /**
     * @return {@code True} if last reference has been released. This method is invoked
     */
    boolean releaseReference() {
        while (true) {
            int refs = refCntUpd.get(this);

            assert refs > 0 : fullId;

            int next = refs == 1 ? -1 : refs - 1;

            if (refCntUpd.compareAndSet(this, refs, next))
                return next == -1;
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        pageMem.releasePage(this);
    }

    /**
     * Temporary page copy.
     */
    private static class TmpCopy {
        /** */
        private ByteBuffer userBuf;

        /** */
        private ByteBuffer sysBuf;

        /** */
        private boolean dirty;

        /**
         * @param pageData Copied page data.
         */
        private TmpCopy(byte[] pageData) {
            sysBuf = ByteBuffer.wrap(pageData);

            sysBuf.position(PAGE_OVERHEAD);

            userBuf = sysBuf.slice();
        }

        @Override public String toString() {
            SB sb = new SB("TmpBuf [dirty=");

            sb.a(dirty);

            sb.a(']');

            return sb.toString();
        }
    }
}
