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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.HashSet;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.FullPageId;

/**
 * Delayed page writes tracker. Provides delayed write implementations and allows to check if page is actually being
 * written to page store.
 */
public class DelayedPageReplacementTracker {
    /** Page size. */
    private final int pageSize;

    /** Flush dirty page real implementation. */
    private final ReplacedPageWriter flushDirtyPage;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * Page IDs which are locked for reading from store. Page content is being written right now. guarded by collection
     * object monitor.
     */
    private final Collection<FullPageId> locked = new HashSet<>(Runtime.getRuntime().availableProcessors() * 2);

    /**
     * Has locked pages, flag for fast check if there are some pages, what were replaced and is being written.
     * Write to field is guarded by {@link #locked} monitor.
     */
    private volatile boolean hasLockedPages;

    /** Byte buffer thread local. */
    private final ThreadLocal<ByteBuffer> byteBufThreadLoc
        = new ThreadLocal<ByteBuffer>() {
        @Override protected ByteBuffer initialValue() {
            ByteBuffer buf = ByteBuffer.allocateDirect(pageSize);

            buf.order(ByteOrder.LITTLE_ENDIAN);

            return buf;
        }
    };

    /**
     * Dirty page write for replacement operations thread local. Because page write {@link DelayedDirtyPageWrite} is
     * stateful and not thread safe, this thread local protects from GC pressure on pages replacement.
     */
    private final ThreadLocal<DelayedDirtyPageWrite> delayedPageWriteThreadLoc
        = new ThreadLocal<DelayedDirtyPageWrite>() {
        @Override protected DelayedDirtyPageWrite initialValue() {
            return new DelayedDirtyPageWrite(flushDirtyPage, byteBufThreadLoc, pageSize,
                DelayedPageReplacementTracker.this);
        }
    };

    /**
     * @param pageSize Page size.
     * @param flushDirtyPage Flush dirty page.
     * @param log Logger.
     */
    public DelayedPageReplacementTracker(int pageSize, ReplacedPageWriter flushDirtyPage,
        IgniteLogger log) {
        this.pageSize = pageSize;
        this.flushDirtyPage = flushDirtyPage;
        this.log = log;
    }

    /**
     * @return delayed page write implementation, finish method to be called to actually write page.
     */
    public DelayedDirtyPageWrite delayedPageWrite() {
        return delayedPageWriteThreadLoc.get();
    }

    /**
     * @param id full page ID to lock from read
     */
    public void lock(FullPageId id) {
        synchronized (locked) {
            hasLockedPages = true;

            boolean add = locked.add(id);

            assert add : "Double locking of page for replacement is not possible";
        }
    }

    /**
     * Method is returned when page is available to be loaded from store, or waits for replacement finish.
     *
     * @param id full page ID to be loaded from store.
     */
    public void waitUnlock(FullPageId id) {
        if (!hasLockedPages)
            return;

        synchronized (locked) {
            if (!hasLockedPages)
                return;

            while (locked.contains(id)) {
                if (log.isDebugEnabled())
                    log.debug("Found replaced page [" + id + "] which is being written to page store, wait for finish replacement");

                try {
                    locked.wait();
                }
                catch (InterruptedException e) {
                    throw new IgniteInterruptedException(e);
                }
            }
        }
    }

    /**
     * @param id full page ID, which write has been finished, it is available for reading.
     */
    public void unlock(FullPageId id) {
        synchronized (locked) {
            boolean rmv = locked.remove(id);

            assert rmv : "Unlocking page ID never locked, id " + id;

            if (locked.isEmpty())
                hasLockedPages = false;

            locked.notifyAll();
        }
    }
}
