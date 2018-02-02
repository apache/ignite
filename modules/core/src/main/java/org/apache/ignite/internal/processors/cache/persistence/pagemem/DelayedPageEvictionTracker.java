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
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.internal.pagemem.FullPageId;

/**
 *
 */
public class DelayedPageEvictionTracker {
    /** Page size. */
    private final int pageSize;

    /** Flush dirty page. */
    private final EvictedPageWriter flushDirtyPage;

    /** Locked. */
    private final Set<FullPageId> locked = new HashSet<>();

    /** Byte buffer thread local. */
    private final ThreadLocal<ByteBuffer> byteBufThreadLoc
        = new ThreadLocal<ByteBuffer>() {
        @Override protected ByteBuffer initialValue() {
            ByteBuffer buf = ByteBuffer.allocateDirect(pageSize);

            buf.order(ByteOrder.LITTLE_ENDIAN);

            return buf;
        }
    };

    /** Dirty page eviction thread local. */
    private final ThreadLocal<DelayedDirtyPageWrite> dirtyPageEvictionThreadLoc
        = new ThreadLocal<DelayedDirtyPageWrite>() {
        @Override protected DelayedDirtyPageWrite initialValue() {
            return new DelayedDirtyPageWrite(flushDirtyPage, byteBufThreadLoc, pageSize,
                DelayedPageEvictionTracker.this);
        }
    };

    /**
     * @param pageSize Page size.
     * @param flushDirtyPage Flush dirty page.
     */
    public DelayedPageEvictionTracker(int pageSize, EvictedPageWriter flushDirtyPage) {
        this.pageSize = pageSize;
        this.flushDirtyPage = flushDirtyPage;
    }

    /**
     * @return
     */
    public DelayedDirtyPageWrite delayedPageWrite() {
        return dirtyPageEvictionThreadLoc.get();
    }

    /**
     * @param id
     */
    public void lock(FullPageId id) {
        synchronized (locked) {
            boolean add = locked.add(id);
            assert add : "Double locking of page for eviction is not possible";
        }
    }

    /**
     * @param id
     * @return
     */
    public void waitUnlock(FullPageId id) {
        synchronized (locked) {
            while (locked.contains(id)) {
                System.err.println("Found evicting page [" + id + "], wait for finish eviction");
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
     * @param id
     */
    public void unlock(FullPageId id) {
        synchronized (locked) {
            boolean rmv = locked.remove(id);

            assert rmv : "Unlocking page ID never locked, id " + id;

            locked.notifyAll();
        }
    }
}
