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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.util.GridUnsafe;
import org.jetbrains.annotations.Nullable;

/**
 * Not thread safe class for evicting one page with delay, not holding segment lock
 */
public class DelayedDirtyPageWrite implements EvictedPageWriter {
    /** Flush dirty page. */
    private final EvictedPageWriter flushDirtyPage;

    /** Page size. */
    private final int pageSize;

    /** Byte buffer thread local. */
    private final ThreadLocal<ByteBuffer> byteBufThreadLoc;

    /** Tracker. */
    private final DelayedPageEvictionTracker tracker;

    /** Full page id. */
    @Nullable private FullPageId fullPageId;

    /** Byte buffer. */
    @Nullable private ByteBuffer byteBuf;

    /** Tag. */
    private int tag;

    /**
     * @param flushDirtyPage
     * @param byteBufThreadLoc
     * @param pageSize
     * @param tracker
     */
    public DelayedDirtyPageWrite(EvictedPageWriter flushDirtyPage,
        ThreadLocal<ByteBuffer> byteBufThreadLoc, int pageSize,
        DelayedPageEvictionTracker tracker) {
        this.flushDirtyPage = flushDirtyPage;
        this.pageSize = pageSize;
        this.byteBufThreadLoc = byteBufThreadLoc;
        this.tracker = tracker;
    }

    /**
     * @throws IgniteCheckedException
     */
    public void finishEviction() throws IgniteCheckedException {
        if (byteBuf == null)
            return;

        try {
            flushDirtyPage.applyx(fullPageId, byteBuf, tag);
        }
        finally {
            tracker.unlock(fullPageId);

            fullPageId = null;
            byteBuf = null;
            tag = -1;
        }
    }


    /** {@inheritDoc} */
    @Override public void applyx(FullPageId fullPageId, ByteBuffer byteBuf, int tag) {
        tracker.lock(fullPageId);

        ByteBuffer tmpWriteBuf = byteBufThreadLoc.get();

        tmpWriteBuf.rewind();

        long writeAddr = GridUnsafe.bufferAddress(tmpWriteBuf);

        long origBufAddr = GridUnsafe.bufferAddress(byteBuf);
        GridUnsafe.copyMemory(origBufAddr, writeAddr, pageSize);

        this.fullPageId = fullPageId;
        this.byteBuf = tmpWriteBuf;
        this.tag = tag;
    }
}
