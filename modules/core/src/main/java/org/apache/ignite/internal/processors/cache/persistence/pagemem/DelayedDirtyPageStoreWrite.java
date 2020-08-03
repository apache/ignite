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
import org.apache.ignite.internal.processors.cache.persistence.PageStoreWriter;
import org.apache.ignite.internal.util.GridUnsafe;
import org.jetbrains.annotations.Nullable;

/**
 * Not thread safe and stateful class for page replacement of one page with write() delay. This allows to write page
 * content without holding segment lock. Page data is copied into temp buffer during {@link #writePage(FullPageId,
 * ByteBuffer, int)} and then sent to real implementation by {@link #finishReplacement()}.
 */
public class DelayedDirtyPageStoreWrite implements PageStoreWriter {
    /** Real flush dirty page implementation. */
    private final PageStoreWriter flushDirtyPage;

    /** Page size. */
    private final int pageSize;

    /** Thread local with byte buffers. */
    private final ThreadLocal<ByteBuffer> byteBufThreadLoc;

    /** Replacing pages tracker, used to register & unregister pages being written. */
    private final DelayedPageReplacementTracker tracker;

    /** Full page id to be written on {@link #finishReplacement()} or null if nothing to write. */
    @Nullable private FullPageId fullPageId;

    /** Byte buffer with page data to be written on {@link #finishReplacement()} or null if nothing to write. */
    @Nullable private ByteBuffer byteBuf;

    /** Partition update tag to be used in{@link #finishReplacement()} or null if -1 to write. */
    private int tag = -1;

    /**
     * @param flushDirtyPage real writer to save page to store.
     * @param byteBufThreadLoc thread local buffers to use for pages copying.
     * @param pageSize page size.
     * @param tracker tracker to lock/unlock page reads.
     */
    public DelayedDirtyPageStoreWrite(
        PageStoreWriter flushDirtyPage,
        ThreadLocal<ByteBuffer> byteBufThreadLoc,
        int pageSize,
        DelayedPageReplacementTracker tracker
    ) {
        this.flushDirtyPage = flushDirtyPage;
        this.pageSize = pageSize;
        this.byteBufThreadLoc = byteBufThreadLoc;
        this.tracker = tracker;
    }

    /** {@inheritDoc} */
    @Override public void writePage(FullPageId fullPageId, ByteBuffer byteBuf, int tag) {
        tracker.lock(fullPageId);

        ByteBuffer tlb = byteBufThreadLoc.get();

        tlb.rewind();

        long writeAddr = GridUnsafe.bufferAddress(tlb);
        long origBufAddr = GridUnsafe.bufferAddress(byteBuf);

        GridUnsafe.copyMemory(origBufAddr, writeAddr, pageSize);

        this.fullPageId = fullPageId;
        this.byteBuf = tlb;
        this.tag = tag;
    }

    /**
     * Runs actual write if required. Method is 'no op' if there was no page selected for replacement.
     * @throws IgniteCheckedException if write failed.
     */
    public void finishReplacement() throws IgniteCheckedException {
        if (byteBuf == null && fullPageId == null)
            return;

        try {
            flushDirtyPage.writePage(fullPageId, byteBuf, tag);
        }
        finally {
            tracker.unlock(fullPageId);

            fullPageId = null;
            byteBuf = null;
            tag = -1;
        }
    }
}
