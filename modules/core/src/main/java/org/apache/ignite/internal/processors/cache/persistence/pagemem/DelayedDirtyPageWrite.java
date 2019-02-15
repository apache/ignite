/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.util.GridUnsafe;
import org.jetbrains.annotations.Nullable;

/**
 * Not thread safe and stateful class for page replacement of one page with write() delay. This allows to write page
 * content without holding segment lock. Page data is copied into temp buffer during {@link #writePage(FullPageId,
 * ByteBuffer, int)} and then sent to real implementation by {@link #finishReplacement()}.
 */
public class DelayedDirtyPageWrite implements ReplacedPageWriter {
    /** Real flush dirty page implementation. */
    private final ReplacedPageWriter flushDirtyPage;

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
    public DelayedDirtyPageWrite(ReplacedPageWriter flushDirtyPage,
        ThreadLocal<ByteBuffer> byteBufThreadLoc, int pageSize,
        DelayedPageReplacementTracker tracker) {
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
