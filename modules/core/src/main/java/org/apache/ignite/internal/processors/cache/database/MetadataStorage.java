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

package org.apache.ignite.internal.processors.cache.database;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;

/**
 *
 */
public class MetadataStorage {
    /** */
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    /** */
    private PageMemory pageMem;

    /**
     * @param pageMem Page memory.
     */
    public MetadataStorage(PageMemory pageMem) {
        this.pageMem = pageMem;
    }

    /**
     * @param cacheId Cache ID.
     * @param idxName Index name.
     * @return A tuple, consisting of a root page ID for the given index and a boolean flag
     *      indicating whether the page was newly allocated.
     */
    public IgniteBiTuple<Long, Boolean> getOrAllocateForIndex(
        int cacheId,
        String idxName
    ) throws IgniteCheckedException {
        byte[] idxNameBytes = idxName.getBytes(UTF_8);

        long metaId = metaPage(cacheId);

        Page meta = pageMem.page(metaId);

        try {
            while (true) {
                SearchState state = new SearchState();

                long existingIdxRoot = tryFindIndexRoot(meta, idxNameBytes, state);

                if (existingIdxRoot > 0)
                    return F.t(existingIdxRoot, false);

                long allocatedRoot = allocateAndWriteIndexRoot(meta, cacheId, idxNameBytes, state);

                if (allocatedRoot > 0)
                    return F.t(allocatedRoot, true);
                // else retry.
            }
        }
        finally {
            pageMem.releasePage(meta);
        }
    }

    /**
     * @param meta Cache metadata start page.
     * @param idxNameBytes Index name to find.
     * @return Non-zero root page iD if an index with the given name was found.
     */
    private long tryFindIndexRoot(Page meta, byte[] idxNameBytes, SearchState state) throws IgniteCheckedException {
        ByteBuffer buf = meta.getForRead();

        try {
            // Save version
            state.ver = meta.version();

            long nextPageId = buf.getLong();

            state.writePage = meta.id();

            long rootPageId = scanForIndexRoot(buf, idxNameBytes);

            if (rootPageId > 0)
                return rootPageId;

            while (nextPageId > 0) {
                Page nextPage = pageMem.page(nextPageId);

                try {
                    state.writePage = nextPageId;

                    nextPageId = buf.getLong();

                    rootPageId = scanForIndexRoot(buf, idxNameBytes);

                    if (rootPageId > 0)
                        return rootPageId;
                }
                finally {
                    pageMem.releasePage(nextPage);
                }
            }

            state.position = buf.position();
        }
        finally {
            meta.releaseRead();
        }

        return 0;
    }

    /**
     * @param meta Cache metadata page.
     * @param cacheId Cache ID.
     * @param idxNameBytes Index name bytes.
     * @param state Search state.
     * @return Root page ID for the index.
     */
    private long allocateAndWriteIndexRoot(
        Page meta,
        int cacheId,
        byte[] idxNameBytes,
        SearchState state
    ) throws IgniteCheckedException {
        ByteBuffer buf = meta.getForWrite();

        try {
            // The only case 0 can be returned.
            if (meta.version() != state.ver)
                return 0;

            // Otherwise it is safe to allocate and write data or link new page directly to the saved page.
            long writePageId = state.writePage;

            Page writePage;
            ByteBuffer writeBuf;

            if (writePageId == meta.id()) {
                writePage = meta;
                writeBuf = buf;
            }
            else {
                writePage = pageMem.page(writePageId);

                writeBuf = writePage.getForWrite();
            }

            try {
                long nextPageId = writeBuf.getLong();

                assert nextPageId == 0;

                // Position buffer to the last record.
                writeBuf.position(state.position);

                long idxRoot = pageMem.allocatePage(cacheId, 0, PageIdAllocator.FLAG_IDX);

                if (writeBuf.remaining() < idxNameBytes.length + 9) {
                    // Link new meta page.
                    long newMeta = pageMem.allocatePage(0, 0, PageIdAllocator.FLAG_META);

                    writeBuf.putLong(0, newMeta);

                    // Release old write-locked page.
                    if (writePageId != meta.id()) {
                        writePage.incrementVersion();

                        writePage.releaseWrite(true);

                        pageMem.releasePage(writePage);
                    }

                    writePage = pageMem.page(newMeta);
                    writeBuf = writePage.getForWrite();
                    writePageId = newMeta;
                }

                writeBuf.put((byte)idxNameBytes.length);
                writeBuf.put(idxNameBytes);
                writeBuf.putLong(idxRoot);

                return idxRoot;
            }
            finally {
                if (writePageId != meta.id()) {
                    writePage.incrementVersion();

                    writePage.releaseWrite(true);

                    pageMem.releasePage(writePage);
                }
            }
        }
        finally {
            meta.releaseWrite(true);
        }
    }

    /**
     * @param buf Byte buffer to scan for the index.
     * @param idxName Index name.
     * @return Non-zero value if index with the given name was found.
     */
    private long scanForIndexRoot(ByteBuffer buf, byte[] idxName) {
        // 10 = 1 byte per size + 1 byte minimal index name + 8 bytes page id.
        while (buf.remaining() >= 10) {
            int nameSize = buf.get() & 0xFF;

            if (nameSize == 0) {
                // Rewind.
                buf.position(buf.position() - 1);

                return 0;
            }

            byte[] name = new byte[nameSize];
            buf.get(name);

            long pageId = buf.getLong();

            if (Arrays.equals(name, idxName))
                return pageId;
        }

        return 0;
    }

    /**
     * @param cacheId Cache ID to get meta page for.
     * @return Meta page.
     */
    private long metaPage(int cacheId) throws IgniteCheckedException {
        Page meta = pageMem.metaPage();

        try {
            boolean written = false;

            ByteBuffer buf = meta.getForWrite();

            try {
                int cnt = buf.getShort() & 0xFFFF;
                int cnt0 = cnt;

                int writePos = 0;

                while (cnt0 > 0) {
                    int readId = buf.getInt();
                    long pageId = buf.getLong();

                    if (readId != 0) {
                        if (readId == cacheId)
                            return pageId;

                        cnt0--;
                    }
                    else
                        writePos = buf.position() - 12;
                }

                if (writePos != 0)
                    buf.position(writePos);

                long pageId = pageMem.allocatePage(cacheId, 0, PageIdAllocator.FLAG_META);

                buf.putInt(cacheId);
                buf.putLong(pageId);

                written = true;

                buf.putShort(0, (short)(cnt + 1));

                return pageId;
            }
            finally {
                meta.incrementVersion();

                meta.releaseWrite(written);
            }
        }
        finally {
            pageMem.releasePage(meta);
        }
    }

    /**
     * Search state.
     */
    private static class SearchState {
        /** */
        private int ver;

        /** */
        private long writePage;

        /** */
        private int position;
    }
}
