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

package org.apache.ignite.internal.processors.query.h2.database;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.database.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cacheobject.IncompleteCacheObject;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;

import static org.apache.ignite.internal.pagemem.PageIdUtils.dwordsOffset;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;

/**
 * Data store for H2 rows.
 */
public class H2RowFactory {
    /** */
    private final PageMemory pageMem;

    /** */
    private final GridCacheContext<?,?> cctx;

    /** */
    private final CacheObjectContext coctx;

    /** */
    private final GridH2RowDescriptor rowDesc;

    /**
     * @param rowDesc Row descriptor.
     * @param cctx Cache context.
     */
    public H2RowFactory(GridH2RowDescriptor rowDesc, GridCacheContext<?,?> cctx) {
        this.rowDesc = rowDesc;
        this.cctx = cctx;

        coctx = cctx.cacheObjectContext();
        pageMem = cctx.shared().database().pageMemory();
    }

    /**
     * !!! This method must be invoked in read or write lock of referring index page. It is needed to
     * !!! make sure that row at this link will be invisible, when the link will be removed from
     * !!! from all the index pages, so that row can be safely erased from the data page.
     *
     * @param link Link.
     * @return Row.
     * @throws IgniteCheckedException If failed.
     */
    public GridH2Row getRow(long link) throws IgniteCheckedException {
        try (Page page = page(pageId(link))) {
            ByteBuffer buf = page.getForRead();

            try {
                DataPageIO io = DataPageIO.VERSIONS.forPage(buf);

                int dataOff = io.getDataOffset(buf, dwordsOffset(link));

                long nextLink = DataPageIO.getNextFragmentLink(buf, dataOff);

                KeyCacheObject key;
                CacheObject val;
                GridCacheVersion ver;

                if (nextLink == 0) {
                    buf.position(dataOff);

                    // Skip entry size.
                    buf.getShort();

                    key = coctx.processor().toKeyCacheObject(coctx, buf);
                    val = coctx.processor().toCacheObject(coctx, buf);

                    ver = readVersion(buf);
                }
                else {
                    DataPageIO.setForFragment(buf, dataOff);

                    final IncompleteEntry entry = new IncompleteEntry();

                    entry.read(buf);

                    assert !entry.isReady() : "Entry is corrupted";

                    while (nextLink != 0) {
                        try (Page p = page(pageId(nextLink))) {
                            final ByteBuffer b = p.getForRead();

                            try {
                                io = DataPageIO.VERSIONS.forPage(b);

                                int off = io.getDataOffset(b, dwordsOffset(nextLink));

                                nextLink = DataPageIO.getNextFragmentLink(b, off);

                                DataPageIO.setForFragment(b, off);

                                entry.read(b);
                            }
                            finally {
                                p.releaseRead();
                            }
                        }
                    }

                    assert entry.isReady() : "Entry is corrupted.";

                    key = entry.key;
                    val = entry.val;
                    ver = entry.ver;
                }

                GridH2Row row;

                try {
                    row = rowDesc.createRow(key, PageIdUtils.partId(link), val, ver, 0);

                    row.link = link;
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }

                assert row.ver != null;

                return row;
            }
            finally {
                page.releaseRead();
            }
        }
    }

    /**
     * @param buf Buffer.
     * @return Version.
     */
    private GridCacheVersion readVersion(final ByteBuffer buf) {
        int topVer = buf.getInt();
        int nodeOrderDrId = buf.getInt();
        long globalTime = buf.getLong();
        long order = buf.getLong();

        return new GridCacheVersion(topVer, nodeOrderDrId, globalTime, order);
    }

    /**
     * @param pageId Page ID.
     * @return Page.
     * @throws IgniteCheckedException If failed.
     */
    private Page page(long pageId) throws IgniteCheckedException {
        return pageMem.page(cctx.cacheId(), pageId);
    }

    /**
     *
     */
    private class IncompleteEntry {
        /** */
        private KeyCacheObject key;

        /** */
        private CacheObject val;

        /** */
        private GridCacheVersion ver;

        /** */
        private IncompleteCacheObject<KeyCacheObject> incompleteKey;

        /** */
        private IncompleteCacheObject<CacheObject> incompleteVal;

        /** */
        private IncompleteCacheObject incompleteVer =
            new IncompleteCacheObject(new byte[DataPageIO.VER_SIZE], (byte) 0);

        /** */
        private int readStage;

        /**
         * Read entry fragment.
         *
         * @param buf To read from.
         * @throws IgniteCheckedException If fail.
         */
        private void read(final ByteBuffer buf) throws IgniteCheckedException {
            if (readStage == 0) {
                incompleteKey = coctx.processor().toKeyCacheObject(coctx, buf, incompleteKey);

                if (incompleteKey.isReady()) {
                    key = incompleteKey.cacheObject();

                    readStage = 1;
                }
            }

            if (readStage == 1) {
                incompleteVal = coctx.processor().toCacheObject(coctx, buf, incompleteVal);

                if (incompleteVal.isReady()) {
                    val = incompleteVal.cacheObject();

                    readStage = 2;
                }
            }

            if (readStage == 2) {
                incompleteVer.readData(buf);

                if (incompleteVer.isReady()) {
                    final ByteBuffer verBuf = ByteBuffer.wrap(incompleteVer.data());

                    verBuf.order(buf.order());

                    ver = readVersion(verBuf);

                    readStage = 3;
                }
            }

            assert !buf.hasRemaining();
        }

        /**
         * @return {@code True} if entry fully read.
         */
        private boolean isReady() {
            return readStage == 3;
        }
    }
}
