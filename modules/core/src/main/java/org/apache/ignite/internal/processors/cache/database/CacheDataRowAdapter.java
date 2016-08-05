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
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.database.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.IncompleteCacheObject;

import java.nio.ByteBuffer;

import static org.apache.ignite.internal.pagemem.PageIdUtils.dwordsOffset;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;

/**
 * Assembles entry from data pages.
 */
public class CacheDataRowAdapter implements CacheDataRow {
    /** */
    protected long link;

    /** */
    protected KeyCacheObject key;

    /** */
    protected CacheObject val;

    /** */
    protected GridCacheVersion ver;

    /** Fragmented entry read phase. Refer {@link #readFragment(ByteBuffer, CacheObjectContext, boolean)} */
    private int phase;

    /** */
    private IncompleteCacheObject<KeyCacheObject> incompleteKey;

    /** */
    private IncompleteCacheObject<CacheObject> incompleteVal;

    /** */
    private IncompleteCacheObject incompleteVer;

    /**
     * @param link Link.
     */
    public CacheDataRowAdapter(long link) {
        this.link = link;
    }

    /**
     * Assemble row from data pages.
     *
     * @param cctx Cache context.
     * @param keyOnly {@code True} if need read only key object.
     * @throws IgniteCheckedException If failed.
     */
    public void assemble(GridCacheContext<?, ?> cctx, boolean keyOnly) throws IgniteCheckedException {
        assert cctx != null;
        assert link != 0;

        phase = 0;

        final CacheObjectContext coctx = cctx.cacheObjectContext();

        try (Page page = page(pageId(link), cctx)) {
            ByteBuffer buf = page.getForRead();

            try {
                DataPageIO io = DataPageIO.VERSIONS.forPage(buf);

                int dataOff = io.getDataOffset(buf, dwordsOffset(link));

                long nextLink = io.getNextFragmentLink(buf, dataOff);

                if (nextLink == 0) {
                    buf.position(dataOff);

                    // Skip entry size.
                    buf.getShort();

                    key = coctx.processor().toKeyCacheObject(coctx, buf);

                    if (!keyOnly) {
                        val = coctx.processor().toCacheObject(coctx, buf);

                        ver = readVersion(buf);
                    }
                }
                else {
                    io.setPositionAndLimitOnFragment(buf, dataOff);

                    readFragment(buf, coctx, keyOnly);

                    if (keyOnly && isKeyReady())
                        return;

                    while (nextLink != 0) {
                        try (final Page p = page(pageId(nextLink), cctx)) {
                            try {
                                final ByteBuffer b = p.getForRead();

                                io = DataPageIO.VERSIONS.forPage(b);

                                final int off = io.getDataOffset(b, dwordsOffset(nextLink));

                                nextLink = io.getNextFragmentLink(b, off);

                                io.setPositionAndLimitOnFragment(b, off);

                                readFragment(b, coctx, keyOnly);

                                if (keyOnly && isKeyReady())
                                    return;
                            }
                            finally {
                                p.releaseRead();
                            }
                        }
                    }
                }
            }
            finally {
                page.releaseRead();
            }
        }
    }

    /**
     * @return {@code True} if key is read.
     */
    public boolean isKeyReady() {
        return phase > 0;
    }

    /**
     * @return {@code True} if entry is ready.
     */
    public boolean isReady() {
        return key != null && val != null && ver != null;
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject key() {
        assert key != null : "Key is not ready";

        return key;
    }

    /** {@inheritDoc} */
    @Override public CacheObject value() {
        assert val != null : "Value is not ready";

        return val;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        assert ver != null : "Version is not ready";

        return ver;
    }

    /** {@inheritDoc} */
    @Override public int partition() {
        return PageIdUtils.partId(link);
    }

    /** {@inheritDoc} */
    @Override public long link() {
        return link;
    }

    /** {@inheritDoc} */
    @Override public void link(long link) {
        throw new UnsupportedOperationException();
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
     * Read entry fragment.
     *
     * @param buf To read from.
     * @throws IgniteCheckedException If fail.
     */
    private void readFragment(final ByteBuffer buf, final CacheObjectContext coctx, final boolean keyOnly) throws IgniteCheckedException {
        if (phase == 0) {
            incompleteKey = coctx.processor().toKeyCacheObject(coctx, buf, incompleteKey);

            if (incompleteKey.isReady()) {
                key = incompleteKey.cacheObject();

                phase = 1;
            }
        }

        if (keyOnly)
            return;

        if (phase == 1) {
            incompleteVal = coctx.processor().toCacheObject(coctx, buf, incompleteVal);

            if (incompleteVal.isReady()) {
                val = incompleteVal.cacheObject();

                phase = 2;
            }
        }

        if (phase == 2) {
            if (buf.remaining() >= DataPageIO.VER_SIZE) {
                ver = readVersion(buf);

                phase = 3;
            }
            else {
                if (incompleteVer == null)
                    incompleteVer = new IncompleteCacheObject(new byte[DataPageIO.VER_SIZE], (byte)0);

                incompleteVer.readData(buf);

                if (incompleteVer.isReady()) {
                    final ByteBuffer verBuf = ByteBuffer.wrap(incompleteVer.data());

                    verBuf.order(buf.order());

                    ver = readVersion(verBuf);

                    phase = 3;
                }
            }
        }

        assert !buf.hasRemaining();
    }

    /**
     * @param pageId Page ID.
     * @param cctx Cache context.
     * @return Page.
     * @throws IgniteCheckedException If failed.
     */
    private Page page(final long pageId, final GridCacheContext cctx) throws IgniteCheckedException {
        return cctx.shared().database().pageMemory().page(cctx.cacheId(), pageId);
    }
}
