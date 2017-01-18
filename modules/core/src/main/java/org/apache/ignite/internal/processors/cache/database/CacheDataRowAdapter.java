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

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.Page;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IncompleteCacheObject;
import org.apache.ignite.internal.processors.cache.IncompleteObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.database.tree.io.CacheVersionIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.DataPagePayload;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.pagemem.PageIdUtils.itemId;
import static org.apache.ignite.internal.pagemem.PageIdUtils.pageId;

/**
 * Cache data row adapter.
 */
public class CacheDataRowAdapter implements CacheDataRow {
    /** */
    @GridToStringExclude
    protected long link;

    /** */
    @GridToStringInclude
    protected KeyCacheObject key;

    /** */
    @GridToStringInclude
    protected CacheObject val;

    /** */
    protected long expireTime;

    /** */
    @GridToStringInclude
    protected GridCacheVersion ver;

    /**
     * @param link Link.
     */
    public CacheDataRowAdapter(long link) {
        // Link can be 0 here.
        this.link = link;
    }

    /**
     * Read row from data pages.
     *
     * @param cctx Cache context.
     * @param keyOnly {@code true} If need to read only key object.
     * @throws IgniteCheckedException If failed.
     */
    public final void initFromLink(GridCacheContext<?, ?> cctx, boolean keyOnly) throws IgniteCheckedException {
        assert cctx != null : "cctx";
        assert link != 0 : "link";
        assert key == null : "key";

        final CacheObjectContext coctx = cctx.cacheObjectContext();

        long nextLink = link;
        IncompleteObject<?> incomplete = null;
        boolean first = true;

        do {
            PageMemory pageMem = cctx.shared().database().pageMemory();

            try (Page page = page(pageId(nextLink), cctx)) {
                long pageAddr = page.getForReadPointer(); // Non-empty data page must not be recycled.

                assert pageAddr != 0L : nextLink;

                try {
                    DataPageIO io = DataPageIO.VERSIONS.forPage(pageAddr);

                    DataPagePayload data = io.readPayload(pageAddr,
                        itemId(nextLink),
                        pageMem.pageSize());

                    nextLink = data.nextLink();

                    if (first) {
                        if (nextLink == 0) {
                            // Fast path for a single page row.
                            readFullRow(coctx, pageAddr + data.offset(), keyOnly);

                            return;
                        }

                        first = false;
                    }

                    ByteBuffer buf = pageMem.pageBuffer(pageAddr);

                    buf.position(data.offset());
                    buf.limit(data.offset() + data.payloadSize());

                    incomplete = readFragment(coctx, buf, keyOnly, incomplete);

                    if (keyOnly && key != null)
                        return;
                }
                finally {
                    page.releaseRead();
                }
            }
        }
        while(nextLink != 0);

        assert isReady() : "ready";
    }

    /**
     * @param coctx Cache object context.
     * @param buf Buffer.
     * @param keyOnly {@code true} If need to read only key object.
     * @param incomplete Incomplete object.
     * @throws IgniteCheckedException If failed.
     * @return Read object.
     */
    private IncompleteObject<?> readFragment(
        CacheObjectContext coctx,
        ByteBuffer buf,
        boolean keyOnly,
        IncompleteObject<?> incomplete
    ) throws IgniteCheckedException {
        // Read key.
        if (key == null) {
            incomplete = readIncompleteKey(coctx, buf, (IncompleteCacheObject)incomplete);

            if (key == null || keyOnly)
                return incomplete;

            incomplete = null;
        }

        if (expireTime == -1) {
            incomplete = readIncompleteExpireTime(buf, incomplete);

            if (expireTime == -1)
                return incomplete;

            incomplete = null;
        }

        // Read value.
        if (val == null) {
            incomplete = readIncompleteValue(coctx, buf, (IncompleteCacheObject)incomplete);

            if (val == null)
                return incomplete;

            incomplete = null;
        }

        // Read version.
        if (ver == null)
            incomplete = readIncompleteVersion(buf, incomplete);

        return incomplete;
    }

    /**
     * @param coctx Cache object context.
     * @param addr Address.
     * @param keyOnly {@code true} If need to read only key object.
     * @throws IgniteCheckedException If failed.
     */
    private void readFullRow(CacheObjectContext coctx, long addr, boolean keyOnly) throws IgniteCheckedException {
        int off = 0;

        int len = PageUtils.getInt(addr, off);
        off += 4;

        byte type = PageUtils.getByte(addr, off);
        off++;

        byte[] bytes = PageUtils.getBytes(addr, off, len);
        off += len;

        key = coctx.processor().toKeyCacheObject(coctx, type, bytes);

        if (keyOnly) {
            assert key != null: "key";

            return;
        }

        len = PageUtils.getInt(addr, off);
        off += 4;

        type = PageUtils.getByte(addr, off);
        off++;

        bytes = PageUtils.getBytes(addr, off, len);
        off += len;

        val = coctx.processor().toCacheObject(coctx, type, bytes);

        ver = CacheVersionIO.read(addr + off, false);

        off += CacheVersionIO.size(ver, false);

        expireTime = PageUtils.getLong(addr, off);

        assert isReady(): "ready";
    }

    /**
     * @param coctx Cache object context.
     * @param buf Buffer.
     * @param incomplete Incomplete object.
     * @return Incomplete object.
     * @throws IgniteCheckedException If failed.
     */
    private IncompleteCacheObject readIncompleteKey(
        CacheObjectContext coctx,
        ByteBuffer buf,
        IncompleteCacheObject incomplete
    ) throws IgniteCheckedException {
        incomplete = coctx.processor().toKeyCacheObject(coctx, buf, incomplete);

        if (incomplete.isReady()) {
            key = (KeyCacheObject)incomplete.object();

            assert key != null;
        }
        else
            assert !buf.hasRemaining();

        return incomplete;
    }

    /**
     * @param coctx Cache object context.
     * @param buf Buffer.
     * @param incomplete Incomplete object.
     * @return Incomplete object.
     * @throws IgniteCheckedException If failed.
     */
    private IncompleteCacheObject readIncompleteValue(
        CacheObjectContext coctx,
        ByteBuffer buf,
        IncompleteCacheObject incomplete
    ) throws IgniteCheckedException {
        incomplete = coctx.processor().toCacheObject(coctx, buf, incomplete);

        if (incomplete.isReady()) {
            val = incomplete.object();

            assert val != null;
        }
        else
            assert !buf.hasRemaining();

        return incomplete;
    }

    /**
     * @param buf Buffer.
     * @param incomplete Incomplete object.
     * @return Incomplete object.
     * @throws IgniteCheckedException If failed.
     */
    private IncompleteObject<?> readIncompleteExpireTime(
        ByteBuffer buf,
        IncompleteObject<?> incomplete
    ) throws IgniteCheckedException {
        if (incomplete == null) {
            int remaining = buf.remaining();

            if (remaining == 0)
                return null;

            int size = 8;

            if (remaining >= size) {
                expireTime = buf.getLong();

                assert expireTime >= 0 : expireTime;

                return null;
            }

            incomplete = new IncompleteObject<>(new byte[size]);
        }

        incomplete.readData(buf);

        if (incomplete.isReady()) {
            final ByteBuffer timeBuf = ByteBuffer.wrap(incomplete.data());

            timeBuf.order(buf.order());

            expireTime = timeBuf.getLong();

            assert expireTime >= 0;
        }

        return incomplete;
    }

    /**
     * @param buf Buffer.
     * @param incomplete Incomplete object.
     * @return Incomplete object.
     * @throws IgniteCheckedException If failed.
     */
    private IncompleteObject<?> readIncompleteVersion(
        ByteBuffer buf,
        IncompleteObject<?> incomplete
    ) throws IgniteCheckedException {
        if (incomplete == null) {
            int remaining = buf.remaining();

            if (remaining == 0)
                return null;

            int size = CacheVersionIO.readSize(buf, false);

            if (remaining >= size) {
                // If the whole version is on a single page, just read it.
                ver = CacheVersionIO.read(buf, false);

                assert !buf.hasRemaining(): buf.remaining();
                assert ver != null;

                return null;
            }

            // We have to read multipart version.
            incomplete = new IncompleteObject<>(new byte[size]);
        }

        incomplete.readData(buf);

        if (incomplete.isReady()) {
            final ByteBuffer verBuf = ByteBuffer.wrap(incomplete.data());

            verBuf.order(buf.order());

            ver = CacheVersionIO.read(verBuf, false);

            assert ver != null;
        }

        assert !buf.hasRemaining();

        return incomplete;
    }

    /**
     * @return {@code True} if entry is ready.
     */
    public boolean isReady() {
        return ver != null && val != null && key != null;
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject key() {
        assert key != null : "Key is not ready: " + this;

        return key;
    }

    /** {@inheritDoc} */
    @Override public CacheObject value() {
        assert val != null : "Value is not ready: " + this;

        return val;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        assert ver != null : "Version is not ready: " + this;

        return ver;
    }

    /** {@inheritDoc} */
    @Override public long expireTime() {
        return expireTime;
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

    /** {@inheritDoc} */
    @Override public int hash() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheDataRowAdapter.class, this, "link", U.hexLong(link));
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
