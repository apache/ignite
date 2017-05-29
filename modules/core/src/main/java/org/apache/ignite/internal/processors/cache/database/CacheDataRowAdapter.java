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
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
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
import org.jetbrains.annotations.Nullable;

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
    protected long expireTime = -1;

    /** */
    @GridToStringInclude
    protected GridCacheVersion ver;

    /** */
    @GridToStringInclude
    protected int cacheId;

    /**
     * @param link Link.
     */
    public CacheDataRowAdapter(long link) {
        // Link can be 0 here.
        this.link = link;
    }

    /**
     * @param key Key.
     * @param val Value.
     * @param expireTime Expire time.
     * @param ver Version.
     */
    public CacheDataRowAdapter(KeyCacheObject key, CacheObject val, GridCacheVersion ver, long expireTime) {
        this.key = key;
        this.val = val;
        this.ver = ver;
        this.expireTime = expireTime;
    }

    /**
     * Read row from data pages.
     *
     * @param cctx Cache context.
     * @param rowData Required row data.
     * @throws IgniteCheckedException If failed.
     */
    public final void initFromLink(GridCacheContext<?, ?> cctx, RowData rowData) throws IgniteCheckedException {
        initFromLink(cctx, cctx.shared(), cctx.memoryPolicy().pageMemory(), rowData);
    }

    /**
     * Read row from data pages.
     * Can be called with cctx == null, if cache instance is unknown, but its ID is stored in the data row.
     *
     * @param cctx Cctx.
     * @param sharedCtx Shared context.
     * @param pageMem Page memory.
     * @param rowData Row data.
     */
    public final void initFromLink(
        @Nullable GridCacheContext<?, ?> cctx,
        GridCacheSharedContext<?, ?> sharedCtx,
        PageMemory pageMem,
        RowData rowData)
        throws IgniteCheckedException {
        assert link != 0 : "link";
        assert key == null : "key";

        CacheObjectContext coctx = null;

        if (cctx != null) {
            cacheId = cctx.memoryPolicy().config().getPageEvictionMode() == DataPageEvictionMode.DISABLED ?
                cctx.cacheId() : 0; // Force cacheId reading for evictable memory policies.

            coctx = cctx.cacheObjectContext();
        }

        long nextLink = link;
        IncompleteObject<?> incomplete = null;
        boolean first = true;

        do {
            final long pageId = pageId(nextLink);
            final long page = pageMem.acquirePage(cacheId, pageId);

            try {
                long pageAddr = pageMem.readLock(cacheId, pageId, page); // Non-empty data page must not be recycled.

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
                            readFullRow(sharedCtx, coctx, pageAddr + data.offset(), rowData);

                            return;
                        }

                        first = false;
                    }

                    ByteBuffer buf = pageMem.pageBuffer(pageAddr);

                    buf.position(data.offset());
                    buf.limit(data.offset() + data.payloadSize());

                    boolean keyOnly = rowData == RowData.KEY_ONLY;

                    incomplete = readFragment(sharedCtx, coctx, buf, keyOnly, incomplete);

                    if (keyOnly && key != null)
                        return;
                }
                finally {
                    pageMem.readUnlock(cacheId, pageId, page);
                }
            }
            finally {
                pageMem.releasePage(cacheId, pageId, page);
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
        GridCacheSharedContext<?, ?> sharedCtx,
        CacheObjectContext coctx,
        ByteBuffer buf,
        boolean keyOnly,
        IncompleteObject<?> incomplete
    ) throws IgniteCheckedException {
        if (cacheId == 0) {
            incomplete = readIncompleteCacheId(buf, incomplete);

            if (cacheId == 0)
                return incomplete;

            incomplete = null;
        }

        if (coctx == null)
            coctx = sharedCtx.cacheContext(cacheId).cacheObjectContext();

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
     * @param rowData Required row data.
     * @throws IgniteCheckedException If failed.
     */
    private void readFullRow(
        GridCacheSharedContext<?, ?> sharedCtx,
        CacheObjectContext coctx,
        long addr,
        RowData rowData)
        throws IgniteCheckedException {
        int off = 0;

        if (cacheId == 0) {
            cacheId = PageUtils.getInt(addr, off);

            off += 4;
        }

        if (coctx == null)
            coctx = sharedCtx.cacheContext(cacheId).cacheObjectContext();

        int len = PageUtils.getInt(addr, off);
        off += 4;

        if (rowData != RowData.NO_KEY) {
            byte type = PageUtils.getByte(addr, off);
            off++;

            byte[] bytes = PageUtils.getBytes(addr, off, len);
            off += len;

            key = coctx.kernalContext().cacheObjects().toKeyCacheObject(coctx, type, bytes);

            if (rowData == RowData.KEY_ONLY)
                return;
        }
        else
            off += len + 1;

        len = PageUtils.getInt(addr, off);
        off += 4;

        byte type = PageUtils.getByte(addr, off);
        off++;

        byte[] bytes = PageUtils.getBytes(addr, off, len);
        off += len;

        val = coctx.kernalContext().cacheObjects().toCacheObject(coctx, type, bytes);

        ver = CacheVersionIO.read(addr + off, false);

        off += CacheVersionIO.size(ver, false);

        expireTime = PageUtils.getLong(addr, off);
    }

    /**
     * @param buf Buffer.
     * @param incomplete Incomplete.
     */
    private IncompleteObject<?> readIncompleteCacheId(
        ByteBuffer buf,
        IncompleteObject<?> incomplete
    ) {
        if (incomplete == null) {
            int remaining = buf.remaining();

            if (remaining == 0)
                return null;

            int size = 4;

            if (remaining >= size) {
                cacheId = buf.getInt();

                return null;
            }

            incomplete = new IncompleteObject<>(new byte[size]);
        }

        incomplete.readData(buf);

        if (incomplete.isReady()) {
            final ByteBuffer timeBuf = ByteBuffer.wrap(incomplete.data());

            timeBuf.order(buf.order());

            cacheId = timeBuf.getInt();
        }

        return incomplete;
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
        incomplete = coctx.kernalContext().cacheObjects().toKeyCacheObject(coctx, buf, incomplete);

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
        incomplete = coctx.kernalContext().cacheObjects().toCacheObject(coctx, buf, incomplete);

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
    ) {
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

    /**
     * @param key Key.
     */
    @Override public void key(KeyCacheObject key) {
        assert key != null;

        this.key = key;
    }

    /** {@inheritDoc} */
    @Override public int cacheId() {
        return cacheId;
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

    /**
     *
     */
    public enum RowData {
        /** */
        FULL,

        /** */
        KEY_ONLY,

        /** */
        NO_KEY
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheDataRowAdapter.class, this, "link", U.hexLong(link));
    }
}
