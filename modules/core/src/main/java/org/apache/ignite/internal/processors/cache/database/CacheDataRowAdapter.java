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

    /* TODO IGNITE-4534: Field is used only during initialization. Can be refactored into main class and builder. */
    byte[] marshalledKey;

    /* TODO IGNITE-4534: Field is used only during initialization. Can be refactored into main class and builder. */
    byte keyType;

    /** */
    @GridToStringInclude
    protected CacheObject val;

    /* TODO IGNITE-4534: Field is used only during initialization. Can be refactored into main class and builder. */
    byte[] marshalledVal;

    /* TODO IGNITE-4534: Field is used only during initialization. Can be refactored into main class and builder. */
    byte valType;

    /** */
    protected long expireTime = -1;

    /** */
    @GridToStringInclude
    protected GridCacheVersion ver;

    /** Cache id, specified only for data rows of evictable cache.*/
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
        readRowData(cctx.shared().database().pageMemory(), rowData, cctx.cacheId());

        CacheObjectContext cacheObjCtx = cctx.cacheObjectContext();

        initCacheObjects(rowData, cacheObjCtx);
    }

    /**
     * @param rowData Row data.
     * @param cacheObjCtx Cache object context.
     */
    public void initCacheObjects(RowData rowData,
        CacheObjectContext cacheObjCtx) throws IgniteCheckedException {
        if (rowData != RowData.NO_KEY)
            key = cacheObjCtx.processor().toKeyCacheObject(cacheObjCtx, keyType, marshalledKey);

        if (rowData != RowData.KEY_ONLY)
            val = cacheObjCtx.processor().toCacheObject(cacheObjCtx, valType, marshalledVal);
    }

    /**
     * @param pageMem Page mem.
     * @param rowData Row data.
     */
    public void readRowData(PageMemory pageMem, RowData rowData) throws IgniteCheckedException {
        readRowData(pageMem, rowData, 0);
    }

    /**
     * @param pageMem Page memory.
     * @param rowData Row data.
     * @param cacheId Cache id.
     */
    public void readRowData(PageMemory pageMem, RowData rowData, int cacheId) throws IgniteCheckedException {
        assert link != 0 : "link";
        assert key == null : "key";

        long nextLink = link;
        IncompleteObject<?> incomplete = null;
        boolean first = true;

        do {
            try (Page page = pageMem.page(cacheId, pageId(nextLink))) {
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
                            readFullRow(pageAddr + data.offset(), rowData);

                            return;
                        }

                        first = false;
                    }

                    ByteBuffer buf = pageMem.pageBuffer(pageAddr);

                    buf.position(data.offset());
                    buf.limit(data.offset() + data.payloadSize());

                    boolean keyOnly = rowData == RowData.KEY_ONLY;

                    incomplete = readFragment(buf, keyOnly, incomplete);

                    if (keyOnly && marshalledKey != null)
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
     * @param buf Buffer.
     * @param keyOnly {@code true} If need to read only key object.
     * @param incomplete Incomplete object.
     * @throws IgniteCheckedException If failed.
     * @return Read object.
     */
    private IncompleteObject<?> readFragment(
        ByteBuffer buf,
        boolean keyOnly,
        IncompleteObject<?> incomplete
    ) throws IgniteCheckedException {
        // Read key.
        if (marshalledKey == null) {
            incomplete = readIncompleteKey(buf, (IncompleteCacheObject)incomplete);

            if (marshalledKey == null || keyOnly)
                return incomplete;

            incomplete = null;
        }

        if (expireTime == -1) {
            incomplete = readIncompleteExpireTime(buf, incomplete);

            if (expireTime == -1)
                return incomplete;

            incomplete = null;
        }

        if (cacheId == 0) {
            // TODO IGNITE-4534: Distinguish situations [cache is not evictable, cacheId = 0] and
            // TODO IGNITE-4534: [cacheId is fragmented, first part is read, cacheId = 0].
            incomplete = readIncompleteCacheId(buf, incomplete);

            if (cacheId == 0)
                return incomplete;

            incomplete = null;
        }

        // Read value.
        if (marshalledVal == null) {
            incomplete = readIncompleteValue(buf, (IncompleteCacheObject)incomplete);

            if (marshalledVal == null)
                return incomplete;

            incomplete = null;
        }

        // Read version.
        if (ver == null)
            incomplete = readIncompleteVersion(buf, incomplete);

        return incomplete;
    }

    /**
     * @param addr Address.
     * @param rowData Required row data.
     * @throws IgniteCheckedException If failed.
     */
    private void readFullRow(long addr, RowData rowData) throws IgniteCheckedException {
        int off = 0;

        int len = PageUtils.getInt(addr, off);
        off += 4;

        if (rowData != RowData.NO_KEY) {
            keyType = PageUtils.getByte(addr, off);
            off++;

            marshalledKey = PageUtils.getBytes(addr, off, len);
            off += len;

            if (rowData == RowData.KEY_ONLY)
                return;
        }
        else
            off += len + 1;

        len = PageUtils.getInt(addr, off);
        off += 4;

        valType = PageUtils.getByte(addr, off);
        off++;

        marshalledVal = PageUtils.getBytes(addr, off, len);
        off += len;

        ver = CacheVersionIO.read(addr + off, false);

        off += CacheVersionIO.size(ver, false);

        expireTime = PageUtils.getLong(addr, off);
        off += 8;

        /* TODO IGNITE-4534: store less than 4 bytes for non-evictable caches */
        cacheId = PageUtils.getInt(addr, off);
    }

    /**
     * @param buf Buffer.
     * @param incomplete Incomplete object.
     * @return Incomplete object.
     * @throws IgniteCheckedException If failed.
     */
    private IncompleteCacheObject readIncompleteKey(
        ByteBuffer buf,
        IncompleteCacheObject incomplete
    ) throws IgniteCheckedException {
        if (incomplete == null)
            incomplete = new IncompleteCacheObject(buf);

        if (incomplete.isReady())
            return incomplete;

        incomplete.readData(buf);

        if (incomplete.isReady()) {
            marshalledKey = incomplete.data();

            keyType = incomplete.type();
        }

        return incomplete;
    }

    /**
     * @param buf Buffer.
     * @param incomplete Incomplete object.
     * @return Incomplete object.
     * @throws IgniteCheckedException If failed.
     */
    private IncompleteCacheObject readIncompleteValue(
        ByteBuffer buf,
        IncompleteCacheObject incomplete
    ) throws IgniteCheckedException {
        if (incomplete == null)
            incomplete = new IncompleteCacheObject(buf);

        if (incomplete.isReady())
            return incomplete;

        incomplete.readData(buf);

        if (incomplete.isReady()) {
            marshalledVal = incomplete.data();

            valType = incomplete.type();
        }

        return incomplete;
    }

    /**
     * @param buf Buffer.
     * @param incomplete Incomplete.
     */
    private IncompleteObject<?> readIncompleteCacheId(
        ByteBuffer buf,
        IncompleteObject<?> incomplete
    ) throws IgniteCheckedException {
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
        return ver != null && marshalledKey != null && marshalledVal != null;
    }

    /** {@inheritDoc} */
    @Override public KeyCacheObject key() {
        assert key != null : "Key is not ready: " + this;

        return key;
    }

    /**
     * @param key Key.
     */
    public void key(KeyCacheObject key) {
        assert key != null;

        this.key = key;
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
    @Override public int cacheId() {
        return cacheId;
    }

    /**
     * @param pageId Page ID.
     * @param cctx Cache context.
     * @return Page.
     * @throws IgniteCheckedException If failed.
     */
    private Page page(final long pageId, final GridCacheContext cctx) throws IgniteCheckedException {
        return cctx.memoryPolicy().pageMemory().page(cctx.cacheId(), pageId);
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
