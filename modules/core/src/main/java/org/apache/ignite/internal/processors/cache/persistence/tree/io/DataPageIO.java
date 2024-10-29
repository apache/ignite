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

package org.apache.ignite.internal.processors.cache.persistence.tree.io;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridStringBuilder;

import static org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO.EntryPart.CACHE_ID;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO.EntryPart.EXPIRE_TIME;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO.EntryPart.KEY;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO.EntryPart.VALUE;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO.EntryPart.VERSION;

/**
 * Data pages IO.
 */
public class DataPageIO extends AbstractDataPageIO<CacheDataRow> {
    /** */
    public static final IOVersions<DataPageIO> VERSIONS = new IOVersions<>(
        new DataPageIO(1)
    );

    /**
     * @param ver Page format version.
     */
    protected DataPageIO(int ver) {
        super(T_DATA, ver);
    }

    /** {@inheritDoc} */
    @Override protected void writeRowData(long pageAddr, int dataOff, int payloadSize, CacheDataRow row,
        boolean newRow) throws IgniteCheckedException {
        assertPageType(pageAddr);

        long addr = pageAddr + dataOff;

        int cacheIdSize = row.cacheId() != 0 ? 4 : 0;

        if (newRow) {
            PageUtils.putShort(addr, 0, (short)payloadSize);
            addr += 2;

            if (cacheIdSize != 0) {
                PageUtils.putInt(addr, 0, row.cacheId());

                addr += cacheIdSize;
            }

            addr += row.key().putValue(addr);
        }
        else
            addr += (2 + cacheIdSize + row.key().valueBytesLength(null));

        addr += row.value().putValue(addr);

        CacheVersionIO.write(addr, row.version(), false);
        addr += CacheVersionIO.size(row.version(), false);

        PageUtils.putLong(addr, 0, row.expireTime());
    }

    /** {@inheritDoc} */
    @Override protected void writeFragmentData(CacheDataRow row, ByteBuffer buf, int rowOff,
        int payloadSize) throws IgniteCheckedException {
        assertPageType(buf);

        final int keySize = row.key().valueBytesLength(null);

        final int valSize = row.value().valueBytesLength(null);

        int written = writeFragment(row, buf, rowOff, payloadSize,
            CACHE_ID, keySize, valSize);
        written += writeFragment(row, buf, rowOff + written, payloadSize - written,
            KEY, keySize, valSize);
        written += writeFragment(row, buf, rowOff + written, payloadSize - written,
            EXPIRE_TIME, keySize, valSize);
        written += writeFragment(row, buf, rowOff + written, payloadSize - written,
            VALUE, keySize, valSize);
        written += writeFragment(row, buf, rowOff + written, payloadSize - written,
            VERSION, keySize, valSize);

        assert written == payloadSize;
    }

    /**
     * Try to write fragment data.
     *
     * @param row Row.
     * @param buf Byte buffer.
     * @param rowOff Offset in row data bytes.
     * @param payloadSize Data length that should be written in this fragment.
     * @param type Type of the part of entry.
     * @param keySize Key size.
     * @param valSize Value size.
     * @return Actually written data.
     * @throws IgniteCheckedException If fail.
     */
    private int writeFragment(
        final CacheDataRow row,
        final ByteBuffer buf,
        final int rowOff,
        final int payloadSize,
        final EntryPart type,
        final int keySize,
        final int valSize
    ) throws IgniteCheckedException {
        if (payloadSize == 0)
            return 0;

        final int prevLen;
        final int curLen;

        int cacheIdSize = row.cacheId() == 0 ? 0 : 4;

        switch (type) {
            case CACHE_ID:
                prevLen = 0;
                curLen = cacheIdSize;

                break;

            case KEY:
                prevLen = cacheIdSize;
                curLen = cacheIdSize + keySize;

                break;

            case EXPIRE_TIME:
                prevLen = cacheIdSize + keySize;
                curLen = cacheIdSize + keySize + 8;

                break;

            case VALUE:
                prevLen = cacheIdSize + keySize + 8;
                curLen = cacheIdSize + keySize + valSize + 8;

                break;

            case VERSION:
                prevLen = cacheIdSize + keySize + valSize + 8;
                curLen = cacheIdSize + keySize + valSize + CacheVersionIO.size(row.version(), false) + 8;

                break;

            default:
                throw new IllegalArgumentException("Unknown entry part type: " + type);
        }

        if (curLen <= rowOff)
            return 0;

        final int len = Math.min(curLen - rowOff, payloadSize);

        if (type == EXPIRE_TIME)
            writeExpireTimeFragment(buf, row.expireTime(), rowOff, len, prevLen);
        else if (type == CACHE_ID)
            writeCacheIdFragment(buf, row.cacheId(), rowOff, len, prevLen);
        else if (type != VERSION) {
            // Write key or value.
            final CacheObject co = type == KEY ? row.key() : row.value();

            co.putValue(buf, rowOff - prevLen, len);
        }
        else
            writeVersionFragment(buf, row.version(), rowOff, len, prevLen);

        return len;
    }

    /**
     * @param buf Byte buffer.
     * @param ver Version.
     * @param rowOff Row offset.
     * @param len Length.
     * @param prevLen previous length.
     */
    private void writeVersionFragment(ByteBuffer buf, GridCacheVersion ver, int rowOff, int len, int prevLen) {
        int verSize = CacheVersionIO.size(ver, false);

        assert len <= verSize : len;

        if (verSize == len) { // Here we check for equality but not <= because version is the last.
            // Here we can write version directly.
            CacheVersionIO.write(buf, ver, false);
        }
        else {
            // We are in the middle of cache version.
            ByteBuffer verBuf = ByteBuffer.allocate(verSize);

            verBuf.order(buf.order());

            CacheVersionIO.write(verBuf, ver, false);

            buf.put(verBuf.array(), rowOff - prevLen, len);
        }
    }

    /**
     * @param buf Byte buffer.
     * @param expireTime Expire time.
     * @param rowOff Row offset.
     * @param len Length.
     * @param prevLen previous length.
     */
    private void writeExpireTimeFragment(ByteBuffer buf, long expireTime, int rowOff, int len, int prevLen) {
        int size = 8;
        if (size <= len)
            buf.putLong(expireTime);
        else {
            ByteBuffer timeBuf = ByteBuffer.allocate(size);

            timeBuf.order(buf.order());

            timeBuf.putLong(expireTime);

            buf.put(timeBuf.array(), rowOff - prevLen, len);
        }
    }

    /**
     * @param buf Buffer.
     * @param cacheId Cache ID.
     * @param rowOff Row offset.
     * @param len Length.
     * @param prevLen Prev length.
     */
    private void writeCacheIdFragment(ByteBuffer buf, int cacheId, int rowOff, int len, int prevLen) {
        if (cacheId == 0)
            return;

        int size = 4;

        if (size <= len)
            buf.putInt(cacheId);
        else {
            ByteBuffer cacheIdBuf = ByteBuffer.allocate(size);

            cacheIdBuf.order(buf.order());

            cacheIdBuf.putInt(cacheId);

            buf.put(cacheIdBuf.array(), rowOff - prevLen, len);
        }
    }

    /** {@inheritDoc} */
    @Override protected void printPage(long addr, int pageSize, GridStringBuilder sb) throws IgniteCheckedException {
        sb.a("DataPageIO [\n");
        printPageLayout(addr, pageSize, sb);
        sb.a("\n]");
    }

    /**
     *
     */
    enum EntryPart {
        /** */
        KEY,

        /** */
        VALUE,

        /** */
        VERSION,

        /** */
        EXPIRE_TIME,

        /** */
        CACHE_ID
    }
}
