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
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersion;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.GridStringBuilder;

import static org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO.EntryPart.CACHE_ID;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO.EntryPart.EXPIRE_TIME;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO.EntryPart.KEY;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO.EntryPart.MVCC_INFO;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO.EntryPart.VALUE;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO.EntryPart.VERSION;

/**
 * Data pages IO.
 */
public class DataPageIO extends AbstractDataPageIO<CacheDataRow> {
    /** */
    public static final int MVCC_INFO_SIZE = 4 * 8;
    
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
        assert row.mvccCoordinatorVersion() > 0 == row.mvccCounter() > 0;

        long addr = pageAddr + dataOff;

        int cacheIdSize = row.cacheId() != 0 ? 4 : 0;
        int mvccInfoSize = row.mvccCoordinatorVersion() > 0 ? MVCC_INFO_SIZE : 0;

        if (newRow) {
            PageUtils.putShort(addr, 0, (short)payloadSize);
            addr += 2;

            if (mvccInfoSize > 0) {
                // xid_min.
                PageUtils.putLong(addr, 0, row.mvccCoordinatorVersion());
                PageUtils.putLong(addr, 8, row.mvccCounter());

                // empty xid_max.
                PageUtils.putLong(addr, 16, row.newMvccCoordinatorVersion());
                PageUtils.putLong(addr, 24, row.newMvccCounter());

                addr += mvccInfoSize;
            }

            if (cacheIdSize != 0) {
                PageUtils.putInt(addr, 0, row.cacheId());

                addr += cacheIdSize;
            }

            addr += row.key().putValue(addr);
        }
        else
            addr += (2 + mvccInfoSize + cacheIdSize  + row.key().valueBytesLength(null));

        addr += row.value().putValue(addr);

        CacheVersionIO.write(addr, row.version(), false);
        addr += CacheVersionIO.size(row.version(), false);

        PageUtils.putLong(addr, 0, row.expireTime());
    }

    /** {@inheritDoc} */
    @Override protected void writeFragmentData(CacheDataRow row, ByteBuffer buf, int rowOff,
        int payloadSize) throws IgniteCheckedException {
        assert row.mvccCoordinatorVersion() > 0 == row.mvccCounter() > 0;

        final int keySize = row.key().valueBytesLength(null);

        final int valSize = row.value().valueBytesLength(null);

        int written = writeFragment(row, buf, rowOff, payloadSize,
            MVCC_INFO, keySize, valSize);
        written += writeFragment(row, buf, rowOff + written, payloadSize - written,
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
        int mvccInfoSize = row.mvccCoordinatorVersion() > 0 ? MVCC_INFO_SIZE : 0;

        switch (type) {
            case MVCC_INFO:
                prevLen = 0;
                curLen = mvccInfoSize;

                break;

            case CACHE_ID:
                prevLen = mvccInfoSize;
                curLen = mvccInfoSize + cacheIdSize;

                break;

            case KEY:
                prevLen = mvccInfoSize + cacheIdSize;
                curLen = mvccInfoSize + cacheIdSize + keySize;

                break;

            case EXPIRE_TIME:
                prevLen = mvccInfoSize + cacheIdSize + keySize;
                curLen = mvccInfoSize + cacheIdSize + keySize + 8;

                break;

            case VALUE:
                prevLen = mvccInfoSize + cacheIdSize + keySize + 8;
                curLen = mvccInfoSize + cacheIdSize + keySize + valSize + 8;

                break;

            case VERSION:
                prevLen = mvccInfoSize + cacheIdSize + keySize + valSize + 8;
                curLen = mvccInfoSize + cacheIdSize + keySize + valSize + CacheVersionIO.size(row.version(), false) + 8;

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
        else if (type == MVCC_INFO)
            writeMvccInfoFragment(buf, row.mvccCoordinatorVersion(), row.mvccCounter(),
                row.newMvccCoordinatorVersion(), row.newMvccCounter(), len);
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
     * Marks row as removed by new version.
     *
     * @param pageAddr Page address.
     * @param dataOff Data offset.
     * @param newVer New version.
     */
    public void markRemoved(long pageAddr, int dataOff, MvccVersion newVer) {
        long addr = pageAddr + dataOff;

        markRemoved(addr, newVer.coordinatorVersion(), newVer.counter());
    }

    /**
     * Marks row as removed by new version.
     *
     * @param pageAddr Page address.
     * @param itemId Item ID.
     * @param pageSize Page size.
     * @param mvccCrd Mvcc coordinator.
     * @param mvccCntr Mvcc counter.
     */
    public void markRemoved(long pageAddr, int itemId, int pageSize, long mvccCrd, long mvccCntr) {
        int dataOff = getDataOffset(pageAddr, itemId, pageSize);

        long addr = pageAddr + dataOff + (isFragmented(pageAddr, dataOff) ? 10 : 2);

        markRemoved(addr, mvccCrd, mvccCntr);
    }

    /**
     * Marks row removed.
     *
     * @param addr Address.
     * @param mvccCrd Mvcc coordinator.
     * @param mvccCntr Mvcc counter.
     */
    private void markRemoved(long addr, long mvccCrd, long mvccCntr) {
        // Skip xid_min.
        addr += 16;

        // TODO uncomment when IGNITE-7764 is implemented.
        //long prevCrd = PageUtils.getLong(addr, 0);
        //long prevCntr =  PageUtils.getLong(addr, 8);

        //assert prevCrd == 0 && prevCntr == MVCC_COUNTER_NA;

        PageUtils.putLong(addr, 0, mvccCrd);
        PageUtils.putLong(addr, 8, mvccCntr);
    }

    /**
     * Returns MVCC coordinator number.
     *
     * @param pageAddr Page address.
     * @param dataOff Data offset.
     * @return MVCC coordinator number.
     */
    public long mvccCoordinator(long pageAddr, int dataOff) {
        long addr = pageAddr + dataOff;

        return PageUtils.getLong(addr, 0);
    }

    /**
     * Returns MVCC counter value.
     *
     * @param pageAddr Page address.
     * @param dataOff Data offset.
     * @return MVCC counter value.
     */
    public long mvccCounter(long pageAddr, int dataOff) {
        long addr = pageAddr + dataOff;

        return PageUtils.getLong(addr, 8);
    }

    /**
     * Returns new MVCC coordinator number.
     *
     * @param pageAddr Page address.
     * @param dataOff Data offset.
     * @return New MVCC coordinator number.
     */
    public long newMvccCoordinator(long pageAddr, int dataOff) {
        long addr = pageAddr + dataOff;

        // Skip xid_min.
        addr += 16;

        return PageUtils.getLong(addr, 0);
    }

    /**
     * Returns new MVCC counter value.
     *
     * @param pageAddr Page address.
     * @param dataOff Data offset.
     * @return New MVCC counter value.
     */
    public long newMvccCounter(long pageAddr, int dataOff) {
        long addr = pageAddr + dataOff;

        // Skip xid_min.
        addr += 16;

        return PageUtils.getLong(addr, 8);
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

        assert len <= verSize: len;

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

    /**
     * @param buf Byte buffer.
     * @param mvccCrd Coordinator version.
     * @param mvccCnt Counter.
     * @param newMvccCrd New coordinator version.
     * @param newMvccCnt New counter version.
     * @param len Length.
     */
    private void writeMvccInfoFragment(ByteBuffer buf, long mvccCrd, long mvccCnt, long newMvccCrd,
        long newMvccCnt, int len) {
        if (mvccCrd == 0)
            return;

        assert len >= MVCC_INFO_SIZE : "Mvcc info should fit on the one page!";

        // xid_min.
        buf.putLong(mvccCrd);
        buf.putLong(mvccCnt);

        // xid_max.
        buf.putLong(newMvccCrd);
        buf.putLong(newMvccCnt);
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
        CACHE_ID,

        /** */
        MVCC_INFO
    }
}
