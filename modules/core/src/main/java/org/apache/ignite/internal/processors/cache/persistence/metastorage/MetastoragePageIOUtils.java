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

package org.apache.ignite.internal.processors.cache.persistence.metastorage;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.util.GridUnsafe;

/**
 * {@link MetastorageInnerIO} and {@link MetastorageLeafIO} use the same page format but cannot explicitly share the
 * same implementation, because they are inherited from two independant abstract classes. So this util class contains
 * that implementation and should be used to avoid code duplication.
 */
public class MetastoragePageIOUtils {
    /**
     * @see MetastorageBPlusIO#getLink(long, int)
     */
    public static <IO extends BPlusIO<MetastorageRow> & MetastorageBPlusIO>
    long getLink(IO io, long pageAddr, int idx) {
        assert idx < io.getCount(pageAddr) : idx;

        return PageUtils.getLong(pageAddr, io.offset(idx));
    }

    /**
     * @see MetastorageBPlusIO#getKeySize(long, int)
     */
    public static <IO extends BPlusIO<MetastorageRow> & MetastorageBPlusIO>
    short getKeySize(IO io, long pageAddr, int idx) {
        return PageUtils.getShort(pageAddr, io.offset(idx) + 8);
    }

    /**
     * @see MetastorageBPlusIO#getKey(long, int, MetastorageRowStore)
     */
    public static <IO extends BPlusIO<MetastorageRow> & MetastorageBPlusIO>
    String getKey(IO io, long pageAddr, int idx, MetastorageRowStore rowStore) throws IgniteCheckedException {
        int off = io.offset(idx);
        int len = PageUtils.getShort(pageAddr, off + 8);

        if (len > MetastorageTree.MAX_KEY_LEN) {
            long keyLink = PageUtils.getLong(pageAddr, off + 10);

            byte[] keyBytes = rowStore.readRow(keyLink);

            assert keyBytes != null : "[pageAddr=" + Long.toHexString(pageAddr) + ", idx=" + idx + ']';

            return new String(keyBytes);
        }
        else {
            byte[] keyBytes = PageUtils.getBytes(pageAddr, off + 10, len);

            return new String(keyBytes);
        }
    }

    /**
     * @see MetastorageBPlusIO#getDataRow(long, int, MetastorageRowStore)
     */
    public static <IO extends BPlusIO<MetastorageRow> & MetastorageBPlusIO>
    MetastorageDataRow getDataRow(IO io, long pageAddr, int idx, MetastorageRowStore rowStore)
        throws IgniteCheckedException
    {
        long link = io.getLink(pageAddr, idx);

        int off = io.offset(idx);
        int len = PageUtils.getShort(pageAddr, off + 8);

        if (len > MetastorageTree.MAX_KEY_LEN) {
            long keyLink = PageUtils.getLong(pageAddr, off + 10);

            byte[] keyBytes = rowStore.readRow(keyLink);

            assert keyBytes != null : "[pageAddr=" + Long.toHexString(pageAddr) + ", idx=" + idx + ']';

            return new MetastorageDataRow(link, new String(keyBytes), keyLink);
        }
        else {
            byte[] keyBytes = PageUtils.getBytes(pageAddr, off + 10, len);

            return new MetastorageDataRow(link, new String(keyBytes), 0L);
        }
    }

    /**
     * @see BPlusIO#store(long, int, org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO, long, int)
     */
    public static <IO extends BPlusIO<MetastorageRow> & MetastorageBPlusIO>
    void store(IO dstIo, long dstPageAddr, int dstIdx, BPlusIO<MetastorageRow> srcIo, long srcPageAddr, int srcIdx) {
        int srcOff = srcIo.offset(srcIdx);
        int dstOff = dstIo.offset(dstIdx);

        GridUnsafe.copyMemory(srcPageAddr + srcOff, dstPageAddr + dstOff, MetastorageTree.MAX_KEY_LEN + 10);
    }

    /**
     * @see BPlusIO#storeByOffset(long, int, Object)
     */
    public static <IO extends BPlusIO<MetastorageRow> & MetastorageBPlusIO>
    void storeByOffset(IO io, long pageAddr, int off, MetastorageRow row) {
        assert row.link() != 0;

        PageUtils.putLong(pageAddr, off, row.link());

        byte[] bytes = row.key().getBytes();
        assert bytes.length <= Short.MAX_VALUE;

        if (row.keyLink() != 0) {
            PageUtils.putShort(pageAddr, off + 8, (short)bytes.length);
            PageUtils.putLong(pageAddr, off + 10, row.keyLink());
        }
        else {
            assert bytes.length <= MetastorageTree.MAX_KEY_LEN;

            PageUtils.putShort(pageAddr, off + 8, (short)bytes.length);
            PageUtils.putBytes(pageAddr, off + 10, bytes);
        }
    }
}
