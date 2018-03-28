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

package org.apache.ignite.internal.processors.cache.mvcc.txlog;

import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;

/** */
public class TxLogLeafIO extends BPlusLeafIO<TxKey> implements TxLogIO {
    /** */
    public static final IOVersions<TxLogLeafIO> VERSIONS = new IOVersions<>(new TxLogLeafIO(1));

    /**
     * @param ver Page format version.
     */
    protected TxLogLeafIO(int ver) {
        super(T_TX_LOG_LEAF, ver, 17);
    }

    /** {@inheritDoc} */
    @Override public void storeByOffset(long pageAddr, int off, TxKey row) {
        TxRow row0 = (TxRow)row;

        setMajor(pageAddr, off, row0.major());
        setMinor(pageAddr, off, row0.minor());
        setState(pageAddr, off, row0.state());
    }

    /** {@inheritDoc} */
    @Override public void store(long dstPageAddr, int dstIdx, BPlusIO<TxKey> srcIo, long srcPageAddr, int srcIdx) {
        TxLogIO srcIo0 = (TxLogIO)srcIo;

        int srcOff = srcIo.offset(srcIdx);
        int dstOff = offset(dstIdx);

        setMajor(dstPageAddr, dstOff, srcIo0.getMajor(srcPageAddr, srcOff));
        setMinor(dstPageAddr, dstOff, srcIo0.getMinor(srcPageAddr, srcOff));
        setState(dstPageAddr, dstOff, srcIo0.getState(srcPageAddr, srcOff));
    }

    /** {@inheritDoc} */
    @Override public TxKey getLookupRow(BPlusTree<TxKey, ?> tree, long pageAddr, int idx) {
        int off = offset(idx);

        return new TxRow(
            getMajor(pageAddr, off),
            getMinor(pageAddr, off),
            getState(pageAddr, off));
    }

    /** {@inheritDoc} */
    @Override public int compare(long pageAddr, int off, TxKey row) {
        int cmp = Long.compare(getMajor(pageAddr, off), row.major());

        return cmp != 0 ? cmp : Long.compare(getMinor(pageAddr, off), row.minor());
    }

    /** {@inheritDoc} */
    @Override public long getMajor(long pageAddr, int off) {
        return PageUtils.getLong(pageAddr, off);
    }

    /** {@inheritDoc} */
    @Override public void setMajor(long pageAddr, int off, long major) {
        PageUtils.putLong(pageAddr, off, major);
    }

    /** {@inheritDoc} */
    @Override public long getMinor(long pageAddr, int off) {
        return PageUtils.getLong(pageAddr, off + 8);
    }

    /** {@inheritDoc} */
    @Override public void setMinor(long pageAddr, int off, long minor) {
        PageUtils.putLong(pageAddr, off + 8, minor);
    }

    /** {@inheritDoc} */
    @Override public byte getState(long pageAddr, int off) {
        return PageUtils.getByte(pageAddr, off + 16);
    }

    /** {@inheritDoc} */
    @Override public void setState(long pageAddr, int off, byte state) {
        PageUtils.putByte(pageAddr, off + 16, state);
    }
}
