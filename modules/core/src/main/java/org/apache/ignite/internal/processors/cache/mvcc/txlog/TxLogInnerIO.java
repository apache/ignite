/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.mvcc.txlog;

import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;

/** */
public class TxLogInnerIO extends BPlusInnerIO<TxKey> implements TxLogIO {
    /** */
    public static final IOVersions<TxLogInnerIO> VERSIONS = new IOVersions<>(new TxLogInnerIO(1));

    /**
     * @param ver Page format version.
     */
    protected TxLogInnerIO(int ver) {
        super(T_TX_LOG_INNER, ver, true, 8 + 8 + 1);
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
        int cmp = Long.compare(PageUtils.getLong(pageAddr, off), row.major());

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
