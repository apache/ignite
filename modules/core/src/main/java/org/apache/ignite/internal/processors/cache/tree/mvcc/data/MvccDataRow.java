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

package org.apache.ignite.internal.processors.cache.tree.mvcc.data;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.mvcc.MvccUtils;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersion;
import org.apache.ignite.internal.processors.cache.mvcc.MvccVersionImpl;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxState;
import org.apache.ignite.internal.processors.cache.tree.DataRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_COUNTER_NA;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_CRD_COUNTER_NA;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_HINTS_BIT_OFF;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_KEY_ABSENT_BEFORE_MASK;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_OP_COUNTER_MASK;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.MVCC_OP_COUNTER_NA;
import static org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter.RowData.FULL_WITH_HINTS;
import static org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter.RowData.NO_KEY_WITH_HINTS;
import static org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO.MVCC_INFO_SIZE;

/**
 *
 */
public class MvccDataRow extends DataRow {
    /** Mvcc coordinator version. */
    @GridToStringInclude
    private long mvccCrd;

    /** Mvcc counter. */
    @GridToStringInclude
    private long mvccCntr;

    /** Mvcc operation counter. */
    @GridToStringInclude
    private int mvccOpCntr;

    /** Mvcc tx state. */
    @GridToStringInclude
    private byte mvccTxState;

    /** New mvcc coordinator version. */
    @GridToStringInclude
    private long newMvccCrd;

    /** New mvcc counter. */
    @GridToStringInclude
    private long newMvccCntr;

    /** New mvcc operation counter. */
    @GridToStringInclude
    private int newMvccOpCntr;

    /** New mvcc tx state. */
    @GridToStringInclude
    private byte newMvccTxState;

    /** Key absent before tx flag. */
    @GridToStringInclude
    private boolean keyAbsentFlag;

    /**
     * @param link Link.
     */
    public MvccDataRow(long link) {
        super(link);
    }

    /**
     * @param grp Context.
     * @param hash Key hash.
     * @param link Link.
     * @param part Partition number.
     * @param rowData Data.
     * @param crdVer Mvcc coordinator version.
     * @param mvccCntr Mvcc counter.
     * @param mvccOpCntr Mvcc operation counter.
     * @param skipVer Skip version flag.
     */
    public MvccDataRow(
        CacheGroupContext grp,
        int hash,
        long link,
        int part,
        RowData rowData,
        long crdVer,
        long mvccCntr,
        int mvccOpCntr,
        boolean skipVer
    ) {
        super(grp, hash, link, part, rowData, skipVer);

        assert MvccUtils.mvccVersionIsValid(crdVer, mvccCntr, mvccOpCntr);

        assert rowData == RowData.LINK_ONLY
            || mvccCoordinatorVersion() == crdVer && mvccCounter() == mvccCntr && mvccOperationCounter() == mvccOpCntr :
        "mvccVer=" + new MvccVersionImpl(crdVer, mvccCntr, mvccOpCntr) +
            ", dataMvccVer=" + new MvccVersionImpl(mvccCoordinatorVersion(), mvccCounter(), mvccOperationCounter());

        if (rowData == RowData.LINK_ONLY) {
            this.mvccCrd = crdVer;
            this.mvccCntr = mvccCntr;
            this.mvccOpCntr = mvccOpCntr;
        }

        assert (mvccOpCntr & ~MVCC_OP_COUNTER_MASK) == 0 : mvccOpCntr;
    }

    /**
     * 
     */
    public MvccDataRow() {
        super(0);
    }

    /**
     * @param key Key.
     * @param val Value.
     * @param ver Version.
     * @param part Partition.
     * @param expireTime Expire time.
     * @param cacheId Cache ID.
     * @param mvccVer Mvcc version.
     * @param newMvccVer New mvcc version.
     */
    public MvccDataRow(KeyCacheObject key, CacheObject val, GridCacheVersion ver, int part, long expireTime, int cacheId,
        MvccVersion mvccVer, MvccVersion newMvccVer) {
        super(key, val, ver, part, expireTime, cacheId);

        this.mvccCrd = mvccVer.coordinatorVersion();
        this.mvccCntr = mvccVer.counter();
        this.mvccOpCntr = mvccVer.operationCounter();

        assert (mvccOpCntr & ~MVCC_OP_COUNTER_MASK) == 0 : mvccOpCntr;

        if (newMvccVer == null) {
            newMvccCrd = MVCC_CRD_COUNTER_NA;
            newMvccCntr = MVCC_COUNTER_NA;
            newMvccOpCntr = MVCC_OP_COUNTER_NA;
        }
        else {
            newMvccCrd = newMvccVer.coordinatorVersion();
            newMvccCntr = newMvccVer.counter();
            newMvccOpCntr = newMvccVer.operationCounter();

            assert (newMvccOpCntr & ~MVCC_OP_COUNTER_MASK) == 0 : newMvccOpCntr;
        }
    }

    /** {@inheritDoc} */
    @Override protected int readHeader(GridCacheSharedContext<?, ?> sharedCtx, long addr, int off, RowData rowData) {
        boolean addHints = rowData == FULL_WITH_HINTS || rowData == NO_KEY_WITH_HINTS;

        // xid_min.
        mvccCrd = PageUtils.getLong(addr, off);
        mvccCntr = PageUtils.getLong(addr, off + 8);

        int withHint = PageUtils.getInt(addr, off + 16);

        mvccOpCntr = withHint & MVCC_OP_COUNTER_MASK;
        mvccTxState = (byte)(withHint >>> MVCC_HINTS_BIT_OFF);

        if (addHints && mvccTxState == TxState.NA)
            mvccTxState = MvccUtils.state(sharedCtx.coordinators(), mvccCrd, mvccCntr, mvccOpCntr);

        assert MvccUtils.mvccVersionIsValid(mvccCrd, mvccCntr, mvccOpCntr);

        keyAbsentFlag = (withHint & MVCC_KEY_ABSENT_BEFORE_MASK) != 0;

        // xid_max.
        newMvccCrd = PageUtils.getLong(addr, off + 20);
        newMvccCntr = PageUtils.getLong(addr, off + 28);

        withHint = PageUtils.getInt(addr, off + 36);

        newMvccOpCntr = withHint & MVCC_OP_COUNTER_MASK;
        newMvccTxState = (byte)(withHint >>> MVCC_HINTS_BIT_OFF);

        assert newMvccCrd == MVCC_CRD_COUNTER_NA || MvccUtils.mvccVersionIsValid(newMvccCrd, newMvccCntr, newMvccOpCntr);

        if (newMvccCrd != MVCC_CRD_COUNTER_NA) {
            keyAbsentFlag = (withHint & MVCC_KEY_ABSENT_BEFORE_MASK) != 0;

            if (addHints && newMvccTxState == TxState.NA)
                newMvccTxState = MvccUtils.state(sharedCtx.coordinators(), newMvccCrd, newMvccCntr, newMvccOpCntr);
        }

        return MVCC_INFO_SIZE;
    }

    /** {@inheritDoc} */
    @Override public long mvccCoordinatorVersion() {
        return mvccCrd;
    }

    /** {@inheritDoc} */
    @Override public long mvccCounter() {
        return mvccCntr;
    }

    /** {@inheritDoc} */
    @Override public int mvccOperationCounter() {
        assert (mvccCntr & ~MVCC_OP_COUNTER_MASK) == 0;

        return mvccOpCntr;
    }

    /** {@inheritDoc} */
    @Override public byte mvccTxState() {
        return mvccTxState;
    }

    /** {@inheritDoc} */
    @Override public long newMvccCoordinatorVersion() {
        return newMvccCrd;
    }

    /** {@inheritDoc} */
    @Override public long newMvccCounter() {
        return newMvccCntr;
    }

    /** {@inheritDoc} */
    @Override public int newMvccOperationCounter() {
        assert (newMvccOpCntr & ~MVCC_OP_COUNTER_MASK) == 0;

        return newMvccOpCntr;
    }

    /** {@inheritDoc} */
    @Override public byte newMvccTxState() {
        return newMvccTxState;
    }

    /** {@inheritDoc} */
    @Override public void newMvccVersion(long crd, long cntr, int opCntr) {
        assert (opCntr & ~MVCC_OP_COUNTER_MASK) == 0 : opCntr;

        newMvccCrd = crd;
        newMvccCntr = cntr;
        newMvccOpCntr = opCntr;

        // reset tx state
        newMvccTxState = TxState.NA;
    }

    /** {@inheritDoc} */
    @Override public void mvccVersion(long crd, long cntr, int opCntr) {
        assert (opCntr & ~MVCC_OP_COUNTER_MASK) == 0 : opCntr;

        mvccCrd = crd;
        mvccCntr = cntr;
        mvccOpCntr = opCntr;

        // reset tx state
        mvccTxState = TxState.NA;
    }

    /**
     * @param mvccTxState Mvcc version Tx state hint.
     */
    public void mvccTxState(byte mvccTxState) {
        this.mvccTxState = mvccTxState;
    }

    /**
     * @param newMvccTxState New Mvcc version Tx state hint.
     */
    public void newMvccTxState(byte newMvccTxState) {
        this.newMvccTxState = newMvccTxState;
    }

    /**
     * @return {@code True} if key absent before.
     */
    protected boolean keyAbsentBeforeFlag() {
        return keyAbsentFlag;
    }

    /**
     * @param flag {@code True} if key is absent before.
     */
    protected void keyAbsentBeforeFlag(boolean flag) {
        keyAbsentFlag = flag;
    }

    /** {@inheritDoc} */
    @Override public int size() throws IgniteCheckedException {
        return super.size() + MVCC_INFO_SIZE;
    }

    /** {@inheritDoc} */
    @Override public int headerSize() {
        return MVCC_INFO_SIZE;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MvccDataRow.class, this, "super", super.toString());
    }
}
