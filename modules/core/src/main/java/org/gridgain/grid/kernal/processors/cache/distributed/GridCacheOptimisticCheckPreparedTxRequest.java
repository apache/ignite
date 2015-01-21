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

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.apache.ignite.lang.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.transactions.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.nio.*;

/**
 * Message sent to check that transactions related to some optimistic transaction
 * were prepared on remote node.
 */
public class GridCacheOptimisticCheckPreparedTxRequest<K, V> extends GridDistributedBaseMessage<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Future ID. */
    private IgniteUuid futId;

    /** Mini future ID. */
    private IgniteUuid miniId;

    /** Near transaction ID. */
    private GridCacheVersion nearXidVer;

    /** Expected number of transactions on node. */
    private int txNum;

    /**
     * Empty constructor required by {@link Externalizable}
     */
    public GridCacheOptimisticCheckPreparedTxRequest() {
        // No-op.
    }

    /**
     * @param tx Transaction.
     * @param txNum Expected number of transactions on remote node.
     * @param futId Future ID.
     * @param miniId Mini future ID.
     */
    public GridCacheOptimisticCheckPreparedTxRequest(IgniteTxEx<K, V> tx, int txNum, IgniteUuid futId, IgniteUuid miniId) {
        super(tx.xidVersion(), 0);

        nearXidVer = tx.nearXidVersion();
        this.futId = futId;
        this.miniId = miniId;
        this.txNum = txNum;
    }

    /**
     * @return Near version.
     */
    public GridCacheVersion nearXidVersion() {
        return nearXidVer;
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @return Mini future ID.
     */
    public IgniteUuid miniId() {
        return miniId;
    }

    /**
     * @return Expected number of transactions on node.
     */
    public int transactions() {
        return txNum;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridCacheOptimisticCheckPreparedTxRequest _clone = new GridCacheOptimisticCheckPreparedTxRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridCacheOptimisticCheckPreparedTxRequest _clone = (GridCacheOptimisticCheckPreparedTxRequest)_msg;

        _clone.futId = futId;
        _clone.miniId = miniId;
        _clone.nearXidVer = nearXidVer;
        _clone.txNum = txNum;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.writeTo(buf))
            return false;

        if (!commState.typeWritten) {
            if (!commState.putByte(directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 8:
                if (!commState.putGridUuid(futId))
                    return false;

                commState.idx++;

            case 9:
                if (!commState.putGridUuid(miniId))
                    return false;

                commState.idx++;

            case 10:
                if (!commState.putCacheVersion(nearXidVer))
                    return false;

                commState.idx++;

            case 11:
                if (!commState.putInt(txNum))
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (commState.idx) {
            case 8:
                IgniteUuid futId0 = commState.getGridUuid();

                if (futId0 == GRID_UUID_NOT_READ)
                    return false;

                futId = futId0;

                commState.idx++;

            case 9:
                IgniteUuid miniId0 = commState.getGridUuid();

                if (miniId0 == GRID_UUID_NOT_READ)
                    return false;

                miniId = miniId0;

                commState.idx++;

            case 10:
                GridCacheVersion nearXidVer0 = commState.getCacheVersion();

                if (nearXidVer0 == CACHE_VER_NOT_READ)
                    return false;

                nearXidVer = nearXidVer0;

                commState.idx++;

            case 11:
                if (buf.remaining() < 4)
                    return false;

                txNum = commState.getInt();

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 18;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheOptimisticCheckPreparedTxRequest.class, this, "super", super.toString());
    }
}
