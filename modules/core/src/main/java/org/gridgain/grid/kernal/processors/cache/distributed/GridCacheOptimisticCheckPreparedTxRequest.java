/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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
            if (!commState.putByte(null, directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 8:
                if (!commState.putGridUuid("futId", futId))
                    return false;

                commState.idx++;

            case 9:
                if (!commState.putGridUuid("miniId", miniId))
                    return false;

                commState.idx++;

            case 10:
                if (!commState.putCacheVersion("nearXidVer", nearXidVer))
                    return false;

                commState.idx++;

            case 11:
                if (!commState.putInt("txNum", txNum))
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
                futId = commState.getGridUuid("futId");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 9:
                miniId = commState.getGridUuid("miniId");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 10:
                nearXidVer = commState.getCacheVersion("nearXidVer");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 11:
                txNum = commState.getInt("txNum");

                if (!commState.lastRead())
                    return false;

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
