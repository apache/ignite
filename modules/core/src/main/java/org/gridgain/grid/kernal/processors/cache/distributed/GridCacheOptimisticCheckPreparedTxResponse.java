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
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.nio.*;

/**
 * Check prepared transactions response.
 */
public class GridCacheOptimisticCheckPreparedTxResponse<K, V> extends GridDistributedBaseMessage<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Future ID. */
    private IgniteUuid futId;

    /** Mini future ID. */
    private IgniteUuid miniId;

    /** Flag indicating if all remote transactions were prepared. */
    private boolean success;

    /**
     * Empty constructor required by {@link Externalizable}
     */
    public GridCacheOptimisticCheckPreparedTxResponse() {
        // No-op.
    }

    /**
     * @param txId Transaction ID.
     * @param futId Future ID.
     * @param miniId Mini future ID.
     * @param success {@code True} if all remote transactions were prepared, {@code false} otherwise.
     */
    public GridCacheOptimisticCheckPreparedTxResponse(GridCacheVersion txId, IgniteUuid futId, IgniteUuid miniId,
        boolean success) {
        super(txId, 0);

        this.futId = futId;
        this.miniId = miniId;
        this.success = success;
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
     * @return {@code True} if all remote transactions were prepared.
     */
    public boolean success() {
        return success;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridCacheOptimisticCheckPreparedTxResponse _clone = new GridCacheOptimisticCheckPreparedTxResponse();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridCacheOptimisticCheckPreparedTxResponse _clone = (GridCacheOptimisticCheckPreparedTxResponse)_msg;

        _clone.futId = futId;
        _clone.miniId = miniId;
        _clone.success = success;
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
                if (!commState.putBoolean("success", success))
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
                success = commState.getBoolean("success");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 19;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheOptimisticCheckPreparedTxResponse.class, this, "super", super.toString());
    }
}
