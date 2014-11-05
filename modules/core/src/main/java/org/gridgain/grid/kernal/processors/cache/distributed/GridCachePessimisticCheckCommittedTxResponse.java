/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;

/**
 * Check prepared transactions response.
 */
public class GridCachePessimisticCheckCommittedTxResponse<K, V> extends GridDistributedBaseMessage<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Future ID. */
    private GridUuid futId;

    /** Mini future ID. */
    private GridUuid miniId;

    /** Committed transaction info. */
    @GridDirectTransient
    private GridCacheCommittedTxInfo<K, V> committedTxInfo;

    /** Serialized transaction info. */
    private byte[] committedTxInfoBytes;

    /**
     * Empty constructor required by {@link Externalizable}
     */
    public GridCachePessimisticCheckCommittedTxResponse() {
        // No-op.
    }

    /**
     * @param txId Transaction ID.
     * @param futId Future ID.
     * @param miniId Mini future ID.
     * @param committedTxInfo Committed transaction info.
     */
    public GridCachePessimisticCheckCommittedTxResponse(GridCacheVersion txId, GridUuid futId, GridUuid miniId,
        @Nullable GridCacheCommittedTxInfo<K, V> committedTxInfo) {
        super(txId, 0);

        this.futId = futId;
        this.miniId = miniId;
        this.committedTxInfo = committedTxInfo;
    }

    /**
     * @return Future ID.
     */
    public GridUuid futureId() {
        return futId;
    }

    /**
     * @return Mini future ID.
     */
    public GridUuid miniId() {
        return miniId;
    }

    /**
     * @return {@code True} if all remote transactions were prepared.
     */
    public GridCacheCommittedTxInfo<K, V> committedTxInfo() {
        return committedTxInfo;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext<K, V> ctx) throws GridException {
        super.prepareMarshal(ctx);

        if (committedTxInfo != null) {
            marshalTx(committedTxInfo.recoveryWrites(), ctx);

            committedTxInfoBytes = ctx.marshaller().marshal(committedTxInfo);
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<K, V> ctx, ClassLoader ldr) throws GridException {
        super.finishUnmarshal(ctx, ldr);

        if (committedTxInfoBytes != null) {
            committedTxInfo = ctx.marshaller().unmarshal(committedTxInfoBytes, ldr);

            unmarshalTx(committedTxInfo.recoveryWrites(), ctx, ldr);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridCachePessimisticCheckCommittedTxResponse _clone = new GridCachePessimisticCheckCommittedTxResponse();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridCachePessimisticCheckCommittedTxResponse _clone = (GridCachePessimisticCheckCommittedTxResponse)_msg;

        _clone.futId = futId;
        _clone.miniId = miniId;
        _clone.committedTxInfo = committedTxInfo;
        _clone.committedTxInfoBytes = committedTxInfoBytes;
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
            case 7:
                if (!commState.putByteArray(committedTxInfoBytes))
                    return false;

                commState.idx++;

            case 8:
                if (!commState.putGridUuid(futId))
                    return false;

                commState.idx++;

            case 9:
                if (!commState.putGridUuid(miniId))
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
            case 7:
                byte[] committedTxInfoBytes0 = commState.getByteArray();

                if (committedTxInfoBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                committedTxInfoBytes = committedTxInfoBytes0;

                commState.idx++;

            case 8:
                GridUuid futId0 = commState.getGridUuid();

                if (futId0 == GRID_UUID_NOT_READ)
                    return false;

                futId = futId0;

                commState.idx++;

            case 9:
                GridUuid miniId0 = commState.getGridUuid();

                if (miniId0 == GRID_UUID_NOT_READ)
                    return false;

                miniId = miniId0;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 21;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCachePessimisticCheckCommittedTxResponse.class, this, "super", super.toString());
    }
}
