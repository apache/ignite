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

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Message sent to check that transactions related to some pessimistic transaction
 * were prepared on remote node.
 */
public class GridCachePessimisticCheckCommittedTxRequest<K, V> extends GridDistributedBaseMessage<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Future ID. */
    private GridUuid futId;

    /** Mini future ID. */
    private GridUuid miniId;

    /** Near transaction ID. */
    private GridCacheVersion nearXidVer;

    /** Originating node ID. */
    private UUID originatingNodeId;

    /** Originating thread ID. */
    private long originatingThreadId;

    /** Flag indicating that this is near-only check. */
    @GridDirectVersion(1)
    private boolean nearOnlyCheck;

    /**
     * Empty constructor required by {@link Externalizable}
     */
    public GridCachePessimisticCheckCommittedTxRequest() {
        // No-op.
    }

    /**
     * @param tx Transaction.
     * @param originatingThreadId Originating thread ID.
     * @param futId Future ID.
     * @param miniId Mini future ID.
     */
    public GridCachePessimisticCheckCommittedTxRequest(GridCacheTxEx<K, V> tx, long originatingThreadId, GridUuid futId,
        GridUuid miniId, boolean nearOnlyCheck) {
        super(tx.xidVersion(), 0);

        this.futId = futId;
        this.miniId = miniId;
        this.nearOnlyCheck = nearOnlyCheck;

        nearXidVer = tx.nearXidVersion();
        originatingNodeId = tx.eventNodeId();
        this.originatingThreadId = originatingThreadId;
    }

    /**
     * @return Near version.
     */
    public GridCacheVersion nearXidVersion() {
        return nearXidVer;
    }

    /**
     * @return Tx originating node ID.
     */
    public UUID originatingNodeId() {
        return originatingNodeId;
    }

    /**
     * @return Tx originating thread ID.
     */
    public long originatingThreadId() {
        return originatingThreadId;
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
     * @return Flag indicating that this request was sent only to near node. If this flag is set, no finalizing
     *      will be executed on receiving (near) node since this is a user node.
     */
    public boolean nearOnlyCheck() {
        return nearOnlyCheck;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridCachePessimisticCheckCommittedTxRequest _clone = new GridCachePessimisticCheckCommittedTxRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridCachePessimisticCheckCommittedTxRequest _clone = (GridCachePessimisticCheckCommittedTxRequest)_msg;

        _clone.futId = futId;
        _clone.miniId = miniId;
        _clone.nearXidVer = nearXidVer;
        _clone.originatingNodeId = originatingNodeId;
        _clone.originatingThreadId = originatingThreadId;
        _clone.nearOnlyCheck = nearOnlyCheck;
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
                if (!commState.putGridUuid(futId))
                    return false;

                commState.idx++;

            case 8:
                if (!commState.putGridUuid(miniId))
                    return false;

                commState.idx++;

            case 9:
                if (!commState.putCacheVersion(nearXidVer))
                    return false;

                commState.idx++;

            case 10:
                if (!commState.putUuid(originatingNodeId))
                    return false;

                commState.idx++;

            case 11:
                if (!commState.putLong(originatingThreadId))
                    return false;

                commState.idx++;

            case 12:
                if (!commState.putBoolean(nearOnlyCheck))
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
                GridUuid futId0 = commState.getGridUuid();

                if (futId0 == GRID_UUID_NOT_READ)
                    return false;

                futId = futId0;

                commState.idx++;

            case 8:
                GridUuid miniId0 = commState.getGridUuid();

                if (miniId0 == GRID_UUID_NOT_READ)
                    return false;

                miniId = miniId0;

                commState.idx++;

            case 9:
                GridCacheVersion nearXidVer0 = commState.getCacheVersion();

                if (nearXidVer0 == CACHE_VER_NOT_READ)
                    return false;

                nearXidVer = nearXidVer0;

                commState.idx++;

            case 10:
                UUID originatingNodeId0 = commState.getUuid();

                if (originatingNodeId0 == UUID_NOT_READ)
                    return false;

                originatingNodeId = originatingNodeId0;

                commState.idx++;

            case 11:
                if (buf.remaining() < 8)
                    return false;

                originatingThreadId = commState.getLong();

                commState.idx++;

            case 12:
                if (buf.remaining() < 1)
                    return false;

                nearOnlyCheck = commState.getBoolean();

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 20;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCachePessimisticCheckCommittedTxRequest.class, this, "super", super.toString());
    }
}
