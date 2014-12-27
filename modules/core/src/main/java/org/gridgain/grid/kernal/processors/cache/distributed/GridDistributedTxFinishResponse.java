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
import org.gridgain.grid.util.tostring.*;

import java.io.*;
import java.nio.*;

/**
 * Transaction finish response.
 */
public class GridDistributedTxFinishResponse<K, V> extends GridCacheMessage<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private GridCacheVersion txId;

    /** Future ID. */
    private IgniteUuid futId;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridDistributedTxFinishResponse() {
        /* No-op. */
    }

    /**
     * @param txId Transaction id.
     * @param futId Future ID.
     */
    public GridDistributedTxFinishResponse(GridCacheVersion txId, IgniteUuid futId) {
        assert txId != null;
        assert futId != null;

        this.txId = txId;
        this.futId = futId;
    }

    /**
     *
     * @return Transaction id.
     */
    public GridCacheVersion xid() {
        return txId;
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors",
        "OverriddenMethodCallDuringObjectConstruction"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridDistributedTxFinishResponse _clone = new GridDistributedTxFinishResponse();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridDistributedTxFinishResponse _clone = (GridDistributedTxFinishResponse)_msg;

        _clone.txId = txId;
        _clone.futId = futId;
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
            case 3:
                if (!commState.putGridUuid("futId", futId))
                    return false;

                commState.idx++;

            case 4:
                if (!commState.putCacheVersion("txId", txId))
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
            case 3:
                futId = commState.getGridUuid("futId");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

            case 4:
                txId = commState.getCacheVersion("txId");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 25;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridDistributedTxFinishResponse.class, this);
    }
}
