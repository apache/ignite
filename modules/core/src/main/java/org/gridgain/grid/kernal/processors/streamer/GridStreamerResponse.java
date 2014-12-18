/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.streamer;

import org.apache.ignite.lang.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.nio.*;

/**
 *
 */
public class GridStreamerResponse extends GridTcpCommunicationMessageAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private IgniteUuid futId;

    /** */
    private byte[] errBytes;

    /**
     *
     */
    public GridStreamerResponse() {
        // No-op.
    }

    /**
     * @param futId Future ID.
     * @param errBytes Serialized error, if any.
     */
    public GridStreamerResponse(IgniteUuid futId, @Nullable byte[] errBytes) {
        assert futId != null;

        this.futId = futId;
        this.errBytes = errBytes;
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @return Serialized error.
     */
    public byte[] errorBytes() {
        return errBytes;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridStreamerResponse.class, this);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridStreamerResponse _clone = new GridStreamerResponse();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        GridStreamerResponse _clone = (GridStreamerResponse)_msg;

        _clone.futId = futId;
        _clone.errBytes = errBytes;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!commState.typeWritten) {
            if (!commState.putByte(null, directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 0:
                if (!commState.putByteArray(null, errBytes))
                    return false;

                commState.idx++;

            case 1:
                if (!commState.putGridUuid(null, futId))
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        switch (commState.idx) {
            case 0:
                byte[] errBytes0 = commState.getByteArray(null);

                if (errBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                errBytes = errBytes0;

                commState.idx++;

            case 1:
                IgniteUuid futId0 = commState.getGridUuid(null);

                if (futId0 == GRID_UUID_NOT_READ)
                    return false;

                futId = futId0;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 77;
    }
}
