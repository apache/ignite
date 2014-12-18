/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.dataload;

import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.nio.*;

/**
 *
 */
public class GridDataLoadResponse extends GridTcpCommunicationMessageAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private long reqId;

    /** */
    private byte[] errBytes;

    /** */
    private boolean forceLocDep;

    /**
     * @param reqId Request ID.
     * @param errBytes Error bytes.
     * @param forceLocDep Force local deployment.
     */
    public GridDataLoadResponse(long reqId, byte[] errBytes, boolean forceLocDep) {
        this.reqId = reqId;
        this.errBytes = errBytes;
        this.forceLocDep = forceLocDep;
    }

    /**
     * {@code Externalizable} support.
     */
    public GridDataLoadResponse() {
        // No-op.
    }

    /**
     * @return Request ID.
     */
    public long requestId() {
        return reqId;
    }

    /**
     * @return Error bytes.
     */
    public byte[] errorBytes() {
        return errBytes;
    }

    /**
     * @return {@code True} to force local deployment.
     */
    public boolean forceLocalDeployment() {
        return forceLocDep;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDataLoadResponse.class, this);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridDataLoadResponse _clone = new GridDataLoadResponse();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        GridDataLoadResponse _clone = (GridDataLoadResponse)_msg;

        _clone.reqId = reqId;
        _clone.errBytes = errBytes;
        _clone.forceLocDep = forceLocDep;
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
                if (!commState.putBoolean(null, forceLocDep))
                    return false;

                commState.idx++;

            case 2:
                if (!commState.putLong(null, reqId))
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
                if (buf.remaining() < 1)
                    return false;

                forceLocDep = commState.getBoolean(null);

                commState.idx++;

            case 2:
                if (buf.remaining() < 8)
                    return false;

                reqId = commState.getLong(null);

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 62;
    }
}
