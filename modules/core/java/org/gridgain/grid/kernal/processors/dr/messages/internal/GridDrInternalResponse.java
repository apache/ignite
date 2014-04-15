/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.dr.messages.internal;

import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;

/**
 * Internal replication response.
 */
public class GridDrInternalResponse extends GridTcpCommunicationMessageAdapter {
    private static final long serialVersionUID = -4148809261176855900L;
    /** Request id. */
    private long id;

    /** Optional error. */
    @GridDirectTransient
    private Throwable err;

    /** Serialized error. */
    private byte[] errBytes;

    /**
     * {@link Externalizable} support.
     */
    public GridDrInternalResponse() {
        // No-op.
    }

    /**
     * @param id Request ID.
     * @param err Error.
     * @param errBytes Serialized error.
     */
    public GridDrInternalResponse(long id, @Nullable Throwable err, @Nullable byte[] errBytes) {
        assert id > 0;

        this.id = id;
        this.err = err;
        this.errBytes = errBytes;
    }

    /**
     * @return Sequence.
     */
    public long id() {
        return id;
    }

    /**
     * @return Error.
     */
    public Throwable error() {
        return err;
    }

    /**
     * @return Serialized error.
     */
    public byte[] errorBytes() {
        return errBytes;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridDrInternalResponse _clone = new GridDrInternalResponse();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        GridDrInternalResponse _clone = (GridDrInternalResponse)_msg;

        _clone.id = id;
        _clone.err = err;
        _clone.errBytes = errBytes;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!commState.typeWritten) {
            if (!commState.putByte(directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 0:
                if (!commState.putByteArray(errBytes))
                    return false;

                commState.idx++;

            case 1:
                if (!commState.putLong(id))
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
                byte[] errBytes0 = commState.getByteArray();

                if (errBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                errBytes = errBytes0;

                commState.idx++;

            case 1:
                if (buf.remaining() < 8)
                    return false;

                id = commState.getLong();

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 64;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDrInternalResponse.class, this);
    }
}
