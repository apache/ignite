/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.continuous;

import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Continuous processor message.
 */
public class GridContinuousMessage extends GridTcpCommunicationMessageAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** Message type. */
    private GridContinuousMessageType type;

    /** Routine ID. */
    private UUID routineId;

    /** Optional message data. */
    @GridToStringInclude
    @GridDirectTransient
    private Object data;

    /** Serialized message data. */
    private byte[] dataBytes;

    /**
     * Required by {@link Externalizable}.
     */
    public GridContinuousMessage() {
        // No-op.
    }

    /**
     * @param type Message type.
     * @param routineId Consume ID.
     * @param data Optional message data.
     */
    GridContinuousMessage(GridContinuousMessageType type, UUID routineId, @Nullable Object data) {
        assert type != null;
        assert routineId != null;

        this.type = type;
        this.routineId = routineId;
        this.data = data;
    }

    /**
     * @return Message type.
     */
    public GridContinuousMessageType type() {
        return type;
    }

    /**
     * @return Consume ID.
     */
    public UUID routineId() {
        return routineId;
    }

    /**
     * @return Message data.
     */
    public <T> T data() {
        return (T)data;
    }

    /**
     * @param data Message data.
     */
    public void data(Object data) {
        this.data = data;
    }

    /**
     * @return Serialized message data.
     */
    public byte[] dataBytes() {
        return dataBytes;
    }

    /**
     * @param dataBytes Serialized message data.
     */
    public void dataBytes(byte[] dataBytes) {
        this.dataBytes = dataBytes;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridContinuousMessage _clone = new GridContinuousMessage();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter msg) {
        GridContinuousMessage clone = (GridContinuousMessage)msg;

        clone.type = type;
        clone.routineId = routineId;
        clone.data = data;
        clone.dataBytes = dataBytes;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("fallthrough")
    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!commState.typeWritten) {
            if (!commState.putByte(null, directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 0:
                if (!commState.putByteArray("dataBytes", dataBytes))
                    return false;

                commState.idx++;

            case 1:
                if (!commState.putUuid("routineId", routineId))
                    return false;

                commState.idx++;

            case 2:
                if (!commState.putEnum("type", type))
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("fallthrough")
    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        switch (commState.idx) {
            case 0:
                byte[] dataBytes0 = commState.getByteArray("dataBytes");

                if (dataBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                dataBytes = dataBytes0;

                commState.idx++;

            case 1:
                UUID routineId0 = commState.getUuid("routineId");

                if (routineId0 == UUID_NOT_READ)
                    return false;

                routineId = routineId0;

                commState.idx++;

            case 2:
                if (buf.remaining() < 1)
                    return false;

                byte type0 = commState.getByte("type");

                type = GridContinuousMessageType.fromOrdinal(type0);

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 60;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridContinuousMessage.class, this);
    }
}
