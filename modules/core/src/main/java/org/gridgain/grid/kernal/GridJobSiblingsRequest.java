/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.lang.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.nio.*;

/**
 * Job siblings request.
 */
public class GridJobSiblingsRequest extends GridTcpCommunicationMessageAdapter {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private IgniteUuid sesId;

    /** */
    @GridDirectTransient
    private Object topic;

    /** */
    private byte[] topicBytes;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridJobSiblingsRequest() {
        // No-op.
    }

    /**
     * @param sesId Session ID.
     * @param topic Topic.
     * @param topicBytes Serialized topic.
     */
    public GridJobSiblingsRequest(IgniteUuid sesId, Object topic, byte[] topicBytes) {
        assert sesId != null;
        assert topic != null || topicBytes != null;

        this.sesId = sesId;
        this.topic = topic;
        this.topicBytes = topicBytes;
    }

    /**
     * @return Session ID.
     */
    public IgniteUuid sessionId() {
        return sesId;
    }

    /**
     * @return Topic.
     */
    public Object topic() {
        return topic;
    }

    /**
     * @return Serialized topic.
     */
    public byte[] topicBytes() {
        return topicBytes;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridJobSiblingsRequest _clone = new GridJobSiblingsRequest();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        GridJobSiblingsRequest _clone = (GridJobSiblingsRequest)_msg;

        _clone.sesId = sesId;
        _clone.topic = topic;
        _clone.topicBytes = topicBytes;
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
                if (!commState.putGridUuid(null, sesId))
                    return false;

                commState.idx++;

            case 1:
                if (!commState.putByteArray(null, topicBytes))
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
                IgniteUuid sesId0 = commState.getGridUuid(null);

                if (sesId0 == GRID_UUID_NOT_READ)
                    return false;

                sesId = sesId0;

                commState.idx++;

            case 1:
                byte[] topicBytes0 = commState.getByteArray(null);

                if (topicBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                topicBytes = topicBytes0;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridJobSiblingsRequest.class, this);
    }
}
