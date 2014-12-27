/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.client.message;

import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.nio.*;

/**
 * Client handshake wrapper for direct marshalling.
 */
public class GridClientHandshakeRequestWrapper extends GridTcpCommunicationMessageAdapter {
    /** */
    private static final long serialVersionUID = -5705048094821942662L;

    /** Signal char. */
    public static final byte HANDSHAKE_HEADER = (byte)0x91;

    /** Handshake bytes. */
    private byte[] bytes;

    /**
     *
     */
    public GridClientHandshakeRequestWrapper() {
        // No-op.
    }

    /**
     *
     * @param req Handshake request.
     */
    public GridClientHandshakeRequestWrapper(GridClientHandshakeRequest req) {
        bytes = req.rawBytes();
    }

    /**
     * @return Handshake bytes.
     */
    public byte[] bytes() {
        return bytes;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!commState.typeWritten) {
            if (!commState.putByte(null, directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 0:
                if (!commState.putByteArray("bytes", bytes))
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        switch (commState.idx) {
            case 0:
                bytes = commState.getByteArray("bytes");

                if (!commState.lastRead())
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return HANDSHAKE_HEADER;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridClientHandshakeRequestWrapper _clone = new GridClientHandshakeRequestWrapper();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        GridClientHandshakeRequestWrapper _clone = (GridClientHandshakeRequestWrapper)_msg;

        _clone.bytes = bytes;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridClientHandshakeRequestWrapper.class, this);
    }
}
