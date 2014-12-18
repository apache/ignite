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
public class GridClientHandshakeResponseWrapper extends GridTcpCommunicationMessageAdapter {
    /** */
    private static final long serialVersionUID = -1529807975073967381L;

    /** */
    private byte code;

    /**
     *
     */
    public GridClientHandshakeResponseWrapper() {
        // No-op.
    }

    /**
     * @param code Response code.
     */
    public GridClientHandshakeResponseWrapper(byte code) {
        this.code = code;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!commState.typeWritten) {
            if (!commState.putByte(null, directType()))
                return false;

            commState.typeWritten = true;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return code;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridClientHandshakeResponseWrapper _clone = new GridClientHandshakeResponseWrapper();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        GridClientHandshakeResponseWrapper _clone = (GridClientHandshakeResponseWrapper)_msg;

        _clone.code = code;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridClientHandshakeResponseWrapper.class, this);
    }
}
