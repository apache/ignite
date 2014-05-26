package org.gridgain.grid.kernal.processors.rest.client.message;

import org.gridgain.grid.util.direct.*;

import java.nio.*;

public class GridClientHandshakeResponseWrapper extends GridTcpCommunicationMessageAdapter {
    byte code;

    public GridClientHandshakeResponseWrapper() {
    }

    public GridClientHandshakeResponseWrapper(byte code) {
        this.code = code;
    }

    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!commState.typeWritten) {
            if (!commState.putByte(directType()))
                return false;

            commState.typeWritten = true;
        }

        return true;
    }

    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        return true;
    }

    @Override public byte directType() {
        return code;
    }

    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridClientHandshakeResponseWrapper _clone = new GridClientHandshakeResponseWrapper();

        clone0(_clone);

        return _clone;
    }

    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        GridClientHandshakeResponseWrapper _clone = (GridClientHandshakeResponseWrapper)_msg;

        _clone.code = code;
    }
}
