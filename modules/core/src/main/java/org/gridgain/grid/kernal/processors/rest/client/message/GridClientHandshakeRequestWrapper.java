package org.gridgain.grid.kernal.processors.rest.client.message;

import org.gridgain.grid.util.direct.*;

import java.nio.*;

public class GridClientHandshakeRequestWrapper extends GridTcpCommunicationMessageAdapter {
    /** Signal char. */
    public static final byte SIGNAL_CHAR = (byte)0x91;

    private byte[] bytes;

    public GridClientHandshakeRequestWrapper() {
    }

    public GridClientHandshakeRequestWrapper(GridClientHandshakeRequest req) {
        bytes = new byte[5];

        byte[] raw = req.rawBytes();

        System.arraycopy(raw, 1, bytes, 0, bytes.length);
    }

    public byte protocol() {
        return bytes[4];
    }

    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!commState.typeWritten) {
            if (!commState.putByte(directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 0:
                if (!commState.putByteArrayClient(bytes))
                    return false;

                commState.idx++;

        }

        return true;
    }

    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        switch (commState.idx) {
            case 0:
                byte[] bytes0 = commState.getByteArrayClient(5);

                if (bytes0 == BYTE_ARR_NOT_READ)
                    return false;

                bytes = bytes0;

                commState.idx++;

        }

        return true;
    }

    @Override public byte directType() {
        return SIGNAL_CHAR;
    }

    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridClientHandshakeRequestWrapper _clone = new GridClientHandshakeRequestWrapper();

        clone0(_clone);

        return _clone;
    }

    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        GridClientHandshakeRequestWrapper _clone = (GridClientHandshakeRequestWrapper)_msg;

        _clone.bytes = bytes;
    }
}
