package org.gridgain.grid.kernal.processors.rest.client.message;

import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.direct.*;

import java.nio.*;
import java.util.*;

/**
 */
public class GridClientMessageWrapper extends GridTcpCommunicationMessageAdapter {
    /** Request header. */
    public static final byte REQ_HEADER = (byte)0x90;

    @GridDirectVersion(1)
    private int msgSize;

    @GridDirectVersion(2)
    private long reqId;

    @GridDirectVersion(3)
    private UUID clientId;

    @GridDirectVersion(4)
    private UUID destId;

    @GridDirectVersion(5)
    private byte[] msg;

    @GridDirectTransient
    private GridClientMessage clientMsg;

    public long getReqId() {
        return reqId;
    }

    public void setReqId(long reqId) {
        this.reqId = reqId;
    }

    public int getMsgSize() {
        return msgSize;
    }

    public void setMsgSize(int msgSize) {
        this.msgSize = msgSize;
    }

    public UUID getClientId() {
        return clientId;
    }

    public void setClientId(UUID clientId) {
        this.clientId = clientId;
    }

    public UUID getDestId() {
        return destId;
    }

    public void setDestId(UUID destId) {
        this.destId = destId;
    }

    public byte[] getMsg() {
        return msg;
    }

    public void setMsg(byte[] msg) {
        this.msg = msg;
    }

    public GridClientMessage getClientMsg() {
        return clientMsg;
    }

    public void setClientMsg(GridClientMessage clientMsg) {
        this.clientMsg = clientMsg;
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
                if (!commState.putIntClient(msgSize))
                    return false;

                commState.idx++;

            case 1:
                if (!commState.putLongClient(reqId))
                    return false;

                commState.idx++;

            case 2:
                if (!commState.putUuidClient(clientId))
                    return false;

                commState.idx++;

            case 3:
                if (!commState.putUuidClient(destId))
                    return false;

                commState.idx++;

            case 4:
                if (!commState.putByteArrayClient(msg))
                    return false;

                commState.idx++;

        }

        return true;
    }

    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        switch (commState.idx) {
            case 0:
                if (buf.remaining() < 4)
                    return false;

                msgSize = commState.getIntClient();

                commState.idx++;

            case 1:
                if (buf.remaining() < 8)
                    return false;

                reqId = commState.getLongClient();

                commState.idx++;

            case 2:
                UUID clientId0 = commState.getUuidClient();

                if (clientId0 == UUID_NOT_READ)
                    return false;

                clientId = clientId0;

                commState.idx++;

            case 3:
                UUID destId0 = commState.getUuidClient();

                if (destId0 == UUID_NOT_READ)
                    return false;

                destId = destId0;

                commState.idx++;

            case 4:
                byte[] msg0 = commState.getByteArrayClient(msgSize - 40);

                if (msg0 == BYTE_ARR_NOT_READ)
                    return false;

                msg = msg0;

                commState.idx++;
        }

        return true;
    }

    @Override public byte directType() {
        return REQ_HEADER;
    }

    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridClientMessageWrapper _clone = new GridClientMessageWrapper();

        clone0(_clone);

        return _clone;
    }

    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        GridClientMessageWrapper _clone = (GridClientMessageWrapper)_msg;

        _clone.reqId = reqId;
        _clone.msgSize = msgSize;
        _clone.clientId = clientId;
        _clone.destId = destId;
        _clone.msg = msg;
    }
}
