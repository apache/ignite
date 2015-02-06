/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.rest.client.message;

import org.apache.ignite.internal.util.direct.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.gridgain.grid.util.direct.*;

import java.nio.*;
import java.util.*;

/**
 * Client message wrapper for direct marshalling.
 */
public class GridClientMessageWrapper extends GridTcpCommunicationMessageAdapter {
    /** */
    private static final long serialVersionUID = 5284375300887454697L;

    /** Client request header. */
    public static final byte REQ_HEADER = (byte)0x90;

    /** Stream. */
    private final GridTcpCommunicationByteBufferStream stream = new GridTcpCommunicationByteBufferStream(null);

    /** */
    private int msgSize;

    /** */
    private long reqId;

    /** */
    private UUID clientId;

    /** */
    private UUID destId;

    /** */
    private ByteBuffer msg;

    /**
     * @return Request ID.
     */
    public long requestId() {
        return reqId;
    }

    /**
     * @param reqId Request ID.
     */
    public void requestId(long reqId) {
        this.reqId = reqId;
    }

    /**
     * @return Message size.
     */
    public int messageSize() {
        return msgSize;
    }

    /**
     * @param msgSize Message size.
     */
    public void messageSize(int msgSize) {
        this.msgSize = msgSize;
    }

    /**
     * @return Client ID.
     */
    public UUID clientId() {
        return clientId;
    }

    /**
     * @param clientId Client ID.
     */
    public void clientId(UUID clientId) {
        this.clientId = clientId;
    }

    /**
     * @return Destination ID.
     */
    public UUID destinationId() {
        return destId;
    }

    /**
     * @param destId Destination ID.
     */
    public void destinationId(UUID destId) {
        this.destId = destId;
    }

    /**
     * @return Message buffer.
     */
    public ByteBuffer message() {
        return msg;
    }

    /**
     * @return Message bytes.
     */
    public byte[] messageArray() {
        assert msg.hasArray();
        assert msg.position() == 0 && msg.remaining() == msg.capacity();

        return msg.array();
    }

    /**
     * @param msg Message bytes.
     */
    public void message(ByteBuffer msg) {
        this.msg = msg;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf) {
        stream.setBuffer(buf);

        if (!commState.typeWritten) {
            if (stream.remaining() < 1)
                return false;

            stream.writeByte(directType());

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 0:
                if (stream.remaining() < 4)
                    return false;

                stream.writeInt(msgSize);

                commState.idx++;

            case 1:
                if (stream.remaining() < 8)
                    return false;

                stream.writeLong(reqId);

                commState.idx++;

            case 2:
                if (stream.remaining() < 16)
                    return false;

                stream.writeByteArray(U.uuidToBytes(clientId), 0, 16);

                commState.idx++;

            case 3:
                if (stream.remaining() < 16)
                    return false;

                stream.writeByteArray(U.uuidToBytes(destId), 0, 16);

                commState.idx++;

            case 4:
                stream.writeByteArray(msg.array(), msg.position(), msg.remaining());

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf) {
        stream.setBuffer(buf);

        switch (commState.idx) {
            case 0:
                if (stream.remaining() < 4)
                    return false;

                msgSize = stream.readInt();

                if (msgSize == 0) // Ping message.
                    return true;

                commState.idx++;

            case 1:
                if (stream.remaining() < 8)
                    return false;

                reqId = stream.readLong();

                commState.idx++;

            case 2:
                if (stream.remaining() < 16)
                    return false;

                clientId = U.bytesToUuid(stream.readByteArray(16), 0);

                commState.idx++;

            case 3:
                if (stream.remaining() < 16)
                    return false;

                destId = U.bytesToUuid(stream.readByteArray(16), 0);

                commState.idx++;

            case 4:
                byte[] msg0 = stream.readByteArray(msgSize);

                if (!stream.lastFinished())
                    return false;

                msg = ByteBuffer.wrap(msg0);

                commState.idx++;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return REQ_HEADER;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridClientMessageWrapper _clone = new GridClientMessageWrapper();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        GridClientMessageWrapper _clone = (GridClientMessageWrapper)_msg;

        _clone.reqId = reqId;
        _clone.msgSize = msgSize;
        _clone.clientId = clientId;
        _clone.destId = destId;
        _clone.msg = msg;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridClientMessageWrapper.class, this);
    }
}
