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

import org.apache.ignite.internal.direct.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;

import java.nio.*;

/**
 * Client handshake wrapper for direct marshalling.
 */
public class GridClientHandshakeRequestWrapper extends MessageAdapter {
    /** */
    private static final long serialVersionUID = -5705048094821942662L;

    /** Signal char. */
    public static final byte HANDSHAKE_HEADER = (byte)0x91;

    /** Stream. */
    private final DirectByteBufferStream stream = new DirectByteBufferStream(null);

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
        stream.setBuffer(buf);

        if (!commState.typeWritten) {
            if (!buf.hasRemaining())
                return false;

            stream.writeByte(directType());

            commState.typeWritten = true;
        }

        stream.writeByteArray(bytes, 0, bytes.length);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf) {
        stream.setBuffer(buf);

        bytes = stream.readByteArray(GridClientHandshakeRequest.PACKET_SIZE);

        return stream.lastFinished();
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return HANDSHAKE_HEADER;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public MessageAdapter clone() {
        GridClientHandshakeRequestWrapper _clone = new GridClientHandshakeRequestWrapper();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(MessageAdapter _msg) {
        GridClientHandshakeRequestWrapper _clone = (GridClientHandshakeRequestWrapper)_msg;

        _clone.bytes = bytes;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridClientHandshakeRequestWrapper.class, this);
    }
}
