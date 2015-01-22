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

package org.gridgain.grid.kernal.processors.rest.protocols.tcp;

import org.apache.ignite.*;
import org.apache.ignite.client.marshaller.*;
import org.gridgain.grid.kernal.processors.rest.client.message.*;
import org.apache.ignite.internal.util.direct.*;
import org.gridgain.grid.util.nio.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.nio.charset.*;
import java.util.*;

import static org.gridgain.grid.kernal.processors.rest.protocols.tcp.GridMemcachedMessage.*;
import static org.gridgain.grid.util.nio.GridNioSessionMetaKey.*;

/**
 *
 */
public class GridTcpRestDirectParser implements GridNioParser {
    /** UTF-8 charset. */
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    /** Message metadata key. */
    private static final int MSG_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** Protocol handler. */
    private final GridTcpRestProtocol proto;

    /** Message reader. */
    private final GridNioMessageReader msgReader;

    /**
     * @param proto Protocol handler.
     * @param msgReader Message reader.
     */
    public GridTcpRestDirectParser(GridTcpRestProtocol proto, GridNioMessageReader msgReader) {
        this.proto = proto;
        this.msgReader = msgReader;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object decode(GridNioSession ses, ByteBuffer buf) throws IOException, IgniteCheckedException {
        ParserState state = ses.removeMeta(PARSER_STATE.ordinal());

        if (state != null) {
            assert state.packetType() == GridClientPacketType.MEMCACHE;

            Object memcacheMsg = parseMemcachePacket(ses, buf, state);

            if (memcacheMsg == null)
                ses.addMeta(PARSER_STATE.ordinal(), state);

            return memcacheMsg;
        }

        GridTcpCommunicationMessageAdapter msg = ses.removeMeta(MSG_META_KEY);

        if (msg == null && buf.hasRemaining()) {
            byte type = buf.get(buf.position());

            if (type == GridClientMessageWrapper.REQ_HEADER) {
                buf.get();

                msg = new GridClientMessageWrapper();
            }
            else if (type == GridClientHandshakeRequestWrapper.HANDSHAKE_HEADER) {
                buf.get();

                msg = new GridClientHandshakeRequestWrapper();
            }
            else if (type == MEMCACHE_REQ_FLAG) {
                state = new ParserState();

                state.packet(new GridMemcachedMessage());
                state.packetType(GridClientPacketType.MEMCACHE);

                Object memcacheMsg = parseMemcachePacket(ses, buf, state);

                if (memcacheMsg == null)
                    ses.addMeta(PARSER_STATE.ordinal(), state);

                return memcacheMsg;
            }
            else
                throw new IOException("Invalid message type: " + type);
        }

        boolean finished = false;

        if (buf.hasRemaining())
            finished = msgReader.read(null, msg, buf);

        if (finished) {
            if (msg instanceof GridClientMessageWrapper) {
                GridClientMessageWrapper clientMsg = (GridClientMessageWrapper)msg;

                if (clientMsg.messageSize() == 0)
                    return GridClientPingPacket.PING_MESSAGE;

                GridClientMarshaller marsh = proto.marshaller(ses);

                GridClientMessage ret = marsh.unmarshal(clientMsg.messageArray());

                ret.requestId(clientMsg.requestId());
                ret.clientId(clientMsg.clientId());
                ret.destinationId(clientMsg.destinationId());

                return ret;
            }
            else {
                assert msg instanceof GridClientHandshakeRequestWrapper;

                GridClientHandshakeRequestWrapper req = (GridClientHandshakeRequestWrapper)msg;

                GridClientHandshakeRequest ret = new GridClientHandshakeRequest();

                ret.putBytes(req.bytes(), 0, 4);

                return ret;
            }
        }
        else {
            ses.addMeta(MSG_META_KEY, msg);

            return null;
        }
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer encode(GridNioSession ses, Object msg) throws IOException, IgniteCheckedException {
        // No encoding needed for direct messages.
        throw new UnsupportedEncodingException();
    }

    /**
     * Parses memcache protocol message.
     *
     * @param ses Session.
     * @param buf Buffer containing not parsed bytes.
     * @param state Current parser state.
     * @return Parsed packet.s
     * @throws IOException If packet cannot be parsed.
     * @throws IgniteCheckedException If deserialization error occurred.
     */
    @Nullable private GridClientMessage parseMemcachePacket(GridNioSession ses, ByteBuffer buf, ParserState state)
        throws IOException, IgniteCheckedException {
        assert state.packetType() == GridClientPacketType.MEMCACHE;
        assert state.packet() != null;
        assert state.packet() instanceof GridMemcachedMessage;

        GridMemcachedMessage req = (GridMemcachedMessage)state.packet();
        ByteArrayOutputStream tmp = state.buffer();
        int i = state.index();

        while (buf.remaining() > 0) {
            byte b = buf.get();

            if (i == 0)
                req.requestFlag(b);
            else if (i == 1)
                req.operationCode(b);
            else if (i == 2 || i == 3) {
                tmp.write(b);

                if (i == 3) {
                    req.keyLength(U.bytesToShort(tmp.toByteArray(), 0));

                    tmp.reset();
                }
            }
            else if (i == 4)
                req.extrasLength(b);
            else if (i >= 8 && i <= 11) {
                tmp.write(b);

                if (i == 11) {
                    req.totalLength(U.bytesToInt(tmp.toByteArray(), 0));

                    tmp.reset();
                }
            }
            else if (i >= 12 && i <= 15) {
                tmp.write(b);

                if (i == 15) {
                    req.opaque(tmp.toByteArray());

                    tmp.reset();
                }
            }
            else if (i >= HDR_LEN && i < HDR_LEN + req.extrasLength()) {
                tmp.write(b);

                if (i == HDR_LEN + req.extrasLength() - 1) {
                    req.extras(tmp.toByteArray());

                    tmp.reset();
                }
            }
            else if (i >= HDR_LEN + req.extrasLength() &&
                i < HDR_LEN + req.extrasLength() + req.keyLength()) {
                tmp.write(b);

                if (i == HDR_LEN + req.extrasLength() + req.keyLength() - 1) {
                    req.key(tmp.toByteArray());

                    tmp.reset();
                }
            }
            else if (i >= HDR_LEN + req.extrasLength() + req.keyLength() &&
                i < HDR_LEN + req.totalLength()) {
                tmp.write(b);

                if (i == HDR_LEN + req.totalLength() - 1) {
                    req.value(tmp.toByteArray());

                    tmp.reset();
                }
            }

            if (i == HDR_LEN + req.totalLength() - 1)
                // Assembled the packet.
                return assemble(ses, req);

            i++;
        }

        state.index(i);

        return null;
    }

    /**
     * Validates incoming packet and deserializes all fields that need to be deserialized.
     *
     * @param ses Session on which packet is being parsed.
     * @param req Raw packet.
     * @return Same packet with fields deserialized.
     * @throws IOException If parsing failed.
     * @throws IgniteCheckedException If deserialization failed.
     */
    private GridClientMessage assemble(GridNioSession ses, GridMemcachedMessage req) throws IOException, IgniteCheckedException {
        byte[] extras = req.extras();

        // First, decode key and value, if any
        if (req.key() != null || req.value() != null) {
            short keyFlags = 0;
            short valFlags = 0;

            if (req.hasFlags()) {
                if (extras == null || extras.length < FLAGS_LENGTH)
                    throw new IOException("Failed to parse incoming packet (flags required for command) [ses=" +
                        ses + ", opCode=" + Integer.toHexString(req.operationCode() & 0xFF) + ']');

                keyFlags = U.bytesToShort(extras, 0);
                valFlags = U.bytesToShort(extras, 2);
            }

            if (req.key() != null) {
                assert req.key() instanceof byte[];

                byte[] rawKey = (byte[])req.key();

                // Only values can be hessian-encoded.
                req.key(decodeObj(keyFlags, rawKey));
            }

            if (req.value() != null) {
                assert req.value() instanceof byte[];

                byte[] rawVal = (byte[])req.value();

                req.value(decodeObj(valFlags, rawVal));
            }
        }

        if (req.hasExpiration()) {
            if (extras == null || extras.length < 8)
                throw new IOException("Failed to parse incoming packet (expiration value required for command) [ses=" +
                    ses + ", opCode=" + Integer.toHexString(req.operationCode() & 0xFF) + ']');

            req.expiration(U.bytesToInt(extras, 4) & 0xFFFFFFFFL);
        }

        if (req.hasInitial()) {
            if (extras == null || extras.length < 16)
                throw new IOException("Failed to parse incoming packet (initial value required for command) [ses=" +
                    ses + ", opCode=" + Integer.toHexString(req.operationCode() & 0xFF) + ']');

            req.initial(U.bytesToLong(extras, 8));
        }

        if (req.hasDelta()) {
            if (extras == null || extras.length < 8)
                throw new IOException("Failed to parse incoming packet (delta value required for command) [ses=" +
                    ses + ", opCode=" + Integer.toHexString(req.operationCode() & 0xFF) + ']');

            req.delta(U.bytesToLong(extras, 0));
        }

        if (extras != null) {
            // Clients that include cache name must always include flags.
            int len = 4;

            if (req.hasExpiration())
                len += 4;

            if (req.hasDelta())
                len += 8;

            if (req.hasInitial())
                len += 8;

            if (extras.length - len > 0) {
                byte[] cacheName = new byte[extras.length - len];

                U.arrayCopy(extras, len, cacheName, 0, extras.length - len);

                req.cacheName(new String(cacheName, UTF_8));
            }
        }

        return req;
    }

    /**
     * Decodes value from a given byte array to the object according to the flags given.
     *
     * @param flags Flags.
     * @param bytes Byte array to decode.
     * @return Decoded value.
     * @throws IgniteCheckedException If deserialization failed.
     */
    private Object decodeObj(short flags, byte[] bytes) throws IgniteCheckedException {
        assert bytes != null;

        if ((flags & SERIALIZED_FLAG) != 0)
            return proto.jdkMarshaller().unmarshal(bytes, null);

        int masked = flags & 0xff00;

        switch (masked) {
            case BOOLEAN_FLAG:
                return bytes[0] == '1';
            case INT_FLAG:
                return U.bytesToInt(bytes, 0);
            case LONG_FLAG:
                return U.bytesToLong(bytes, 0);
            case DATE_FLAG:
                return new Date(U.bytesToLong(bytes, 0));
            case BYTE_FLAG:
                return bytes[0];
            case FLOAT_FLAG:
                return Float.intBitsToFloat(U.bytesToInt(bytes, 0));
            case DOUBLE_FLAG:
                return Double.longBitsToDouble(U.bytesToLong(bytes, 0));
            case BYTE_ARR_FLAG:
                return bytes;
            default:
                return new String(bytes, UTF_8);
        }
    }

    /**
     * Holder for parser state and temporary buffer.
     */
    protected static class ParserState {
        /** Parser index. */
        private int idx;

        /** Temporary data buffer. */
        private ByteArrayOutputStream buf = new ByteArrayOutputStream();

        /** Packet being assembled. */
        private GridClientMessage packet;

        /** Packet type. */
        private GridClientPacketType packetType;

        /** Header data. */
        private HeaderData hdr;

        /**
         * @return Stored parser index.
         */
        public int index() {
            return idx;
        }

        /**
         * @param idx Index to store.
         */
        public void index(int idx) {
            this.idx = idx;
        }

        /**
         * @return Temporary data buffer.
         */
        public ByteArrayOutputStream buffer() {
            return buf;
        }

        /**
         * @return Pending packet.
         */
        @Nullable public GridClientMessage packet() {
            return packet;
        }

        /**
         * @param packet Pending packet.
         */
        public void packet(GridClientMessage packet) {
            assert this.packet == null;

            this.packet = packet;
        }

        /**
         * @return Pending packet type.
         */
        public GridClientPacketType packetType() {
            return packetType;
        }

        /**
         * @param packetType Pending packet type.
         */
        public void packetType(GridClientPacketType packetType) {
            this.packetType = packetType;
        }

        /**
         * @return Header.
         */
        public HeaderData header() {
            return hdr;
        }

        /**
         * @param hdr Header.
         */
        public void header(HeaderData hdr) {
            this.hdr = hdr;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ParserState.class, this);
        }
    }

    /**
     * Header.
     */
    protected static class HeaderData {
        /** Request Id. */
        private final long reqId;

        /** Request Id. */
        private final UUID clientId;

        /** Request Id. */
        private final UUID destId;

        /**
         * @param reqId Request Id.
         * @param clientId Client Id.
         * @param destId Destination Id.
         */
        private HeaderData(long reqId, UUID clientId, UUID destId) {
            this.reqId = reqId;
            this.clientId = clientId;
            this.destId = destId;
        }

        /**
         * @return Request Id.
         */
        public long reqId() {
            return reqId;
        }

        /**
         * @return Client Id.
         */
        public UUID clientId() {
            return clientId;
        }

        /**
         * @return Destination Id.
         */
        public UUID destinationId() {
            return destId;
        }
    }
}
