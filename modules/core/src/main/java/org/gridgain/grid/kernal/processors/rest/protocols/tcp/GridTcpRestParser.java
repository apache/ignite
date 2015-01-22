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
import org.apache.ignite.marshaller.*;
import org.apache.ignite.marshaller.jdk.*;
import org.apache.ignite.client.marshaller.*;
import org.gridgain.grid.kernal.processors.rest.client.message.*;
import org.gridgain.grid.util.*;
import org.apache.ignite.internal.util.nio.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.nio.charset.*;
import java.util.*;

import static org.gridgain.grid.kernal.processors.rest.protocols.tcp.GridMemcachedMessage.*;
import static org.apache.ignite.internal.util.nio.GridNioSessionMetaKey.*;

/**
 * Parser for extended memcache protocol. Handles parsing and encoding activity.
 */
public class GridTcpRestParser implements GridNioParser {
    /** UTF-8 charset. */
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    /** JDK marshaller. */
    private final IgniteMarshaller jdkMarshaller = new IgniteJdkMarshaller();

    /** {@inheritDoc} */
    @Nullable @Override public GridClientMessage decode(GridNioSession ses, ByteBuffer buf) throws IOException,
        IgniteCheckedException {
        ParserState state = ses.removeMeta(PARSER_STATE.ordinal());

        if (state == null)
            state = new ParserState();

        GridClientPacketType type = state.packetType();

        if (type == null) {
            byte hdr = buf.get(buf.position());

            switch (hdr) {
                case MEMCACHE_REQ_FLAG:
                    state.packet(new GridMemcachedMessage());
                    state.packetType(GridClientPacketType.MEMCACHE);

                    break;

                case GRIDGAIN_REQ_FLAG:
                    // Skip header.
                    buf.get();

                    state.packetType(GridClientPacketType.GRIDGAIN);

                    break;

                case GRIDGAIN_HANDSHAKE_FLAG:
                    // Skip header.
                    buf.get();

                    state.packetType(GridClientPacketType.GRIDGAIN_HANDSHAKE);

                    break;

                default:
                    throw new IOException("Failed to parse incoming packet (invalid packet start) [ses=" + ses +
                        ", b=" + Integer.toHexString(hdr & 0xFF) + ']');
            }
        }

        GridClientMessage res = null;

        switch (state.packetType()) {
            case MEMCACHE:
                res = parseMemcachePacket(ses, buf, state);

                break;

            case GRIDGAIN_HANDSHAKE:
                res = parseHandshake(buf, state);

                break;

            case GRIDGAIN:
                res = parseCustomPacket(ses, buf, state);

                break;
        }

        if (res == null)
            // Packet was not fully parsed yet.
            ses.addMeta(PARSER_STATE.ordinal(), state);

        return res;
    }

    /** {@inheritDoc} */
    @Override public ByteBuffer encode(GridNioSession ses, Object msg0) throws IOException, IgniteCheckedException {
        assert msg0 != null;

        GridClientMessage msg = (GridClientMessage)msg0;

        if (msg instanceof GridMemcachedMessage)
            return encodeMemcache((GridMemcachedMessage)msg);
        else if (msg == GridClientPingPacket.PING_MESSAGE)
            return ByteBuffer.wrap(GridClientPingPacket.PING_PACKET);
        else if (msg instanceof GridClientHandshakeResponse)
            return ByteBuffer.wrap(new byte[] {
                ((GridClientHandshakeResponse)msg).resultCode()
            });
        else {
            GridClientMarshaller marsh = marshaller(ses);

            ByteBuffer res = marsh.marshal(msg, 45);

            ByteBuffer slice = res.slice();

            slice.put(GRIDGAIN_REQ_FLAG);
            slice.putInt(res.remaining() - 5);
            slice.putLong(msg.requestId());
            slice.put(U.uuidToBytes(msg.clientId()));
            slice.put(U.uuidToBytes(msg.destinationId()));

            return res;
        }
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
     * Parses a client handshake, checking a client version and
     * reading the marshaller protocol ID.
     *
     * @param buf Message bytes.
     * @param state Parser state.
     * @return True if a hint was parsed, false if still need more bytes to parse.
     */
    @Nullable private GridClientMessage parseHandshake(ByteBuffer buf, ParserState state) {
        assert state.packetType() == GridClientPacketType.GRIDGAIN_HANDSHAKE;

        int idx = state.index();

        GridClientHandshakeRequest packet = (GridClientHandshakeRequest)state.packet();

        if (packet == null) {
            packet = new GridClientHandshakeRequest();

            state.packet(packet);
        }

        int rem = buf.remaining();

        if (rem > 0) {
            byte[] bbuf = new byte[5]; // Buffer to read data to.

            int nRead = Math.min(rem, bbuf.length); // Number of bytes to read.

            buf.get(bbuf, 0, nRead); // Batch read from buffer.

            int nAvailable = nRead; // Number of available bytes.

            if (idx < 4) { // Need to read version bytes.
                int len = Math.min(nRead, 4 - idx); // Number of version bytes available in buffer.

                packet.putBytes(bbuf, idx, len);

                idx += len;
                state.index(idx);
                nAvailable -= len;
            }

            assert idx <= 4 : "Wrong idx: " + idx;
            assert nAvailable == 0 || nAvailable == 1 : "Wrong nav: " + nAvailable;

            if (idx == 4 && nAvailable > 0)
                return packet;
        }

        return null; // Wait for more data.
    }

    /**
     * Parses custom packet serialized by GridGain marshaller.
     *
     * @param ses Session.
     * @param buf Buffer containing not parsed bytes.
     * @param state Parser state.
     * @return Parsed message.
     * @throws IOException If packet parsing or deserialization failed.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable private GridClientMessage parseCustomPacket(GridNioSession ses, ByteBuffer buf, ParserState state)
        throws IOException, IgniteCheckedException {
        assert state.packetType() == GridClientPacketType.GRIDGAIN;
        assert state.packet() == null;

        ByteArrayOutputStream tmp = state.buffer();

        int len = state.index();

        if (buf.remaining() > 0) {
            if (len == 0) { // Don't know the size yet.
                byte[] lenBytes = statefulRead(buf, tmp, 4);

                if (lenBytes != null) {
                    len = U.bytesToInt(lenBytes, 0);

                    if (len == 0)
                        return GridClientPingPacket.PING_MESSAGE;
                    else if (len < 0)
                        throw new IOException("Failed to parse incoming packet (invalid packet length) [ses=" + ses +
                            ", len=" + len + ']');

                    state.index(len);
                }
            }

            if (len > 0 && state.header() == null) {
                byte[] hdrBytes = statefulRead(buf, tmp, 40);

                if (hdrBytes != null) {
                    long reqId = GridClientByteUtils.bytesToLong(hdrBytes, 0);
                    UUID clientId = GridClientByteUtils.bytesToUuid(hdrBytes, 8);
                    UUID destId = GridClientByteUtils.bytesToUuid(hdrBytes, 24);

                    state.header(new HeaderData(reqId, clientId, destId));
                }
            }

            if (len > 0 && state.header() != null) {
                final int packetSize = len - 40;

                if (tmp.size() + buf.remaining() >= packetSize) {
                    if (buf.remaining() > 0) {
                        byte[] bodyBytes = new byte[packetSize - tmp.size()];

                        buf.get(bodyBytes);

                        tmp.write(bodyBytes);
                    }

                    return parseClientMessage(ses, state);
                }
                else
                    copyRemaining(buf, tmp);
            }
        }

        return null;
    }

    /**
     * Tries to read the specified amount of bytes using intermediate buffer. Stores
     * the bytes to intermediate buffer, if size requirement is not met.
     *
     * @param buf Byte buffer to read from.
     * @param intBuf Intermediate buffer to read bytes from and to save remaining bytes to.
     * @param size Number of bytes to read.
     * @return Resulting byte array or {@code null}, if both buffers contain less bytes
     *         than required. In case of non-null result, the intermediate buffer is empty.
     *         In case of {@code null} result, the input buffer is empty (read fully).
     * @throws IOException If IO error occurs.
     */
    @Nullable private byte[] statefulRead(ByteBuffer buf, ByteArrayOutputStream intBuf, int size) throws IOException {
        if (intBuf.size() + buf.remaining() >= size) {
            int off = 0;
            byte[] bytes = new byte[size];

            if (intBuf.size() > 0) {
                assert intBuf.size() < size;

                byte[] tmpBytes = intBuf.toByteArray();

                System.arraycopy(tmpBytes, 0, bytes, 0, tmpBytes.length);

                off = intBuf.size();

                intBuf.reset();
            }

            buf.get(bytes, off, size - off);

            return bytes;
        }
        else {
            copyRemaining(buf, intBuf);

            return null;
        }
    }

    /**
     * Copies remaining bytes from byte buffer to output stream.
     *
     * @param src Source buffer.
     * @param dest Destination stream.
     * @throws IOException If IO error occurs.
     */
    private void copyRemaining(ByteBuffer src, OutputStream dest) throws IOException {
        byte[] b = new byte[src.remaining()];

        src.get(b);

        dest.write(b);
    }

    /**
     * Parses {@link GridClientMessage} from raw bytes.
     *
     * @param ses Session.
     * @param state Parser state.
     * @return A parsed client message.
     * @throws IOException On marshaller error.
     * @throws IgniteCheckedException If no marshaller was defined for the session.
     */
    protected GridClientMessage parseClientMessage(GridNioSession ses, ParserState state) throws IOException, IgniteCheckedException {
        GridClientMarshaller marsh = marshaller(ses);

        GridClientMessage msg = marsh.unmarshal(state.buffer().toByteArray());

        msg.requestId(state.header().reqId());
        msg.clientId(state.header().clientId());
        msg.destinationId(state.header().destinationId());

        return msg;
    }

    /**
     * Encodes memcache message to a raw byte array.
     *
     * @param msg Message being serialized.
     * @return Serialized message.
     * @throws IgniteCheckedException If serialization failed.
     */
    private ByteBuffer encodeMemcache(GridMemcachedMessage msg) throws IgniteCheckedException {
        GridByteArrayList res = new GridByteArrayList(HDR_LEN);

        int keyLen = 0;

        int keyFlags = 0;

        if (msg.key() != null) {
            ByteArrayOutputStream rawKey = new ByteArrayOutputStream();

            keyFlags = encodeObj(msg.key(), rawKey);

            msg.key(rawKey.toByteArray());

            keyLen = rawKey.size();
        }

        int dataLen = 0;

        int valFlags = 0;

        if (msg.value() != null) {
            ByteArrayOutputStream rawVal = new ByteArrayOutputStream();

            valFlags = encodeObj(msg.value(), rawVal);

            msg.value(rawVal.toByteArray());

            dataLen = rawVal.size();
        }

        int flagsLen = 0;

        if (msg.addFlags())// || keyFlags > 0 || valFlags > 0)
            flagsLen = FLAGS_LENGTH;

        res.add(MEMCACHE_RES_FLAG);

        res.add(msg.operationCode());

        // Cast is required due to packet layout.
        res.add((short)keyLen);

        // Cast is required due to packet layout.
        res.add((byte)flagsLen);

        // Data type is always 0x00.
        res.add((byte)0x00);

        res.add((short)msg.status());

        res.add(keyLen + flagsLen + dataLen);

        res.add(msg.opaque(), 0, msg.opaque().length);

        // CAS, unused.
        res.add(0L);

        assert res.size() == HDR_LEN;

        if (flagsLen > 0) {
            res.add((short) keyFlags);
            res.add((short) valFlags);
        }

        assert msg.key() == null || msg.key() instanceof byte[];
        assert msg.value() == null || msg.value() instanceof byte[];

        if (keyLen > 0)
            res.add((byte[])msg.key(), 0, ((byte[])msg.key()).length);

        if (dataLen > 0)
            res.add((byte[])msg.value(), 0, ((byte[])msg.value()).length);

        return ByteBuffer.wrap(res.entireArray());
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
            return jdkMarshaller.unmarshal(bytes, null);

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
     * Encodes given object to a byte array and returns flags that describe the type of serialized object.
     *
     * @param obj Object to serialize.
     * @param out Output stream to which object should be written.
     * @return Serialization flags.
     * @throws IgniteCheckedException If JDK serialization failed.
     */
    private int encodeObj(Object obj, ByteArrayOutputStream out) throws IgniteCheckedException {
        int flags = 0;

        byte[] data = null;

        if (obj instanceof String)
            data = ((String)obj).getBytes(UTF_8);
        else if (obj instanceof Boolean) {
            data = new byte[] {(byte)((Boolean)obj ? '1' : '0')};

            flags |= BOOLEAN_FLAG;
        }
        else if (obj instanceof Integer) {
            data = U.intToBytes((Integer)obj);

            flags |= INT_FLAG;
        }
        else if (obj instanceof Long) {
            data = U.longToBytes((Long)obj);

            flags |= LONG_FLAG;
        }
        else if (obj instanceof Date) {
            data = U.longToBytes(((Date)obj).getTime());

            flags |= DATE_FLAG;
        }
        else if (obj instanceof Byte) {
            data = new byte[] {(Byte)obj};

            flags |= BYTE_FLAG;
        }
        else if (obj instanceof Float) {
            data = U.intToBytes(Float.floatToIntBits((Float)obj));

            flags |= FLOAT_FLAG;
        }
        else if (obj instanceof Double) {
            data = U.longToBytes(Double.doubleToLongBits((Double)obj));

            flags |= DOUBLE_FLAG;
        }
        else if (obj instanceof byte[]) {
            data = (byte[])obj;

            flags |= BYTE_ARR_FLAG;
        }
        else {
            jdkMarshaller.marshal(obj, out);

            flags |= SERIALIZED_FLAG;
        }

        if (data != null)
            out.write(data, 0, data.length);

        return flags;
    }

    /**
     * Returns marshaller.
     *
     * @return Marshaller.
     */
    protected GridClientMarshaller marshaller(GridNioSession ses) {
        GridClientMarshaller marsh = ses.meta(MARSHALLER.ordinal());

        assert marsh != null;

        return marsh;
    }

    /** {@inheritDoc} */
    public String toString() {
        return S.toString(GridTcpRestParser.class, this);
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
