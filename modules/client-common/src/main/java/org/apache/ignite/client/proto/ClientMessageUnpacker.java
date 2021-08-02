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

package org.apache.ignite.client.proto;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.UUID;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import org.apache.ignite.lang.IgniteException;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageSizeException;
import org.msgpack.core.MessageTypeException;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.buffer.InputStreamBufferInput;

import static org.apache.ignite.client.proto.ClientDataType.BITMASK;
import static org.apache.ignite.client.proto.ClientDataType.BYTES;
import static org.apache.ignite.client.proto.ClientDataType.DECIMAL;
import static org.apache.ignite.client.proto.ClientDataType.DOUBLE;
import static org.apache.ignite.client.proto.ClientDataType.FLOAT;
import static org.apache.ignite.client.proto.ClientDataType.INT16;
import static org.apache.ignite.client.proto.ClientDataType.INT32;
import static org.apache.ignite.client.proto.ClientDataType.INT64;
import static org.apache.ignite.client.proto.ClientDataType.INT8;
import static org.apache.ignite.client.proto.ClientDataType.STRING;

/**
 * Ignite-specific MsgPack extension based on Netty ByteBuf.
 * <p>
 * Releases wrapped buffer on {@link #close()} .
 */
public class ClientMessageUnpacker extends MessageUnpacker {
    /** Underlying buffer. */
    private final ByteBuf buf;

    /** Closed flag. */
    private boolean closed = false;

    /**
     * Constructor.
     *
     * @param buf Input.
     */
    public ClientMessageUnpacker(ByteBuf buf) {
        super(new InputStreamBufferInput(new ByteBufInputStream(buf)), MessagePack.DEFAULT_UNPACKER_CONFIG);

        this.buf = buf;
    }

    /**
     * Reads an UUID.
     *
     * @return UUID value.
     * @throws IOException when underlying input throws IOException.
     * @throws MessageTypeException when type is not UUID.
     * @throws MessageSizeException when size is not correct.
     */
    public UUID unpackUuid() throws IOException {
        var hdr = unpackExtensionTypeHeader();
        var type = hdr.getType();
        var len = hdr.getLength();

        if (type != ClientMsgPackType.UUID)
            throw new MessageTypeException("Expected UUID extension (1), but got " + type);

        if (len != 16)
            throw new MessageSizeException("Expected 16 bytes for UUID extension, but got " + len, len);

        var bytes = readPayload(16);

        ByteBuffer bb = ByteBuffer.wrap(bytes);

        return new UUID(bb.getLong(), bb.getLong());
    }

    /**
     * Reads a decimal.
     *
     * @return Decimal value.
     * @throws IOException when underlying input throws IOException.
     */
    public BigDecimal unpackDecimal() throws IOException {
        throw new IOException("TODO: IGNITE-15163");
    }

    /**
     * Reads a bit set.
     *
     * @return Bit set.
     * @throws IOException when underlying input throws IOException.
     */
    public BitSet unpackBitSet() throws IOException {
        throw new IOException("TODO: IGNITE-15163");
    }

    /**
     * Unpacks an object based on the specified type.
     *
     * @param dataType Data type code.
     *
     * @return Unpacked object.
     * @throws IOException when underlying input throws IOException.
     * @throws IgniteException when data type is not valid.
     */
    public Object unpackObject(int dataType) throws IOException {
        if (tryUnpackNil())
            return null;

        switch (dataType) {
            case INT8:
                return unpackByte();

            case INT16:
                return unpackShort();

            case INT32:
                return unpackInt();

            case INT64:
                return unpackLong();

            case FLOAT:
                return unpackFloat();

            case DOUBLE:
                return unpackDouble();

            case ClientDataType.UUID:
                return unpackUuid();

            case STRING:
                return unpackString();

            case BYTES:
                var cnt = unpackBinaryHeader();

                return readPayload(cnt);

            case DECIMAL:
                return unpackDecimal();

            case BITMASK:
                return unpackBitSet();
        }

        throw new IgniteException("Unknown client data type: " + dataType);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        if (closed)
            return;

        closed = true;

        if (buf.refCnt() > 0)
            buf.release();
    }
}
