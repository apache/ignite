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
import io.netty.buffer.ByteBufOutputStream;
import org.apache.ignite.lang.IgniteException;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.buffer.OutputStreamBufferOutput;

import static org.apache.ignite.client.proto.ClientMessageCommon.HEADER_SIZE;

/**
 * Ignite-specific MsgPack extension based on Netty ByteBuf.
 * <p>
 * Releases wrapped buffer on {@link #close()} .
 */
public class ClientMessagePacker extends MessagePacker {
    /** Underlying buffer. */
    private final ByteBuf buf;

    /** Closed flag. */
    private boolean closed = false;

    /**
     * Constructor.
     *
     * @param buf Buffer.
     */
    public ClientMessagePacker(ByteBuf buf) {
        // Reserve 4 bytes for the message length.
        super(new OutputStreamBufferOutput(new ByteBufOutputStream(buf.writerIndex(HEADER_SIZE))),
                MessagePack.DEFAULT_PACKER_CONFIG);

        this.buf = buf;
    }

    /**
     * Gets the underlying buffer.
     *
     * @return Underlying buffer.
     * @throws IgniteException When flush fails.
     */
    public ByteBuf getBuffer() {
        try {
            flush();
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }

        buf.setInt(0, buf.writerIndex() - HEADER_SIZE);

        return buf;
    }

    /**
     * Writes an UUID.
     *
     * @param val UUID value.
     * @return This instance.
     * @throws IOException when underlying output throws IOException.
     */
    public ClientMessagePacker packUuid(UUID val) throws IOException {
        packExtensionTypeHeader(ClientMsgPackType.UUID, 16);

        var bytes = new byte[16];
        ByteBuffer bb = ByteBuffer.wrap(bytes);

        bb.putLong(val.getMostSignificantBits());
        bb.putLong(val.getLeastSignificantBits());

        writePayload(bytes);

        return this;
    }

    /**
     * Writes a decimal.
     *
     * @param val Decimal value.
     * @return This instance.
     * @throws IOException when underlying output throws IOException.
     */
    public ClientMessagePacker packDecimal(BigDecimal val) throws IOException {
        throw new IOException("TODO: IGNITE-15163");
    }

    /**
     * Writes a bit set.
     *
     * @param val Bit set value.
     * @return This instance.
     * @throws IOException when underlying output throws IOException.
     */
    public ClientMessagePacker packBitSet(BitSet val) throws IOException {
        throw new IOException("TODO: IGNITE-15163");
    }

    /**
     * Packs an object.
     *
     * @param val Object value.
     * @return This instance.
     * @throws IOException when underlying output throws IOException.
     */
    public ClientMessagePacker packObject(Object val) throws IOException {
        if (val == null)
            return (ClientMessagePacker) packNil();

        if (val instanceof Integer)
            return (ClientMessagePacker) packInt((int) val);

        if (val instanceof Long)
            return (ClientMessagePacker) packLong((long) val);

        if (val instanceof UUID)
            return packUuid((UUID) val);

        if (val instanceof String)
            return (ClientMessagePacker) packString((String) val);

        if (val instanceof byte[]) {
            byte[] bytes = (byte[]) val;
            packBinaryHeader(bytes.length);
            writePayload(bytes);

            return this;
        }

        if (val instanceof BigDecimal)
            return packDecimal((BigDecimal) val);

        if (val instanceof BitSet)
            return packBitSet((BitSet) val);

        // TODO: Support all basic types IGNITE-15163
        throw new IOException("Unsupported type, can't serialize: " + val.getClass());
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
