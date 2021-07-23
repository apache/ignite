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

import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.buffer.ArrayBufferOutput;

/**
 * Ignite-specific MsgPack extension.
 */
public class ClientMessagePacker extends MessageBufferPacker {
    /**
     * Constructor.
     */
    public ClientMessagePacker() {
        // TODO: Pooled buffers IGNITE-15162.
        super(new ArrayBufferOutput(), MessagePack.DEFAULT_PACKER_CONFIG);
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
}
