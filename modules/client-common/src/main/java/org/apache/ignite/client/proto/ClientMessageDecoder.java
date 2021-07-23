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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.CharsetUtil;
import org.apache.ignite.lang.IgniteException;

/**
 * Decodes full client messages:
 * 1. MAGIC for first message.
 * 2. Payload length (varint).
 * 3. Payload (bytes).
 */
public class ClientMessageDecoder extends ByteToMessageDecoder {
    /** Magic bytes before handshake. */
    public static final byte[] MAGIC_BYTES = new byte[]{0x49, 0x47, 0x4E, 0x49}; // IGNI

    /** Data buffer. */
    private byte[] data = new byte[4]; // TODO: Pooled buffers IGNITE-15162.

    /** Remaining byte count. */
    private int cnt = -4;

    /** Message size. */
    private int msgSize = -1;

    /** Magic decoded flag. */
    private boolean magicDecoded;

    /** Magic decoding failed flag. */
    private boolean magicFailed;

    /** {@inheritDoc} */
    @Override protected void decode(ChannelHandlerContext ctx, ByteBuf byteBuf, List<Object> list) {
        if (!readMagic(byteBuf))
            return;

        while (read(byteBuf))
            list.add(ByteBuffer.wrap(data));
    }

    /**
     * Checks the magic header for the first message.
     *
     * @param byteBuf Buffer.
     * @return {@code true} when magic header has been received and is valid, {@code false} otherwise.
     * @throws IgniteException When magic is invalid.
     */
    private boolean readMagic(ByteBuf byteBuf) {
        if (magicFailed)
            return false;

        if (magicDecoded)
            return true;

        if (byteBuf.readableBytes() < MAGIC_BYTES.length)
            return false;

        assert data.length == MAGIC_BYTES.length;

        byteBuf.readBytes(data, 0, MAGIC_BYTES.length);

        magicDecoded = true;
        cnt = -1;
        msgSize = 0;

        if (Arrays.equals(data, MAGIC_BYTES))
            return true;

        magicFailed = true;

        throw new IgniteException("Invalid magic header in thin client connection. " +
                "Expected 'IGNI', but was '" + new String(data, CharsetUtil.US_ASCII) + "'.");
    }

    /**
     * Reads the buffer.
     *
     * @param buf Buffer.
     * @return True when a complete message has been received; false otherwise.
     * @throws IgniteException when message is invalid.
     */
    private boolean read(ByteBuf buf) {
        if (buf.readableBytes() == 0)
            return false;

        if (cnt < 0) {
            if (buf.readableBytes() < 4)
                return false;

            msgSize = buf.readInt();

            assert msgSize >= 0;
            data = new byte[msgSize];
            cnt = 0;
        }

        assert data != null;
        assert msgSize >= 0;

        int remaining = buf.readableBytes();

        if (remaining > 0) {
            int missing = msgSize - cnt;

            if (missing > 0) {
                int len = Math.min(missing, remaining);

                buf.readBytes(data, cnt, len);

                cnt += len;
            }
        }

        if (cnt == msgSize) {
            cnt = -1;
            msgSize = -1;

            return true;
        }

        return false;
    }
}
