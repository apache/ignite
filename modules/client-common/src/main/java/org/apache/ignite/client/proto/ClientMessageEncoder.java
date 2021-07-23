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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * Encodes client messages:
 * 1. MAGIC for first message.
 * 2. Payload length (varint).
 * 3. Payload (bytes).
 */
public class ClientMessageEncoder extends MessageToByteEncoder<ByteBuffer> {
    /** Magic encoded flag. */
    private boolean magicEncoded;

    /** {@inheritDoc} */
    @Override protected void encode(ChannelHandlerContext ctx, ByteBuffer message, ByteBuf out) {
        if (!magicEncoded) {
            out.writeBytes(ClientMessageDecoder.MAGIC_BYTES);

            magicEncoded = true;
        }

        out.writeInt(message.remaining());
        out.writeBytes(message);
    }
}
