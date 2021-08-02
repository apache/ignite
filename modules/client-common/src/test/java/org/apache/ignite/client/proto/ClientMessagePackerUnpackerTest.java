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
import java.util.UUID;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests Ignite-specific MsgPack extensions.
 */
public class ClientMessagePackerUnpackerTest {
    @Test
    public void testPackerCloseReleasesPooledBuffer() {
        var buf = PooledByteBufAllocator.DEFAULT.directBuffer();
        var packer = new ClientMessagePacker(buf);

        assertEquals(1, buf.refCnt());

        packer.close();

        assertEquals(0, buf.refCnt());
    }

    @Test
    public void testPackerIncludesFourByteMessageLength() throws IOException {
        try (var packer = new ClientMessagePacker(PooledByteBufAllocator.DEFAULT.directBuffer())) {
            packer.packInt(1); // 1 byte
            packer.packString("Foo"); // 4 bytes

            var buf = packer.getBuffer();
            var len = buf.readInt();

            assertEquals(5, len);
            assertEquals(9, buf.writerIndex());
            assertEquals(Integer.MAX_VALUE, buf.maxCapacity());
        }
    }

    @Test
    public void testEmptyPackerReturnsFourZeroBytes() {
        try (var packer = new ClientMessagePacker(PooledByteBufAllocator.DEFAULT.directBuffer())) {
            var buf = packer.getBuffer();
            var len = buf.readInt();

            assertEquals(0, len);
            assertEquals(4, buf.writerIndex());
        }
    }

    @Test
    public void testUUID() throws IOException {
        testUUID(UUID.randomUUID());
        testUUID(new UUID(0, 0));
    }

    private void testUUID(UUID u) throws IOException {
        try (var packer = new ClientMessagePacker(PooledByteBufAllocator.DEFAULT.directBuffer())) {
            packer.packUuid(u);

            var buf = packer.getBuffer();
            var len = buf.readInt();

            byte[] data = new byte[buf.readableBytes()];
            buf.readBytes(data);

            try (var unpacker = new ClientMessageUnpacker(Unpooled.wrappedBuffer(data))) {
                var res = unpacker.unpackUuid();

                assertEquals(18, len); // 1 ext + 1 ext type + 16 UUID data
                assertEquals(u, res);
            }
        }
    }
}
