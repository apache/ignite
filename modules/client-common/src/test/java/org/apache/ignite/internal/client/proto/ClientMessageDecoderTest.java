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

package org.apache.ignite.internal.client.proto;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Message decoding tests.
 */
public class ClientMessageDecoderTest {
    @Test
    void testEmptyBufferReturnsNoResults() throws Exception {
        var res = decode(new byte[0]);

        assertNull(res);
    }

    @Test
    void testValidMagicAndMessageReturnsPayload() throws Exception {
        byte[] bytes = decode(getMagicWithPayload());

        assertArrayEquals(new byte[]{33, 44}, bytes);
    }

    @Test
    void testInvalidMagicThrowsException() {
        byte[] buf = {66, 69, 69, 70, 1, 2, 3};

        var t = assertThrows(IgniteException.class, () -> decode(buf));

        assertEquals("Invalid magic header in thin client connection. Expected 'IGNI', but was 'BEEF'.",
                t.getMessage());
    }

    private static byte[] getMagicWithPayload() {
        var buf = new byte[10];

        // Magic.
        System.arraycopy(ClientMessageCommon.MAGIC_BYTES, 0, buf, 0, 4);

        // Message size.
        buf[7] = 2;

        // Payload.
        buf[8] = 33;
        buf[9] = 44;

        return buf;
    }

    private static byte[] decode(byte[] request) throws Exception {
        var resBuf = (ByteBuf)new ClientMessageDecoder().decode(null, Unpooled.wrappedBuffer(request));

        if (resBuf == null)
            return null;

        var bytes = new byte[resBuf.readableBytes()];
        resBuf.readBytes(bytes);

        return bytes;
    }
}
