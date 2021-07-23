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

import io.netty.buffer.Unpooled;
import org.apache.ignite.client.proto.ClientMessageDecoder;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Message decoding tests.
 */
public class ClientMessageDecoderTest {
    @Test
    void testEmptyBufferReturnsNoResults() throws Exception {
        var buf = new byte[0];
        var res = new ArrayList<>();

        new ClientMessageDecoder().decode(null, Unpooled.wrappedBuffer(buf), res);

        assertEquals(0, res.size());
    }

    @Test
    void testValidMagicAndMessageReturnsPayload() {
        var res = new ArrayList<>();
        new ClientMessageDecoder().decode(null, Unpooled.wrappedBuffer(getMagicWithPayload()), res);

        assertEquals(1, res.size());

        var resBuf = (ByteBuffer)res.get(0);
        assertArrayEquals(new byte[]{33, 44}, resBuf.array());
    }

    @Test
    void testInvalidMagicThrowsException() {
        byte[] buf = {66, 69, 69, 70, 1, 2, 3};

        var t = assertThrows(IgniteException.class,
                () -> new ClientMessageDecoder().decode(null, Unpooled.wrappedBuffer(buf), new ArrayList<>()));

        assertEquals("Invalid magic header in thin client connection. Expected 'IGNI', but was 'BEEF'.",
                t.getMessage());
    }

    /**
     * Tests multipart buffer arrival: socket can split incoming stream into arbitrary chunks.
     */
    @Test
    void testMultipartValidMagicAndMessageReturnsPayload() throws Exception {
        var decoder = new ClientMessageDecoder();
        var res = new ArrayList<>();

        byte[] data = getMagicWithPayload();

        decoder.decode(null, Unpooled.wrappedBuffer(data, 0, 4), res);
        assertEquals(0, res.size());

        decoder.decode(null, Unpooled.wrappedBuffer(data, 4, 4), res);
        assertEquals(0, res.size());

        decoder.decode(null, Unpooled.wrappedBuffer(data, 8, 1), res);
        assertEquals(0, res.size());

        decoder.decode(null, Unpooled.wrappedBuffer(data, 9, 1), res);
        assertEquals(1, res.size());

        var resBuf = (ByteBuffer) res.get(0);
        assertArrayEquals(new byte[]{33, 44}, resBuf.array());
    }

    private byte[] getMagicWithPayload() {
        var buf = new byte[10];

        // Magic.
        System.arraycopy(ClientMessageDecoder.MAGIC_BYTES, 0, buf, 0, 4);

        // Message size.
        buf[7] = 2;

        // Payload.
        buf[8] = 33;
        buf[9] = 44;

        return buf;
    }
}
