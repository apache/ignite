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
import org.apache.ignite.client.proto.ClientMessageEncoder;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/**
 * Message encoding tests.
 */
public class ClientMessageEncoderTest {
    @Test
    public void testEncodeIncludesMagicWithFirstMessage() {
        var encoder = new ClientMessageEncoder();

        byte[] res = encode(new byte[]{1, 2}, encoder);
        assertArrayEquals(new byte[]{0x49, 0x47, 0x4E, 0x49, 0, 0, 0, 2, 1, 2}, res);

        byte[] res2 = encode(new byte[]{7, 8, 9}, encoder);
        assertArrayEquals(new byte[]{0, 0, 0, 3, 7, 8, 9}, res2);

        byte[] res3 = encode(new byte[0], encoder);
        assertArrayEquals(new byte[]{0, 0, 0, 0}, res3);
    }

    private byte[] encode(byte[] array, ClientMessageEncoder encoder) {
        var target = Unpooled.buffer(100);
        encoder.encode(null, ByteBuffer.wrap(array), target);

        return Arrays.copyOf(target.array(), target.writerIndex());
    }
}
