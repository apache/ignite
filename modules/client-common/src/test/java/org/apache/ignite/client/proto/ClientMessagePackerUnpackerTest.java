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

import org.apache.ignite.client.proto.ClientMessagePacker;
import org.apache.ignite.client.proto.ClientMessageUnpacker;
import org.junit.jupiter.api.Test;
import org.msgpack.core.buffer.ArrayBufferInput;

import java.io.IOException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests Ignite-specific MsgPack extensions.
 */
public class ClientMessagePackerUnpackerTest {
    @Test
    public void testUUID() throws IOException {
        testUUID(UUID.randomUUID());
        testUUID(new UUID(0, 0));
    }

    private void testUUID(UUID u) throws IOException {
        var packer = new ClientMessagePacker();
        packer.packUuid(u);
        byte[] data = packer.toByteArray();

        var unpacker = new ClientMessageUnpacker(new ArrayBufferInput(data));
        var res = unpacker.unpackUuid();

        assertEquals(u, res);
    }
}
