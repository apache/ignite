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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.math.BigInteger;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.lang.IgniteUuid;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.msgpack.core.ExtensionTypeHeader;

/**
 * Tests Ignite ByteBuf-based unpacker.
 */
public class ClientMessageUnpackerTest {
    private static void testUnpacker(
            Consumer<ClientMessagePacker> pack,
            Function<ClientMessageUnpacker, Object> unpack,
            Object value
    ) {
        try (var packer = new ClientMessagePacker(Unpooled.buffer())) {
            pack.accept(packer);
            
            ByteBuf buf = packer.getBuffer().copy();
            buf.readerIndex(ClientMessageCommon.HEADER_SIZE);
            
            Object res = unpack.apply(new ClientMessageUnpacker(buf));
            
            if (value != null && value.getClass().isArray()) {
                assertArrayEquals((byte[]) value, (byte[]) res);
            } else {
                assertEquals(value, res);
            }
        }
    }
    
    @Test
    public void testUnpackNil() {
        testUnpacker(ClientMessagePacker::packNil, u -> {
            u.unpackNil();
            return null;
        }, null);
    }
    
    @Test
    public void testTryUnpackNil() {
        testUnpacker(ClientMessagePacker::packNil, ClientMessageUnpacker::tryUnpackNil, true);
        testUnpacker(clientMessagePacker -> clientMessagePacker.packInt(1), ClientMessageUnpacker::tryUnpackNil, false);
    }
    
    @ParameterizedTest
    @ValueSource(bytes = {0, 1, -1, Byte.MAX_VALUE, Byte.MIN_VALUE})
    public void testUnpackByte(byte b) {
        testUnpacker(p -> p.packByte(b), ClientMessageUnpacker::unpackByte, b);
    }
    
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testUnpackBoolean(boolean b) {
        testUnpacker(p -> p.packBoolean(b), ClientMessageUnpacker::unpackBoolean, b);
    }
    
    @ParameterizedTest
    @ValueSource(shorts = {0, 1, -1, Byte.MAX_VALUE, Byte.MIN_VALUE, Short.MIN_VALUE, Short.MAX_VALUE})
    public void testUnpackShort(short s) {
        testUnpacker(p -> p.packShort(s), ClientMessageUnpacker::unpackShort, s);
    }
    
    @ParameterizedTest
    @ValueSource(ints = {0, 1, -1, Byte.MAX_VALUE, Byte.MIN_VALUE, Short.MIN_VALUE, Short.MAX_VALUE, Integer.MIN_VALUE,
            Integer.MAX_VALUE})
    public void testUnpackInt(int i) {
        testUnpacker(p -> p.packInt(i), ClientMessageUnpacker::unpackInt, i);
    }
    
    @ParameterizedTest
    @ValueSource(longs = {0, 1, -1, Byte.MAX_VALUE, Byte.MIN_VALUE, Short.MIN_VALUE, Short.MAX_VALUE, Integer.MIN_VALUE,
            Integer.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE})
    public void testUnpackLong(long l) {
        testUnpacker(p -> p.packLong(l), ClientMessageUnpacker::unpackLong, l);
    }
    
    @ParameterizedTest
    @ValueSource(longs = {0, 1, -1, Byte.MAX_VALUE, Byte.MIN_VALUE, Short.MIN_VALUE, Short.MAX_VALUE, Integer.MIN_VALUE,
            Integer.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE})
    public void testUnpackBigInteger(long l) {
        var bi = BigInteger.valueOf(l);
        testUnpacker(p -> p.packBigInteger(bi), ClientMessageUnpacker::unpackBigInteger, bi);
    }
    
    @ParameterizedTest
    @ValueSource(floats = {0, 1, -1, Byte.MAX_VALUE, Byte.MIN_VALUE, Short.MIN_VALUE, Short.MAX_VALUE, Integer.MIN_VALUE,
            Integer.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE, Float.MIN_VALUE, Float.MAX_VALUE})
    public void testUnpackFloat(float f) {
        testUnpacker(p -> p.packFloat(f), ClientMessageUnpacker::unpackFloat, f);
    }
    
    @ParameterizedTest
    @ValueSource(doubles = {0, 1, -1, Byte.MAX_VALUE, Byte.MIN_VALUE, Short.MIN_VALUE, Short.MAX_VALUE, Integer.MIN_VALUE,
            Integer.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE, Float.MIN_VALUE, Float.MAX_VALUE,
            Double.MIN_VALUE, Double.MAX_VALUE})
    public void testUnpackDouble(double d) {
        testUnpacker(p -> p.packDouble(d), ClientMessageUnpacker::unpackDouble, d);
    }
    
    @ParameterizedTest
    @ValueSource(strings = {"", "Abc", "Абв", "🔥", "𒀖𝄞"})
    public void testUnpackString(String s) {
        testUnpacker(p -> p.packString(s), ClientMessageUnpacker::unpackString, s);
    }
    
    @ParameterizedTest
    @ValueSource(ints = {0, 1, 255, 256, 65535, 65536, Integer.MAX_VALUE})
    public void testUnpackArrayHeader(int i) {
        testUnpacker(p -> p.packArrayHeader(i), ClientMessageUnpacker::unpackArrayHeader, i);
    }
    
    @ParameterizedTest
    @ValueSource(ints = {0, 1, 255, 256, 65535, 65536, Integer.MAX_VALUE})
    public void testUnpackMapHeader(int i) {
        testUnpacker(p -> p.packMapHeader(i), ClientMessageUnpacker::unpackMapHeader, i);
    }
    
    @ParameterizedTest
    @ValueSource(ints = {0, 1, 255, 256, 65535, 65536, Integer.MAX_VALUE})
    public void testUnpackExtensionTypeHeader(int i) {
        testUnpacker(p -> p.packExtensionTypeHeader((byte) 33, i), ClientMessageUnpacker::unpackExtensionTypeHeader,
                new ExtensionTypeHeader((byte) 33, i));
    }
    
    @ParameterizedTest
    @ValueSource(ints = {0, 1, 255, 256, 65535, 65536, Integer.MAX_VALUE})
    public void testUnpackBinaryHeader(int i) {
        testUnpacker(p -> p.packBinaryHeader(i), ClientMessageUnpacker::unpackBinaryHeader, i);
    }
    
    @ParameterizedTest
    @ValueSource(ints = {0, 1, 255, 256, 65535, 65536, Integer.MAX_VALUE})
    public void testUnpackRawStringHeader(int i) {
        testUnpacker(p -> p.packRawStringHeader(i), ClientMessageUnpacker::unpackRawStringHeader, i);
    }
    
    @Test
    public void testReadPayload() {
        var b = new byte[]{1, 5, 120};
        
        testUnpacker(p -> p.writePayload(b), p -> p.readPayload(b.length), b);
        testUnpacker(p -> p.writePayload(b, 1, 1), p -> p.readPayload(1), new byte[]{5});
    }
    
    @Test
    public void testSkipValues() {
        testUnpacker(p -> {
            p.packInt(123456);
            p.packBoolean(false);
            
            p.packMapHeader(2);
            p.packString("x");
            p.packNil();
            p.packUuid(UUID.randomUUID());
            p.packIgniteUuid(new IgniteUuid(UUID.randomUUID(), 123));
            
            p.packDouble(1.1);
            p.packDouble(2.2);
        }, p -> {
            p.skipValues(3);
            
            return p.unpackDouble();
        }, 1.1);
    }
}
